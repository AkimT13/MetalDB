// PostgreSQL v3 wire protocol over the mini-SQL executor.
//
// Supported: SSL / GSS negotiation (declined with 'N'), StartupMessage, optional
// cleartext password authentication, ParameterStatus + BackendKeyData, simple
// Query messages (multiple ';'-separated statements), the extended query protocol
// (Parse / Bind / Describe / Execute / Close / Sync / Flush: named and unnamed
// statements and portals, text and binary parameters, text and binary results,
// row-limited Execute with PortalSuspended), server-side cursors (DECLARE / FETCH /
// MOVE / CLOSE, also addressable as protocol portals), RowDescription with type
// OIDs derived from MiniSQLResult::types, CommandComplete tags, SQLSTATE-coded
// ErrorResponse, EmptyQueryResponse, Terminate.
//
// Compatibility shims: BEGIN / START TRANSACTION / COMMIT / END / ROLLBACK / SET /
// DISCARD / RESET are accepted as no-ops (every MetalDB statement is already
// atomic and durable on its own; ROLLBACK emits a NOTICE saying nothing was undone),
// and `SELECT <integer>` (connection-pool health checks) answers without a table.
//
// Not supported: COPY FROM STDIN / TO STDOUT (server-side file COPY works), SSL,
// SCRAM / MD5 auth, NULL parameters (MetalDB has no NULLs), binary `numeric`.
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "Engine.hpp"
#include "MiniSQL.hpp"
#include "SqlParser.hpp"
#include "Server.hpp"

namespace {

constexpr int32_t kProtocolV3 = 196608;      // 3.0
constexpr int32_t kSslRequest = 80877103;
constexpr int32_t kGssEncRequest = 80877104;
constexpr int32_t kCancelRequest = 80877102;

// Postgres type OIDs.
constexpr int32_t kOidInt8 = 20;
constexpr int32_t kOidText = 25;
constexpr int32_t kOidFloat4 = 700;
constexpr int32_t kOidFloat8 = 701;

// ── message building ─────────────────────────────────────────────────────────

class Msg {
public:
    explicit Msg(char type) : type_(type) {}
    Msg& i16(int16_t v) {
        body_.push_back(static_cast<char>((v >> 8) & 0xFF));
        body_.push_back(static_cast<char>(v & 0xFF));
        return *this;
    }
    Msg& i32(int32_t v) {
        for (int shift = 24; shift >= 0; shift -= 8) body_.push_back(static_cast<char>((v >> shift) & 0xFF));
        return *this;
    }
    Msg& byte(char c) {
        body_.push_back(c);
        return *this;
    }
    Msg& cstr(const std::string& s) {
        body_.append(s);
        body_.push_back('\0');
        return *this;
    }
    Msg& bytes(const std::string& s) {
        body_.append(s);
        return *this;
    }
    std::string str() const {
        std::string out;
        out.push_back(type_);
        const int32_t len = static_cast<int32_t>(body_.size() + 4);
        for (int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<char>((len >> shift) & 0xFF));
        out += body_;
        return out;
    }

private:
    char type_;
    std::string body_;
};

int32_t readI32(const std::string& buf, size_t pos) {
    return static_cast<int32_t>((uint32_t(uint8_t(buf[pos])) << 24) | (uint32_t(uint8_t(buf[pos + 1])) << 16) |
                                (uint32_t(uint8_t(buf[pos + 2])) << 8) | uint32_t(uint8_t(buf[pos + 3])));
}

std::string errorResponse(const char* severity, const char* sqlstate, const std::string& message) {
    return Msg('E').byte('S').cstr(severity).byte('V').cstr(severity).byte('C').cstr(sqlstate).byte('M')
        .cstr(message).byte('\0').str();
}

std::string notice(const std::string& message) {
    return Msg('N').byte('S').cstr("NOTICE").byte('V').cstr("NOTICE").byte('C').cstr("00000").byte('M')
        .cstr(message).byte('\0').str();
}

std::string readyForQuery() { return Msg('Z').byte('I').str(); }

std::string commandComplete(const std::string& tag) { return Msg('C').cstr(tag).str(); }

int32_t oidFor(ColType t) {
    switch (t) {
        case ColType::UINT32:  // widened: uint32 does not fit int4
        case ColType::INT64: return kOidInt8;
        case ColType::FLOAT: return kOidFloat4;
        case ColType::DOUBLE: return kOidFloat8;
        case ColType::STRING: return kOidText;
    }
    return kOidText;
}

int16_t typeLen(ColType t) {
    switch (t) {
        case ColType::UINT32:
        case ColType::INT64: return 8;
        case ColType::FLOAT: return 4;
        case ColType::DOUBLE: return 8;
        case ColType::STRING: return -1;
    }
    return -1;
}

// Text (format 0) or binary (format 1) per result column. `formats` follows the
// Bind message rule: empty = all text, one entry = applies to all, else per column.
int16_t formatFor(const std::vector<int16_t>& formats, size_t col) {
    if (formats.empty()) return 0;
    return formats.size() == 1 ? formats[0] : (col < formats.size() ? formats[col] : 0);
}

std::string rowDescription(const std::vector<std::string>& names, const std::vector<ColType>& types,
                           const std::vector<int16_t>& formats = {}) {
    Msg m('T');
    m.i16(static_cast<int16_t>(names.size()));
    for (size_t i = 0; i < names.size(); ++i) {
        const ColType t = i < types.size() ? types[i] : ColType::STRING;
        m.cstr(names[i]).i32(0).i16(0).i32(oidFor(t)).i16(typeLen(t)).i32(-1).i16(formatFor(formats, i));
    }
    return m.str();
}

std::string be64(uint64_t v) {
    std::string out(8, '\0');
    for (int i = 0; i < 8; ++i) out[i] = static_cast<char>((v >> (56 - 8 * i)) & 0xFF);
    return out;
}

// Binary wire encoding of a text cell for the column's OID.
std::string binaryCell(const std::string& text, ColType t) {
    switch (t) {
        case ColType::UINT32:
        case ColType::INT64: return be64(static_cast<uint64_t>(std::stoll(text)));
        case ColType::DOUBLE: {
            const double d = std::strtod(text.c_str(), nullptr);
            uint64_t bits;
            std::memcpy(&bits, &d, 8);
            return be64(bits);
        }
        case ColType::FLOAT: {
            const float f = std::strtof(text.c_str(), nullptr);
            uint32_t bits;
            std::memcpy(&bits, &f, 4);
            return be64(bits).substr(4);
        }
        case ColType::STRING: return text;
    }
    return text;
}

std::string dataRow(const std::vector<std::string>& cells, const std::vector<ColType>& types = {},
                    const std::vector<int16_t>& formats = {}) {
    Msg m('D');
    m.i16(static_cast<int16_t>(cells.size()));
    for (size_t i = 0; i < cells.size(); ++i) {
        std::string v = cells[i];
        // Empty aggregate results (MIN over no rows) are SQL NULL.
        const ColType t = i < types.size() ? types[i] : ColType::STRING;
        if (v.empty() && t != ColType::STRING) {
            m.i32(-1);
            continue;
        }
        if (formatFor(formats, i) == 1) v = binaryCell(v, t);
        m.i32(static_cast<int32_t>(v.size())).bytes(v);
    }
    return m.str();
}

// ── statement handling ───────────────────────────────────────────────────────

// Splits on ';' outside single-quoted literals and `--` comments.
std::vector<std::string> splitStatements(const std::string& sql) {
    std::vector<std::string> out;
    std::string cur;
    bool inString = false;
    for (size_t i = 0; i < sql.size(); ++i) {
        const char ch = sql[i];
        if (!inString && ch == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
            while (i < sql.size() && sql[i] != '\n') cur.push_back(sql[i++]);
            if (i < sql.size()) cur.push_back('\n');
            continue;
        }
        if (ch == '\'') inString = !inString;
        if (ch == ';' && !inString) {
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(ch);
        }
    }
    out.push_back(cur);
    return out;
}

// True if the statement has nothing but whitespace and `--` comments.
bool isBlank(const std::string& s) {
    for (size_t i = 0; i < s.size(); ++i) {
        if (std::isspace(static_cast<unsigned char>(s[i]))) continue;
        if (s[i] == '-' && i + 1 < s.size() && s[i + 1] == '-') {
            while (i < s.size() && s[i] != '\n') ++i;
            continue;
        }
        return false;
    }
    return true;
}

std::vector<std::string> words(const std::string& s, size_t max) {
    std::vector<std::string> out;
    std::string w;
    for (char ch : s) {
        if (std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' || ch == '-') {
            w.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
        } else if (!w.empty()) {
            out.push_back(w);
            w.clear();
            if (out.size() == max) return out;
        }
    }
    if (!w.empty() && out.size() < max) out.push_back(w);
    return out;
}

const char* sqlStateFor(const std::string& m, bool userError) {
    if (!userError) return "XX000";  // internal_error
    if (m.find("does not exist") != std::string::npos) return "42P01";         // undefined_table
    if (m.find("already exists") != std::string::npos) return "42P07";         // duplicate_table
    if (m.find("out of bounds") != std::string::npos ||
        m.find("must appear in") != std::string::npos) return "42703";         // undefined_column
    if (m.rfind("expected", 0) == 0 || m.rfind("unexpected", 0) == 0 || m.rfind("unterminated", 0) == 0 ||
        m.rfind("unknown", 0) == 0) return "42601";                           // syntax_error
    if (m.find("parameter") != std::string::npos) return "08P01";              // protocol_violation
    if (m.find("range") != std::string::npos) return "22003";                  // numeric_value_out_of_range
    if (m.find("must be relative") != std::string::npos ||
        m.find("'..'") != std::string::npos) return "42501";                   // insufficient_privilege
    return "22023";                                                           // invalid_parameter_value
}

// A statement failure carrying its SQLSTATE.
struct PgError : std::runtime_error {
    PgError(const char* state, const std::string& msg) : std::runtime_error(msg), sqlstate(state) {}
    const char* sqlstate;
};

// Result of one statement, independent of simple / extended protocol framing.
struct Outcome {
    bool returnsRows = false;
    MiniSQLResult result;           // headers / types / rows when returnsRows
    std::string tag;                // CommandComplete tag; for SELECT the row count is appended at completion
    bool tagCountsRows = false;     // tag + " " + rows sent
    std::vector<std::string> notices;
};

bool allDigits(const std::string& s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) { return std::isdigit(static_cast<unsigned char>(c)); });
}

// Session / transaction commands and health checks that never reach the SQL
// executor. Returns false if `stmt` is not one of them.
bool runSpecial(const std::string& stmt, Outcome& out) {
    const auto w = words(stmt, 3);
    const std::string first = w.empty() ? "" : w[0];
    if (first == "BEGIN" || (first == "START" && w.size() > 1 && w[1] == "TRANSACTION")) {
        out.tag = "BEGIN";
    } else if (first == "COMMIT" || first == "END") {
        out.tag = "COMMIT";
    } else if (first == "ROLLBACK" || first == "ABORT") {
        out.notices.push_back("MetalDB statements autocommit; ROLLBACK did not undo anything");
        out.tag = "ROLLBACK";
    } else if (first == "SET" || first == "RESET" || first == "DISCARD") {
        out.tag = first;
    } else if (first == "SELECT" && w.size() == 2 && allDigits(w[1])) {  // SELECT <integer>
        out.returnsRows = true;
        out.result.headers = {"?column?"};
        out.result.types = {ColType::INT64};
        out.result.rows = {{w[1]}};
        out.tag = "SELECT";
        out.tagCountsRows = true;
    } else {
        return false;
    }
    return true;
}

// ── server-side cursors: DECLARE / FETCH / CLOSE ─────────────────────────────
// The cursor's query runs at DECLARE; FETCH pages through the materialized rows.

struct Cursor {
    Outcome outcome;
    size_t pos = 0;
};

struct SessionState {
    std::map<std::string, Cursor> cursors;
};

std::string cursorName(const sql::Token& t) {
    if (t.kind == sql::TokenKind::QuotedIdent) return t.text;
    if (t.kind != sql::TokenKind::Identifier) throw PgError("42601", "expected cursor name");
    std::string lower = t.text;
    for (char& c : lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return lower;  // unquoted identifiers fold to lower case, as in Postgres
}

// Offset just past the first whole word `word` (case-insensitive) at or after `from`.
size_t afterWord(const std::string& s, const char* word, size_t from) {
    const size_t n = std::strlen(word);
    for (size_t i = from; i + n <= s.size(); ++i) {
        if ((i > 0 && (std::isalnum(static_cast<unsigned char>(s[i - 1])) || s[i - 1] == '_'))) continue;
        bool eq = true;
        for (size_t k = 0; k < n && eq; ++k)
            eq = std::toupper(static_cast<unsigned char>(s[i + k])) == word[k];
        if (eq && (i + n == s.size() || !(std::isalnum(static_cast<unsigned char>(s[i + n])) || s[i + n] == '_')))
            return i + n;
    }
    return std::string::npos;
}

Outcome runStatement(const std::string& stmt, Engine& engine, const std::vector<std::string>* params,
                     bool describeOnly, SessionState& session);

bool runCursorCommand(const std::string& stmt, Engine& engine, const std::vector<std::string>* params,
                      bool describeOnly, SessionState& session, Outcome& out) {
    const auto w = words(stmt, 1);
    const std::string first = w.empty() ? "" : w[0];
    if (first != "DECLARE" && first != "FETCH" && first != "CLOSE" && first != "MOVE") return false;

    std::vector<sql::Token> toks;
    try {
        // Only the command head is tokenized; DECLARE's query is handed on verbatim.
        const size_t headEnd = first == "DECLARE" ? afterWord(stmt, "CURSOR", 0) : stmt.size();
        toks = sql::tokenize(stmt.substr(0, headEnd == std::string::npos ? stmt.size() : headEnd));
    } catch (const std::invalid_argument& ex) {
        throw PgError("42601", ex.what());
    }
    if (toks.size() < 2) throw PgError("42601", "incomplete " + first + " command");

    if (first == "DECLARE") {
        const std::string name = cursorName(toks[1]);
        const size_t cur = afterWord(stmt, "CURSOR", 0);
        const size_t forPos = cur == std::string::npos ? cur : afterWord(stmt, "FOR", cur);
        if (forPos == std::string::npos) throw PgError("42601", "expected DECLARE name CURSOR FOR query");
        const std::string query = stmt.substr(forPos);
        out.tag = "DECLARE CURSOR";
        if (describeOnly) return true;
        if (session.cursors.count(name)) throw PgError("42P03", "cursor \"" + name + "\" already exists");
        Cursor c;
        c.outcome = runStatement(query, engine, params, false, session);
        if (!c.outcome.returnsRows) throw PgError("42P11", "cursor query must return rows");
        session.cursors[name] = std::move(c);
        return true;
    }

    if (first == "CLOSE") {
        out.tag = "CLOSE CURSOR";
        if (describeOnly) return true;
        if (toks[1].kind == sql::TokenKind::Identifier && sql::equalsIgnoreCase(toks[1].text, "ALL")) {
            session.cursors.clear();
        } else if (!session.cursors.erase(cursorName(toks[1]))) {
            throw PgError("34000", "cursor \"" + cursorName(toks[1]) + "\" does not exist");
        }
        return true;
    }

    // FETCH | MOVE [FORWARD | NEXT] [count | ALL] [FROM | IN] name
    size_t i = 1;
    uint64_t count = 1;
    bool all = false;
    auto isKw = [&](const char* kw) {
        return i < toks.size() && toks[i].kind == sql::TokenKind::Identifier && sql::equalsIgnoreCase(toks[i].text, kw);
    };
    if (isKw("FORWARD") || isKw("NEXT")) ++i;
    if (i < toks.size() && toks[i].kind == sql::TokenKind::Number && allDigits(toks[i].text)) {
        count = std::stoull(toks[i++].text);
    } else if (isKw("ALL")) {
        all = true;
        ++i;
    }
    if (isKw("FROM") || isKw("IN")) ++i;
    if (i >= toks.size() || toks[i].kind == sql::TokenKind::End) throw PgError("42601", "expected cursor name");
    const std::string name = cursorName(toks[i]);
    auto it = session.cursors.find(name);
    if (it == session.cursors.end()) throw PgError("34000", "cursor \"" + name + "\" does not exist");
    Cursor& c = it->second;

    const auto& rows = c.outcome.result.rows;
    const size_t end = all ? rows.size() : static_cast<size_t>(std::min<uint64_t>(rows.size(), c.pos + count));
    if (first == "MOVE") {
        out.tag = "MOVE " + std::to_string(describeOnly ? 0 : end - c.pos);
        if (!describeOnly) c.pos = end;
        return true;
    }
    out.returnsRows = true;
    out.result.headers = c.outcome.result.headers;
    out.result.types = c.outcome.result.types;
    out.tag = "FETCH";
    out.tagCountsRows = true;
    if (describeOnly) return true;
    out.result.rows.assign(rows.begin() + static_cast<std::ptrdiff_t>(c.pos), rows.begin() + static_cast<std::ptrdiff_t>(end));
    c.pos = end;
    return true;
}

// Runs (or, with describeOnly, just describes) one statement. Throws PgError.
Outcome runStatement(const std::string& stmt, Engine& engine, const std::vector<std::string>* params,
                     bool describeOnly, SessionState& session) {
    Outcome out;
    if (runSpecial(stmt, out)) return out;
    if (runCursorCommand(stmt, engine, params, describeOnly, session, out)) return out;

    const auto w = words(stmt, 1);
    const std::string first = w.empty() ? "" : w[0];
    try {
        MiniSQLResult r = describeOnly ? describeMiniSQL(engine, stmt, params) : executeMiniSQL(engine, stmt, params);
        const std::string affected = (r.headers.size() == 1 && r.headers[0] == "rows_affected" && !r.rows.empty())
                                         ? r.rows[0][0]
                                         : "0";
        if (first == "INSERT") {
            out.tag = "INSERT 0 " + affected;
        } else if (first == "DELETE") {
            out.tag = "DELETE " + affected;
        } else if (first == "UPDATE") {
            out.tag = "UPDATE " + affected;
        } else if (first == "COPY") {
            out.tag = "COPY " + affected;
        } else if (first == "CREATE") {
            out.tag = "CREATE TABLE";
        } else {
            out.returnsRows = true;
            out.result = std::move(r);
            if (first == "EXPLAIN") {
                out.tag = "EXPLAIN";
            } else {
                out.tag = "SELECT";
                out.tagCountsRows = true;
            }
        }
        return out;
    } catch (const std::invalid_argument& ex) {
        throw PgError(sqlStateFor(ex.what(), true), ex.what());
    } catch (const PgError&) {
        throw;
    } catch (const std::exception& ex) {
        throw PgError(sqlStateFor(ex.what(), false), ex.what());
    }
}

std::string completeTag(const Outcome& o, size_t rowsSent) {
    return o.tagCountsRows ? o.tag + " " + std::to_string(rowsSent) : o.tag;
}

std::string handleQuery(const std::string& sql, Engine& engine, SessionState& session) {
    std::string out;
    bool any = false;
    for (const auto& stmt : splitStatements(sql)) {
        if (isBlank(stmt)) continue;
        any = true;
        try {
            const Outcome o = runStatement(stmt, engine, nullptr, false, session);
            for (const auto& n : o.notices) out += notice(n);
            if (o.returnsRows) {
                out += rowDescription(o.result.headers, o.result.types);
                for (const auto& row : o.result.rows) out += dataRow(row, o.result.types);
            }
            out += commandComplete(completeTag(o, o.result.rows.size()));
        } catch (const PgError& ex) {
            out += errorResponse("ERROR", ex.sqlstate, ex.what());
            break;  // an error aborts the rest of the Query message
        }
    }
    if (!any) out += Msg('I').str();  // EmptyQueryResponse
    out += readyForQuery();
    return out;
}

// ── extended query protocol ──────────────────────────────────────────────────

struct PreparedStatement {
    std::string sql;               // single statement (may be blank)
    std::vector<int32_t> paramOids;  // as declared by the client (0 = unspecified)
    size_t paramCount = 0;
};

struct Portal {
    std::string sql;
    std::vector<std::string> params;
    std::vector<int16_t> resultFormats;
    bool executed = false;
    bool empty = false;
    Outcome outcome;
    size_t cursor = 0;             // rows already sent (for row-limited Execute)
};

// Reads big-endian ints / C strings from a message body.
class Reader {
public:
    explicit Reader(const std::string& b) : b_(b) {}
    int16_t i16() {
        need(2);
        const int16_t v = static_cast<int16_t>((uint16_t(uint8_t(b_[pos_])) << 8) | uint8_t(b_[pos_ + 1]));
        pos_ += 2;
        return v;
    }
    int32_t i32() {
        need(4);
        const int32_t v = readI32(b_, pos_);
        pos_ += 4;
        return v;
    }
    std::string cstr() {
        const size_t end = b_.find('\0', pos_);
        if (end == std::string::npos) throw PgError("08P01", "malformed message: unterminated string");
        std::string s = b_.substr(pos_, end - pos_);
        pos_ = end + 1;
        return s;
    }
    std::string bytes(size_t n) {
        need(n);
        std::string s = b_.substr(pos_, n);
        pos_ += n;
        return s;
    }
    char byte() {
        need(1);
        return b_[pos_++];
    }

private:
    void need(size_t n) const {
        if (pos_ + n > b_.size()) throw PgError("08P01", "malformed message: truncated");
    }
    const std::string& b_;
    size_t pos_ = 0;
};

int64_t beInt(const std::string& b) {
    uint64_t v = 0;
    for (unsigned char c : b) v = (v << 8) | c;
    // sign-extend from b.size() bytes
    const int bits = static_cast<int>(b.size()) * 8;
    if (bits < 64 && (v >> (bits - 1)) & 1) v |= ~uint64_t(0) << bits;
    return static_cast<int64_t>(v);
}

// Decodes one Bind parameter into the text form the SQL layer binds.
std::string decodeParam(const std::string& raw, int16_t format, int32_t oid) {
    if (format == 0) return raw;
    switch (oid) {
        case 21:  // int2
        case 23:  // int4
        case 20:  // int8
            if (raw.size() != (oid == 21 ? 2u : oid == 23 ? 4u : 8u))
                throw PgError("22P03", "invalid binary integer parameter length");
            return std::to_string(beInt(raw));
        case 700: {  // float4
            if (raw.size() != 4) throw PgError("22P03", "invalid binary float4 parameter length");
            const uint32_t bits = static_cast<uint32_t>(beInt(raw));
            float f;
            std::memcpy(&f, &bits, 4);
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%.9g", static_cast<double>(f));
            return buf;
        }
        case 701: {  // float8
            if (raw.size() != 8) throw PgError("22P03", "invalid binary float8 parameter length");
            const uint64_t bits = static_cast<uint64_t>(beInt(raw));
            double d;
            std::memcpy(&d, &bits, 8);
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%.17g", d);
            return buf;
        }
        case 16:  // bool
            return (!raw.empty() && raw[0]) ? "1" : "0";
        case 25: case 1043: case 1042: case 19: case 705: case 0:  // text-like / unknown: bytes as-is
            return raw;
        default:
            throw PgError("0A000", "binary format for parameter type OID " + std::to_string(oid) +
                                       " is not supported; send it as text");
    }
}

class ExtendedSession {
public:
    ExtendedSession(Engine& engine, SessionState& session) : engine_(engine), session_(session) {}

    // Returns the bytes to send for one extended-protocol message. While in the
    // error state everything but Sync is discarded, as the protocol requires.
    std::string handle(char type, const std::string& body) {
        if (failed_ && type != 'S') return "";
        try {
            switch (type) {
                case 'P': return parse(body);
                case 'B': return bind(body);
                case 'D': return describe(body);
                case 'E': return execute(body);
                case 'C': return close(body);
                case 'H': return "";  // Flush: we never buffer responses
                case 'S':
                    failed_ = false;
                    portals_.erase("");  // end of implicit transaction closes the unnamed portal
                    return readyForQuery();
            }
            throw PgError("08P01", std::string("unexpected message type '") + type + "'");
        } catch (const PgError& ex) {
            failed_ = true;
            return errorResponse("ERROR", ex.sqlstate, ex.what());
        } catch (const std::exception& ex) {
            failed_ = true;
            return errorResponse("ERROR", "XX000", ex.what());
        }
    }

    void resetForSimpleQuery() {
        failed_ = false;
        portals_.erase("");
    }

private:
    std::string parse(const std::string& body) {
        Reader r(body);
        PreparedStatement ps;
        const std::string name = r.cstr();
        ps.sql = r.cstr();
        const int16_t n = r.i16();
        for (int i = 0; i < n; ++i) ps.paramOids.push_back(r.i32());

        const auto parts = splitStatements(ps.sql);
        size_t nonBlank = 0;
        for (const auto& p : parts) nonBlank += isBlank(p) ? 0 : 1;
        if (nonBlank > 1) throw PgError("42601", "cannot insert multiple commands into a prepared statement");
        for (const auto& p : parts)
            if (!isBlank(p)) ps.sql = p;
        try {
            ps.paramCount = std::max<size_t>(sql::countParams(ps.sql), ps.paramOids.size());
        } catch (const std::invalid_argument& ex) {
            throw PgError("42601", ex.what());
        }
        if (!name.empty() && statements_.count(name))
            throw PgError("42P05", "prepared statement \"" + name + "\" already exists");
        statements_[name] = std::move(ps);
        return Msg('1').str();  // ParseComplete
    }

    std::string bind(const std::string& body) {
        Reader r(body);
        const std::string portalName = r.cstr();
        const std::string stmtName = r.cstr();
        auto it = statements_.find(stmtName);
        if (it == statements_.end())
            throw PgError("26000", "prepared statement \"" + stmtName + "\" does not exist");
        const PreparedStatement& ps = it->second;

        std::vector<int16_t> paramFormats(static_cast<size_t>(std::max<int16_t>(r.i16(), 0)));
        for (auto& f : paramFormats) f = r.i16();
        const int16_t nparams = r.i16();
        if (static_cast<size_t>(nparams) != ps.paramCount)
            throw PgError("08P01", "bind message supplies " + std::to_string(nparams) + " parameters, but prepared "
                                   "statement requires " + std::to_string(ps.paramCount));
        Portal portal;
        portal.sql = ps.sql;
        portal.empty = isBlank(ps.sql);
        for (int i = 0; i < nparams; ++i) {
            const int32_t len = r.i32();
            if (len < 0) throw PgError("0A000", "NULL parameter values are not supported (MetalDB has no NULLs)");
            const std::string raw = r.bytes(static_cast<size_t>(len));
            const int16_t fmt = formatFor(paramFormats, static_cast<size_t>(i));
            const int32_t oid = static_cast<size_t>(i) < ps.paramOids.size() ? ps.paramOids[i] : 0;
            portal.params.push_back(decodeParam(raw, fmt, oid));
        }
        const int16_t nres = r.i16();
        for (int i = 0; i < nres; ++i) portal.resultFormats.push_back(r.i16());
        for (int16_t f : portal.resultFormats)
            if (f != 0 && f != 1) throw PgError("08P01", "invalid result format code");
        portals_[portalName] = std::move(portal);
        return Msg('2').str();  // BindComplete
    }

    std::string describe(const std::string& body) {
        Reader r(body);
        const char what = r.byte();
        const std::string name = r.cstr();
        if (what == 'S') {
            auto it = statements_.find(name);
            if (it == statements_.end())
                throw PgError("26000", "prepared statement \"" + name + "\" does not exist");
            const PreparedStatement& ps = it->second;
            Msg pd('t');  // ParameterDescription: undeclared parameters are reported as text
            pd.i16(static_cast<int16_t>(ps.paramCount));
            for (size_t i = 0; i < ps.paramCount; ++i)
                pd.i32(i < ps.paramOids.size() && ps.paramOids[i] != 0 ? ps.paramOids[i] : 25);
            if (isBlank(ps.sql)) return pd.str() + Msg('n').str();
            const Outcome shape = runStatement(ps.sql, engine_, nullptr, /*describeOnly=*/true, session_);
            return pd.str() + shapeMessage(shape, {});
        }
        if (what == 'P') {
            if (!portals_.count(name) && session_.cursors.count(name))  // DECLAREd cursors are portals too
                return shapeMessage(session_.cursors[name].outcome, {});
            Portal& p = portal(name);
            if (p.empty) return Msg('n').str();
            if (!p.executed) {
                const Outcome shape = runStatement(p.sql, engine_, &p.params, /*describeOnly=*/true, session_);
                return shapeMessage(shape, p.resultFormats);
            }
            return shapeMessage(p.outcome, p.resultFormats);
        }
        throw PgError("08P01", "invalid Describe target");
    }

    std::string execute(const std::string& body) {
        Reader r(body);
        const std::string name = r.cstr();
        const int32_t maxRows = r.i32();
        if (!portals_.count(name) && session_.cursors.count(name)) {
            Cursor& c = session_.cursors[name];
            const auto& rows = c.outcome.result.rows;
            const size_t end = maxRows > 0 ? std::min(rows.size(), c.pos + static_cast<size_t>(maxRows)) : rows.size();
            std::string out;
            const size_t start = c.pos;
            for (; c.pos < end; ++c.pos) out += dataRow(rows[c.pos], c.outcome.result.types);
            if (c.pos < rows.size()) return out + Msg('s').str();
            return out + commandComplete("FETCH " + std::to_string(c.pos - start));
        }
        Portal& p = portal(name);
        if (p.empty) return Msg('I').str();  // EmptyQueryResponse

        std::string out;
        if (!p.executed) {
            p.outcome = runStatement(p.sql, engine_, &p.params, false, session_);
            p.executed = true;
            for (const auto& n : p.outcome.notices) out += notice(n);
        }
        const Outcome& o = p.outcome;
        if (o.returnsRows) {
            const size_t total = o.result.rows.size();
            const size_t limit = maxRows > 0 ? std::min(total, p.cursor + static_cast<size_t>(maxRows)) : total;
            for (; p.cursor < limit; ++p.cursor) out += dataRow(o.result.rows[p.cursor], o.result.types, p.resultFormats);
            if (p.cursor < total) return out + Msg('s').str();  // PortalSuspended
            return out + commandComplete(completeTag(o, total));
        }
        return out + commandComplete(completeTag(o, 0));
    }

    std::string close(const std::string& body) {
        Reader r(body);
        const char what = r.byte();
        const std::string name = r.cstr();
        if (what == 'S') statements_.erase(name);
        else if (what == 'P') portals_.erase(name);
        else throw PgError("08P01", "invalid Close target");
        return Msg('3').str();  // CloseComplete
    }

    std::string shapeMessage(const Outcome& o, const std::vector<int16_t>& formats) {
        if (!o.returnsRows) return Msg('n').str();  // NoData
        return rowDescription(o.result.headers, o.result.types, formats);
    }

    Portal& portal(const std::string& name) {
        auto it = portals_.find(name);
        if (it == portals_.end()) throw PgError("34000", "portal \"" + name + "\" does not exist");
        return it->second;
    }

    Engine& engine_;
    SessionState& session_;
    std::map<std::string, PreparedStatement> statements_;
    std::map<std::string, Portal> portals_;
    bool failed_ = false;
};

// Grows `buf` to at least `n` bytes. False on EOF / error / idle timeout / shutdown,
// with the reason left in `why`.
bool fill(SessionIO& io, std::string& buf, size_t n, SessionIO::Status& why) {
    while (buf.size() < n) {
        why = io.readSome(buf);
        if (why != SessionIO::Status::Data) return false;
    }
    return true;
}

bool constantTimeEquals(const std::string& a, const std::string& b) {
    unsigned char diff = static_cast<unsigned char>(a.size() != b.size());
    for (size_t i = 0; i < a.size() && i < b.size(); ++i) diff |= static_cast<unsigned char>(a[i] ^ b[i]);
    return diff == 0;
}

}  // namespace

void handlePgSession(SessionIO& io, Engine& engine, const ServerOptions& opts) {
    std::string buf;
    SessionIO::Status idle = SessionIO::Status::Data;  // why the last read stopped
    std::string user = "metaldb";

    // ── startup phase: [SSLRequest|GSSENCRequest]* StartupMessage ──────────────
    while (true) {
        if (!fill(io, buf, 8, idle)) return;
        const int32_t len = readI32(buf, 0);
        if (len < 8 || static_cast<size_t>(len) > 10000) return;
        if (!fill(io, buf, static_cast<size_t>(len), idle)) return;
        const int32_t code = readI32(buf, 4);
        const std::string payload = buf.substr(8, static_cast<size_t>(len) - 8);
        buf.erase(0, static_cast<size_t>(len));

        if (code == kSslRequest || code == kGssEncRequest) {
            if (!io.send("N")) return;  // no encryption; client proceeds in plaintext or gives up
            continue;
        }
        if (code == kCancelRequest) return;  // statements are short; nothing to cancel
        if (code != kProtocolV3) {
            (void)io.send(errorResponse("FATAL", "0A000", "unsupported frontend protocol"));
            return;
        }
        // key\0value\0 ... \0
        size_t pos = 0;
        while (pos < payload.size() && payload[pos] != '\0') {
            const std::string key = payload.c_str() + pos;
            pos += key.size() + 1;
            if (pos > payload.size()) break;
            const std::string value = payload.c_str() + pos;
            pos += value.size() + 1;
            if (key == "user") user = value;
        }
        break;
    }

    // ── authentication ─────────────────────────────────────────────────────────
    if (!opts.password.empty()) {
        if (!io.send(Msg('R').i32(3).str())) return;  // AuthenticationCleartextPassword
        if (!fill(io, buf, 5, idle)) return;
        const int32_t len = readI32(buf, 1);
        if (buf[0] != 'p' || len < 4 || len > 10000) return;
        if (!fill(io, buf, static_cast<size_t>(len) + 1, idle)) return;
        const std::string given(buf.c_str() + 5);
        buf.erase(0, static_cast<size_t>(len) + 1);
        if (!constantTimeEquals(given, opts.password)) {
            (void)io.send(errorResponse("FATAL", "28P01", "password authentication failed for user \"" + user + "\""));
            return;
        }
    }

    std::string hello = Msg('R').i32(0).str();  // AuthenticationOk
    const std::pair<const char*, const char*> params[] = {
        {"server_version", "14.0 (MetalDB)"}, {"server_encoding", "UTF8"}, {"client_encoding", "UTF8"},
        {"DateStyle", "ISO, MDY"}, {"TimeZone", "UTC"}, {"integer_datetimes", "on"},
        {"standard_conforming_strings", "on"}, {"application_name", ""}, {"is_superuser", "off"},
    };
    for (const auto& [k, v] : params) hello += Msg('S').cstr(k).cstr(v).str();
    hello += Msg('K').i32(static_cast<int32_t>(::getpid())).i32(0x4D444221).str();  // BackendKeyData
    hello += readyForQuery();
    if (!io.send(hello)) return;

    // ── query phase ──────────────────────────────────────────────────────────
    SessionState session;
    ExtendedSession ext(engine, session);
    while (true) {
        if (!fill(io, buf, 5, idle)) break;
        const char type = buf[0];
        const int32_t len = readI32(buf, 1);
        if (len < 4 || static_cast<size_t>(len) > opts.maxRequestBytes) {
            (void)io.send(errorResponse("FATAL", "54000", "message too large"));
            return;
        }
        if (!fill(io, buf, static_cast<size_t>(len) + 1, idle)) break;
        const std::string body = buf.substr(5, static_cast<size_t>(len) - 4);
        buf.erase(0, static_cast<size_t>(len) + 1);

        std::string reply;
        switch (type) {
            case 'Q': {
                ext.resetForSimpleQuery();
                const std::string sql(body.c_str());  // NUL-terminated
                reply = handleQuery(sql, engine, session);
                break;
            }
            case 'X':  // Terminate
                return;
            case 'P': case 'B': case 'D': case 'E': case 'C': case 'H': case 'S':
                reply = ext.handle(type, body);
                break;
            default:
                (void)io.send(errorResponse("FATAL", "08P01", std::string("unexpected message type '") + type + "'"));
                return;
        }
        if (!reply.empty() && !io.send(reply)) return;
    }
    if (idle == SessionIO::Status::IdleTimeout)
        (void)io.send(errorResponse("FATAL", "57P05", "terminating connection due to idle timeout"));
    else if (idle == SessionIO::Status::Stopping)
        (void)io.send(errorResponse("FATAL", "57P01", "terminating connection due to administrator command"));
}
