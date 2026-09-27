// PostgreSQL v3 wire protocol (simple-query subset) over the mini-SQL executor.
//
// Supported: SSL / GSS negotiation (declined with 'N'), StartupMessage, optional
// cleartext password authentication, ParameterStatus + BackendKeyData, simple
// Query messages (multiple ';'-separated statements), RowDescription / DataRow in
// text format with type OIDs derived from MiniSQLResult::types, CommandComplete
// tags, SQLSTATE-coded ErrorResponse, EmptyQueryResponse, Terminate.
//
// Compatibility shims: BEGIN / START TRANSACTION / COMMIT / END / ROLLBACK / SET /
// DISCARD / RESET are accepted as no-ops (every MetalDB statement is already
// atomic and durable on its own; ROLLBACK emits a NOTICE saying nothing was undone),
// and `SELECT <integer>` (connection-pool health checks) answers without a table.
//
// Not supported: the extended query protocol (Parse/Bind/Execute): it gets an
// ErrorResponse and the session resynchronizes at the next Sync. Nor COPY FROM
// STDIN / TO STDOUT (server-side file COPY works), SSL, or SCRAM / MD5 auth.
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "Engine.hpp"
#include "MiniSQL.hpp"
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

std::string rowDescription(const std::vector<std::string>& names, const std::vector<ColType>& types) {
    Msg m('T');
    m.i16(static_cast<int16_t>(names.size()));
    for (size_t i = 0; i < names.size(); ++i) {
        const ColType t = i < types.size() ? types[i] : ColType::STRING;
        m.cstr(names[i]).i32(0).i16(0).i32(oidFor(t)).i16(typeLen(t)).i32(-1).i16(0);
    }
    return m.str();
}

std::string dataRow(const std::vector<std::string>& cells) {
    Msg m('D');
    m.i16(static_cast<int16_t>(cells.size()));
    for (const auto& c : cells) m.i32(static_cast<int32_t>(c.size())).bytes(c);
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

const char* sqlStateFor(const std::exception& ex, bool userError) {
    if (!userError) return "XX000";  // internal_error
    const std::string m = ex.what();
    if (m.find("does not exist") != std::string::npos) return "42P01";         // undefined_table
    if (m.find("already exists") != std::string::npos) return "42P07";         // duplicate_table
    if (m.find("out of bounds") != std::string::npos ||
        m.find("must appear in") != std::string::npos) return "42703";         // undefined_column
    if (m.rfind("expected", 0) == 0 || m.rfind("unexpected", 0) == 0 || m.rfind("unterminated", 0) == 0 ||
        m.rfind("unknown", 0) == 0) return "42601";                           // syntax_error
    if (m.find("range") != std::string::npos) return "22003";                  // numeric_value_out_of_range
    if (m.find("must be relative") != std::string::npos ||
        m.find("'..'") != std::string::npos) return "42501";                   // insufficient_privilege
    return "22023";                                                           // invalid_parameter_value
}

// Returns false if the rest of the Query message must be skipped (error).
bool runStatement(const std::string& stmt, Engine& engine, std::string& out) {
    const auto w = words(stmt, 2);
    const std::string first = w.empty() ? "" : w[0];

    // Transaction / session commands: accepted for driver compatibility.
    if (first == "BEGIN" || (first == "START" && w.size() > 1 && w[1] == "TRANSACTION")) {
        out += commandComplete("BEGIN");
        return true;
    }
    if (first == "COMMIT" || first == "END") {
        out += commandComplete("COMMIT");
        return true;
    }
    if (first == "ROLLBACK" || first == "ABORT") {
        out += notice("MetalDB statements autocommit; ROLLBACK did not undo anything");
        out += commandComplete("ROLLBACK");
        return true;
    }
    if (first == "SET" || first == "RESET" || first == "DISCARD") {
        out += commandComplete(first);
        return true;
    }
    // Health checks: SELECT <integer>
    if (first == "SELECT" && w.size() == 2 && !w[1].empty() &&
        std::all_of(w[1].begin(), w[1].end(), [](char c) { return std::isdigit(static_cast<unsigned char>(c)); }) &&
        words(stmt, 3).size() == 2) {
        out += rowDescription({"?column?"}, {ColType::INT64});
        out += dataRow({w[1]});
        out += commandComplete("SELECT 1");
        return true;
    }

    try {
        const MiniSQLResult r = executeMiniSQL(engine, stmt);
        const std::string affected = (r.headers.size() == 1 && r.headers[0] == "rows_affected" && !r.rows.empty())
                                         ? r.rows[0][0]
                                         : "0";
        if (first == "INSERT") {
            out += commandComplete("INSERT 0 " + affected);
        } else if (first == "DELETE") {
            out += commandComplete("DELETE " + affected);
        } else if (first == "UPDATE") {
            out += commandComplete("UPDATE " + affected);
        } else if (first == "COPY") {
            out += commandComplete("COPY " + affected);
        } else if (first == "CREATE") {
            out += commandComplete("CREATE TABLE");
        } else {
            out += rowDescription(r.headers, r.types);
            for (const auto& row : r.rows) out += dataRow(row);
            out += commandComplete((first == "EXPLAIN" ? "EXPLAIN" : "SELECT ") +
                                   (first == "EXPLAIN" ? std::string() : std::to_string(r.rows.size())));
        }
        return true;
    } catch (const std::invalid_argument& ex) {
        out += errorResponse("ERROR", sqlStateFor(ex, true), ex.what());
    } catch (const std::exception& ex) {
        out += errorResponse("ERROR", sqlStateFor(ex, false), ex.what());
    }
    return false;
}

std::string handleQuery(const std::string& sql, Engine& engine) {
    std::string out;
    bool any = false;
    for (const auto& stmt : splitStatements(sql)) {
        if (isBlank(stmt)) continue;
        any = true;
        if (!runStatement(stmt, engine, out)) break;
    }
    if (!any) out += Msg('I').str();  // EmptyQueryResponse
    out += readyForQuery();
    return out;
}

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
    bool skipUntilSync = false;
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

        switch (type) {
            case 'Q': {
                const std::string sql(body.c_str());  // NUL-terminated
                if (!io.send(handleQuery(sql, engine))) return;
                break;
            }
            case 'X':  // Terminate
                return;
            case 'S':  // Sync: end of an extended-protocol batch
                skipUntilSync = false;
                if (!io.send(readyForQuery())) return;
                break;
            case 'H':  // Flush
                break;
            case 'P': case 'B': case 'E': case 'D': case 'C': case 'F':
                if (!skipUntilSync) {
                    skipUntilSync = true;
                    if (!io.send(errorResponse("ERROR", "0A000",
                                               "extended query protocol is not supported by MetalDB; "
                                               "use the simple query protocol")))
                        return;
                }
                break;
            default:
                (void)io.send(errorResponse("FATAL", "08P01", std::string("unexpected message type '") + type + "'"));
                return;
        }
    }
    if (idle == SessionIO::Status::IdleTimeout)
        (void)io.send(errorResponse("FATAL", "57P05", "terminating connection due to idle timeout"));
    else if (idle == SessionIO::Status::Stopping)
        (void)io.send(errorResponse("FATAL", "57P01", "terminating connection due to administrator command"));
}
