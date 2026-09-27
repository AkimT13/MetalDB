#include "PgCatalog.hpp"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <dirent.h>
#include <functional>
#include <regex>
#include <string>
#include <sys/stat.h>
#include <vector>

#include "Engine.hpp"
#include "Table.hpp"

namespace pgcat {

const char* const kNull = "\x01NULL";

namespace {

constexpr int kFirstOid = 16384;  // first OID Postgres assigns to user objects
constexpr const char* kVersion =
    "PostgreSQL 14.0 (MetalDB: GPU-accelerated column store; PostgreSQL wire protocol)";

// ── text helpers ─────────────────────────────────────────────────────────────

std::string lower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && (std::isspace(static_cast<unsigned char>(s[b - 1])) || s[b - 1] == ';')) --b;
    return s.substr(a, b - a);
}

bool contains(const std::string& haystack, const char* needle) { return haystack.find(needle) != std::string::npos; }

// Splits `s` at top-level (outside quotes / parentheses) occurrences of `sep`.
std::vector<std::string> splitTopLevel(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    int depth = 0;
    char quote = 0;
    for (char ch : s) {
        if (quote) {
            cur.push_back(ch);
            if (ch == quote) quote = 0;
            continue;
        }
        if (ch == '\'' || ch == '"') quote = ch;
        else if (ch == '(') ++depth;
        else if (ch == ')') --depth;
        if (ch == sep && depth == 0) {
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(ch);
        }
    }
    out.push_back(cur);
    return out;
}

// Position of the first top-level whole-word `word` (lower-case) in `lowerSql`.
size_t findTopLevelWord(const std::string& lowerSql, const std::string& word, size_t from = 0) {
    int depth = 0;
    char quote = 0;
    for (size_t i = from; i < lowerSql.size(); ++i) {
        const char ch = lowerSql[i];
        if (quote) {
            if (ch == quote) quote = 0;
            continue;
        }
        if (ch == '\'' || ch == '"') { quote = ch; continue; }
        if (ch == '(') { ++depth; continue; }
        if (ch == ')') { --depth; continue; }
        if (depth != 0 || lowerSql.compare(i, word.size(), word) != 0) continue;
        const bool startOk = i == 0 || !(std::isalnum(static_cast<unsigned char>(lowerSql[i - 1])) || lowerSql[i - 1] == '_');
        const size_t e = i + word.size();
        const bool endOk = e >= lowerSql.size() || !(std::isalnum(static_cast<unsigned char>(lowerSql[e])) || lowerSql[e] == '_');
        if (startOk && endOk) return i;
    }
    return std::string::npos;
}

struct SelectItemText {
    std::string expr;   // lower-case expression text
    std::string name;   // output column name
};

// Select list of a (single) SELECT: items between SELECT and the top-level FROM.
std::vector<SelectItemText> selectList(const std::string& sql) {
    const std::string l = lower(sql);
    const size_t sel = findTopLevelWord(l, "select");
    if (sel == std::string::npos) return {};
    size_t from = findTopLevelWord(l, "from", sel + 6);
    if (from == std::string::npos) from = l.size();
    std::vector<SelectItemText> items;
    for (std::string part : splitTopLevel(sql.substr(sel + 6, from - sel - 6), ',')) {
        part = trim(part);
        if (part.empty()) continue;
        SelectItemText item;
        std::string exprText = part;
        static const std::regex aliasRe(R"(^([\s\S]*?)\s+(?:[aA][sS]\s+)?("(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_]*)$)");
        std::smatch m;
        const std::string lp = lower(part);
        // Only treat the last word as an alias if an explicit AS precedes it or it is quoted.
        if (std::regex_match(part, m, aliasRe) &&
            (m[2].str()[0] == '"' || std::regex_search(lp, std::regex(R"(\sas\s+[a-z_][a-z0-9_]*$)")))) {
            exprText = m[1].str();
            std::string alias = m[2].str();
            if (alias[0] == '"') alias = alias.substr(1, alias.size() - 2);
            item.name = alias;
        } else {
            // c.relname → relname; pg_catalog.format_type(...) → format_type
            std::string e = trim(part);
            const size_t paren = e.find('(');
            std::string head = paren == std::string::npos ? e : e.substr(0, paren);
            const size_t dot = head.rfind('.');
            head = dot == std::string::npos ? head : head.substr(dot + 1);
            head = trim(head);
            const bool ident = !head.empty() && std::all_of(head.begin(), head.end(), [](char c) {
                return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
            });
            item.name = ident ? lower(head) : "?column?";
        }
        item.expr = lower(trim(exprText));
        items.push_back(item);
    }
    return items;
}

// First single-quoted literal following `key` (lower-case) in `lowerSql`, or "".
std::string literalAfter(const std::string& lowerSql, const std::string& original, const std::string& key) {
    const size_t k = lowerSql.find(key);
    if (k == std::string::npos) return "";
    const size_t q1 = original.find('\'', k + key.size());
    if (q1 == std::string::npos) return "";
    const size_t q2 = original.find('\'', q1 + 1);
    if (q2 == std::string::npos) return "";
    return original.substr(q1 + 1, q2 - q1 - 1);
}

// ── table inventory ──────────────────────────────────────────────────────────

struct TableInfo {
    std::string name;  // relative to the data directory, without .mdb
    int oid = 0;
    std::vector<ColType> types;
    uint64_t rows = 0;
    uint64_t bytes = 0;
};

uint64_t fileSize(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0 ? static_cast<uint64_t>(st.st_size) : 0;
}

void scanDir(const std::string& root, const std::string& rel, int depth, std::vector<std::string>& out) {
    DIR* d = ::opendir((root + (rel.empty() ? "" : "/" + rel)).c_str());
    if (!d) return;
    while (dirent* e = ::readdir(d)) {
        const std::string name = e->d_name;
        if (name == "." || name == "..") continue;
        const std::string relPath = rel.empty() ? name : rel + "/" + name;
        struct stat st{};
        if (::stat((root + "/" + relPath).c_str(), &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) {
            if (depth < 3) scanDir(root, relPath, depth + 1, out);
        } else if (name.size() > 4 && name.compare(name.size() - 4, 4, ".mdb") == 0 &&
                   name.find(".compact-tmp") == std::string::npos) {
            out.push_back(relPath.substr(0, relPath.size() - 4));
        }
    }
    ::closedir(d);
}

std::vector<TableInfo> listTables(Engine& engine) {
    const std::string root = engine.dataDir().empty() ? "." : engine.dataDir();
    std::vector<std::string> names;
    scanDir(root, "", 0, names);
    std::sort(names.begin(), names.end());
    std::vector<TableInfo> tables;
    for (size_t i = 0; i < names.size(); ++i) {
        TableInfo t;
        t.name = names[i];
        t.oid = kFirstOid + static_cast<int>(i);
        try {
            std::lock_guard<std::mutex> lk(engine.tableMutex(t.name));
            Table& table = engine.openTable(t.name);
            t.types = table.columnTypes();
            t.rows = table.rowCount();
            const std::string base = engine.resolveTableBase(t.name);
            t.bytes = fileSize(base + ".mdb") + fileSize(base + ".mdb.idx") + fileSize(base + ".mdb.wal");
            for (size_t c = 0; c < t.types.size(); ++c)
                if (t.types[c] == ColType::STRING) t.bytes += fileSize(base + ".mdb." + std::to_string(c) + ".str");
        } catch (const std::exception&) {
            continue;  // unreadable / foreign file: not a table
        }
        tables.push_back(std::move(t));
    }
    return tables;
}

std::string pgTypeName(ColType t) {
    switch (t) {
        case ColType::UINT32:
        case ColType::INT64: return "bigint";
        case ColType::FLOAT: return "real";
        case ColType::DOUBLE: return "double precision";
        case ColType::STRING: return "text";
    }
    return "text";
}

std::string pgUdtName(ColType t) {
    switch (t) {
        case ColType::UINT32:
        case ColType::INT64: return "int8";
        case ColType::FLOAT: return "float4";
        case ColType::DOUBLE: return "float8";
        case ColType::STRING: return "text";
    }
    return "text";
}

int pgTypeOid(ColType t) {
    switch (t) {
        case ColType::UINT32:
        case ColType::INT64: return 20;
        case ColType::FLOAT: return 700;
        case ColType::DOUBLE: return 701;
        case ColType::STRING: return 25;
    }
    return 25;
}

// pg_size_pretty formatting.
std::string sizePretty(uint64_t bytes) {
    const char* units[] = {"bytes", "kB", "MB", "GB", "TB"};
    double v = static_cast<double>(bytes);
    int u = 0;
    while (u < 4 && v >= 10 * 1024) {
        v /= 1024;
        ++u;
    }
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.0f %s", v, units[u]);
    return buf;
}

// Applies psql's `relname OPERATOR(pg_catalog.~) '^(pattern)$'` (or `~`) filter.
std::function<bool(const std::string&)> relnameFilter(const std::string& lowerSql, const std::string& sql) {
    std::string pattern = literalAfter(lowerSql, sql, "relname operator(pg_catalog.~)");
    if (pattern.empty()) pattern = literalAfter(lowerSql, sql, "relname ~");
    if (pattern.empty()) {
        const std::string eq = literalAfter(lowerSql, sql, "relname =");
        if (eq.empty()) return [](const std::string&) { return true; };
        return [eq](const std::string& n) { return n == eq; };
    }
    try {
        const std::regex re(pattern);
        return [re](const std::string& n) { return std::regex_search(n, re); };
    } catch (const std::regex_error&) {
        return [](const std::string&) { return false; };
    }
}

// ── result building ──────────────────────────────────────────────────────────

MiniSQLResult shaped(const std::vector<SelectItemText>& items) {
    MiniSQLResult r;
    for (const auto& it : items) {
        r.headers.push_back(it.name);
        r.types.push_back(ColType::STRING);
    }
    return r;
}

using ValueFn = std::function<std::string(const SelectItemText&)>;

void addRow(MiniSQLResult& r, const std::vector<SelectItemText>& items, const ValueFn& value) {
    std::vector<std::string> row;
    for (const auto& it : items) row.push_back(value(it));
    r.rows.push_back(std::move(row));
}

// Value for a pg_class-shaped expression describing table `t`.
std::string classValue(const SelectItemText& it, const TableInfo& t, const std::string& user) {
    const std::string e = it.expr + " " + lower(it.name);  // e.g. `false AS relhasoids`
    const std::string n = lower(it.name);
    if (n == "schema" || contains(e, "nspname")) return "public";
    if (n == "name" || contains(e, "relname")) return t.name;
    if (n == "type" || contains(e, "case c.relkind")) return "table";
    if (n == "owner" || contains(e, "pg_get_userbyid") || contains(e, "relowner")) return user;
    if (n == "persistence" || contains(e, "case c.relpersistence")) return "permanent";
    if (n == "access method" || contains(e, "amname")) return "metaldb";
    if (n == "size" || contains(e, "pg_size_pretty")) return sizePretty(t.bytes);
    if (n == "description" || contains(e, "obj_description")) return kNull;
    if (contains(e, "c.oid") || e == "oid") return std::to_string(t.oid);
    if (contains(e, "relchecks")) return "0";
    if (contains(e, "relkind")) return "r";
    if (contains(e, "relhasindex") || contains(e, "relhasrules") || contains(e, "relhastriggers") ||
        contains(e, "relrowsecurity") || contains(e, "relforcerowsecurity") || contains(e, "relhasoids") ||
        contains(e, "relispartition") || contains(e, "relhassubclass"))
        return "f";
    if (contains(e, "reloptions")) return "";
    if (contains(e, "reltablespace")) return "0";
    if (contains(e, "reloftype")) return "";
    if (contains(e, "relpersistence")) return "p";
    if (contains(e, "relreplident")) return "d";
    if (contains(e, "reltuples")) return std::to_string(t.rows);
    if (contains(e, "relam")) return "2";
    return kNull;
}

// Value for a pg_attribute-shaped expression describing column `col` of `t`.
std::string attributeValue(const SelectItemText& it, const TableInfo& t, size_t col) {
    const std::string e = it.expr + " " + lower(it.name);
    const ColType type = t.types[col];
    // Function calls first: their arguments mention other attributes (attnum...).
    if (contains(e, "col_description")) return kNull;
    if (contains(e, "format_type")) return pgTypeName(type);
    if (contains(e, "pg_get_expr") || contains(e, "adbin")) return kNull;  // no defaults
    if (contains(e, "attname")) return "c" + std::to_string(col);
    if (contains(e, "attnotnull")) return "t";                             // no NULLs in MetalDB
    if (contains(e, "collname") || contains(e, "attcollation")) return kNull;
    if (contains(e, "attidentity") || contains(e, "attgenerated")) return "";
    if (contains(e, "attnum")) return std::to_string(col + 1);
    if (contains(e, "atttypid")) return std::to_string(pgTypeOid(type));
    if (contains(e, "atttypmod")) return "-1";
    if (contains(e, "attstorage")) return type == ColType::STRING ? "x" : "p";
    if (contains(e, "typstorage")) return type == ColType::STRING ? "x" : "p";
    if (contains(e, "attcompression")) return "";
    if (contains(e, "attstattarget")) return "-1";
    if (contains(e, "attfdwoptions")) return kNull;
    if (contains(e, "attisdropped") || contains(e, "atthasdef")) return "f";
    return kNull;
}

// information_schema.columns / .tables value by column name.
std::string infoSchemaValue(const std::string& column, const TableInfo& t, size_t col, bool columnsView) {
    const std::string c = lower(column);
    if (c == "table_catalog") return "metaldb";
    if (c == "table_schema") return "public";
    if (c == "table_name") return t.name;
    if (c == "table_type") return "BASE TABLE";
    if (!columnsView) {
        if (c == "is_insertable_into") return "YES";
        if (c == "is_typed") return "NO";
        return kNull;
    }
    const ColType type = t.types[col];
    if (c == "column_name") return "c" + std::to_string(col);
    if (c == "ordinal_position") return std::to_string(col + 1);
    if (c == "data_type") return pgTypeName(type);
    if (c == "udt_name") return pgUdtName(type);
    if (c == "udt_schema") return "pg_catalog";
    if (c == "is_nullable") return "NO";
    if (c == "column_default") return kNull;
    if (c == "numeric_precision")
        return type == ColType::STRING ? kNull : (type == ColType::FLOAT ? "24" : type == ColType::DOUBLE ? "53" : "64");
    if (c == "is_identity" || c == "is_generated") return c == "is_generated" ? "NEVER" : "NO";
    return kNull;
}

bool matchesStringFilter(const std::string& lowerSql, const std::string& sql, const std::string& key,
                         const std::string& value) {
    const std::string want = literalAfter(lowerSql, sql, key);
    return want.empty() || want == value;
}

}  // namespace

bool answer(const std::string& rawSql, Engine& engine, const std::string& user, MiniSQLResult& out) {
    const std::string sql = trim(rawSql);
    const std::string l = lower(sql);
    auto oneValue = [&](const std::string& name, const std::string& value) {
        out = MiniSQLResult{};
        out.headers = {name};
        out.types = {ColType::STRING};
        out.rows = {{value}};
        return true;
    };

    // ── scalar probes ────────────────────────────────────────────────────────
    static const std::regex fnProbe(R"(^select\s+(pg_catalog\.)?(version|current_database|current_schema|current_user|session_user|user)\s*(\(\s*\))?\s*(as\s+("?)(\w+)\5)?$)");
    std::smatch m;
    if (std::regex_match(l, m, fnProbe)) {
        const std::string fn = m[2].str();
        const std::string alias = m[6].matched ? m[6].str() : fn;
        if (fn == "version") return oneValue(alias, kVersion);
        if (fn == "current_database") return oneValue(alias, "metaldb");
        if (fn == "current_schema") return oneValue(alias, "public");
        return oneValue(alias, user);
    }
    static const std::regex showRe(R"(^show\s+(\w+(\s+\w+)*)$)");
    if (std::regex_match(l, m, showRe)) {
        const std::string what = m[1].str();
        std::string value = "";
        if (what == "server_version") value = "14.0";
        else if (what == "server_version_num") value = "140000";
        else if (what == "transaction isolation level" || what == "transaction_isolation") value = "serializable";
        else if (what == "standard_conforming_strings") value = "on";
        else if (what == "client_encoding" || what == "server_encoding") value = "UTF8";
        else if (what == "timezone") value = "UTC";
        else if (what == "datestyle") value = "ISO, MDY";
        else if (what == "search_path") value = "public";
        else if (what == "max_identifier_length") value = "63";
        std::string col = what;
        std::replace(col.begin(), col.end(), ' ', '_');
        return oneValue(col, value);
    }

    const bool catalog = contains(l, "pg_catalog.") || contains(l, "information_schema.") ||
                         std::regex_search(l, std::regex(R"(\bfrom\s+pg_[a-z_]+)"));
    if (!catalog || findTopLevelWord(l, "select") == std::string::npos) return false;

    const auto items = selectList(sql);
    out = shaped(items);
    const size_t fromPos = findTopLevelWord(l, "from");
    const std::string fromClause = fromPos == std::string::npos ? "" : l.substr(fromPos);
    const std::string where = [&] {
        const size_t w = findTopLevelWord(l, "where");
        return w == std::string::npos ? std::string() : l.substr(w);
    }();

    // Relations that describe things MetalDB does not have: always empty.
    for (const char* none : {"pg_index", "pg_constraint", "pg_trigger", "pg_policy", "pg_statistic_ext",
                             "pg_publication", "pg_inherits", "pg_rewrite", "pg_foreign", "pg_partitioned",
                             "pg_extension", "pg_proc", "pg_event_trigger", "pg_matviews", "pg_views",
                             "pg_sequence", "pg_description", "pg_shdescription", "pg_depend", "pg_roles",
                             "pg_auth", "pg_user", "pg_settings", "pg_stat", "pg_locks", "pg_cursors"}) {
        if (contains(fromClause, none)) return true;
    }

    // information_schema.tables / .columns
    if (contains(fromClause, "information_schema.tables") || contains(fromClause, "information_schema.columns")) {
        const bool columnsView = contains(fromClause, "information_schema.columns");
        const bool star = items.size() == 1 && items[0].expr == "*";
        std::vector<std::string> cols;
        if (star) {
            cols = columnsView ? std::vector<std::string>{"table_catalog", "table_schema", "table_name", "column_name",
                                                          "ordinal_position", "column_default", "is_nullable",
                                                          "data_type", "numeric_precision", "udt_name"}
                               : std::vector<std::string>{"table_catalog", "table_schema", "table_name", "table_type"};
            out = MiniSQLResult{};
            for (const auto& c : cols) {
                out.headers.push_back(c);
                out.types.push_back(ColType::STRING);
            }
        } else {
            for (const auto& it : items) {
                const size_t dot = it.expr.rfind('.');
                cols.push_back(dot == std::string::npos ? it.expr : it.expr.substr(dot + 1));
            }
        }
        if (!matchesStringFilter(l, sql, "table_schema =", "public") ||
            !matchesStringFilter(l, sql, "table_catalog =", "metaldb"))
            return true;
        for (const auto& t : listTables(engine)) {
            if (!matchesStringFilter(l, sql, "table_name =", t.name)) continue;
            const size_t n = columnsView ? t.types.size() : 1;
            for (size_t c = 0; c < n; ++c) {
                if (columnsView && !matchesStringFilter(l, sql, "column_name =", "c" + std::to_string(c))) continue;
                std::vector<std::string> row;
                for (const auto& col : cols) row.push_back(infoSchemaValue(col, t, c, columnsView));
                out.rows.push_back(std::move(row));
            }
        }
        return true;
    }

    // \l
    if (contains(fromClause, "pg_database")) {
        addRow(out, items, [&](const SelectItemText& it) -> std::string {
            const std::string n = lower(it.name);
            if (n == "name" || contains(it.expr, "datname")) return "metaldb";
            if (n == "owner" || contains(it.expr, "datdba")) return user;
            if (n == "encoding" || contains(it.expr, "encoding")) return "UTF8";
            if (n == "locale provider") return "libc";
            if (n == "collate" || n == "ctype" || contains(it.expr, "datcollate") || contains(it.expr, "datctype"))
                return "C";
            return kNull;
        });
        return true;
    }

    // \dn
    if (contains(fromClause, "pg_namespace n") && !contains(fromClause, "pg_class")) {
        addRow(out, items, [&](const SelectItemText& it) -> std::string {
            const std::string n = lower(it.name);
            if (n == "name" || contains(it.expr, "nspname")) return "public";
            if (n == "owner" || contains(it.expr, "nspowner")) return user;
            if (contains(it.expr, "oid")) return "2200";
            return kNull;
        });
        return true;
    }

    // pg_attribute: one row per column of the table named by attrelid.
    if (contains(fromClause, "pg_attribute")) {
        const std::string rel = literalAfter(l, sql, "attrelid =");
        for (const auto& t : listTables(engine)) {
            if (!rel.empty() && rel != std::to_string(t.oid)) continue;
            for (size_t c = 0; c < t.types.size(); ++c)
                addRow(out, items, [&](const SelectItemText& it) { return attributeValue(it, t, c); });
        }
        return true;
    }

    // pg_type lookups by OID (drivers resolving result types).
    if (contains(fromClause, "pg_type")) {
        const std::string oid = literalAfter(l, sql, "oid =");
        const std::vector<std::pair<std::string, std::string>> known = {
            {"20", "int8"}, {"25", "text"}, {"700", "float4"}, {"701", "float8"}, {"16", "bool"}, {"23", "int4"}};
        for (const auto& [o, name] : known) {
            if (!oid.empty() && oid != o) continue;
            addRow(out, items, [&](const SelectItemText& it) -> std::string {
                if (contains(it.expr, "typname")) return name;
                if (contains(it.expr, "nspname")) return "pg_catalog";
                if (it.expr == "oid" || contains(it.expr, "t.oid")) return o;
                if (contains(it.expr, "typtype")) return "b";
                return kNull;
            });
        }
        return true;
    }

    // pg_class: table listings (\dt, \d), name lookups (\d name), and per-table
    // details (WHERE c.oid = '<oid>').
    if (contains(fromClause, "pg_class")) {
        if (contains(where, "relkind in") && !contains(where, "'r'")) return true;  // views, indexes, sequences...
        if (contains(where, "nspname") && contains(where, "'pg_catalog'") && !contains(where, "<> 'pg_catalog'"))
            return true;
        const std::string oid = literalAfter(l, sql, "c.oid =");
        const auto nameOk = relnameFilter(l, sql);
        for (const auto& t : listTables(engine)) {
            if (!oid.empty() && oid != std::to_string(t.oid)) continue;
            if (!nameOk(t.name)) continue;
            addRow(out, items, [&](const SelectItemText& it) { return classValue(it, t, user); });
        }
        return true;
    }

    return true;  // other catalog relations: correctly shaped, empty
}

}  // namespace pgcat
