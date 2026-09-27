// Synthesized PostgreSQL catalog (PgCatalog.cpp): replays the exact catalog
// queries psql 16 sends for \dt, \dt+, \d, \d+, \l, \dn and checks the answers,
// plus information_schema and driver probe queries.
#include "../Engine.hpp"
#include "../MiniSQL.hpp"
#include "../PgCatalog.hpp"

#include <cassert>
#include <cstdio>
#include <string>
#include <sys/stat.h>
#include <vector>

#include "psql16_catalog_queries.inc"

namespace {

using Rows = std::vector<std::vector<std::string>>;

const std::string kDir = "/tmp/pg_catalog_test";

void removeTable(const std::string& base, int strCol) {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal"}) std::remove((base + ext).c_str());
    std::remove((base + ".mdb." + std::to_string(strCol) + ".str").c_str());
}

MiniSQLResult ask(Engine& e, const std::string& sql) {
    MiniSQLResult r;
    const bool handled = pgcat::answer(sql, e, "alice", r);
    if (!handled) {
        std::fprintf(stderr, "not handled: %s\n", sql.c_str());
        std::exit(1);
    }
    assert(r.headers.size() == r.types.size());
    for (const auto& row : r.rows) assert(row.size() == r.headers.size());
    return r;
}

const PsqlQuery& psql(const std::string& label) {
    for (const auto& q : kPsql16Queries)
        if (label == q.label) return q;
    std::fprintf(stderr, "no captured query %s\n", label.c_str());
    std::exit(1);
}

}  // namespace

int main() {
    ::mkdir(kDir.c_str(), 0777);
    ::mkdir((kDir + "/sales").c_str(), 0777);
    removeTable(kDir + "/people", 1);
    removeTable(kDir + "/sales/orders", 1);

    Engine e;
    e.setDataDir(kDir);
    executeMiniSQL(e, "CREATE TABLE people (UINT32, STRING, DOUBLE)");  // sorted first → OID 16384
    executeMiniSQL(e, "INSERT INTO people VALUES (1, 'a', 1.5), (2, 'b', 2.5)");
    executeMiniSQL(e, "CREATE TABLE 'sales/orders' (INT64, STRING, FLOAT)");
    const std::string N = pgcat::kNull;

    // \dt and \dt+
    auto r = ask(e, psql("\\dt").sql);
    assert((r.headers == std::vector<std::string>{"Schema", "Name", "Type", "Owner"}));
    assert((r.rows == Rows{{"public", "people", "table", "alice"}, {"public", "sales/orders", "table", "alice"}}));
    r = ask(e, psql("\\dt+").sql);
    assert(r.headers.size() == 8 && r.headers[6] == "Size");
    assert(r.rows.size() == 2 && r.rows[0][4] == "permanent" && r.rows[0][5] == "metaldb" && r.rows[0][7] == N);
    assert(r.rows[0][6].find("kB") != std::string::npos || r.rows[0][6].find("bytes") != std::string::npos);

    // \d people: lookup → details → columns → (empty) policies, statistics, ...
    for (const std::string cmd : {"\\d people", "\\d+ people"}) {
        r = ask(e, psql(cmd + ": lookup").sql);
        assert((r.rows == Rows{{"16384", "public", "people"}}));

        r = ask(e, psql(cmd + ": table").sql);
        assert(r.rows.size() == 1);
        assert(r.headers[0] == "relchecks" && r.rows[0][0] == "0");
        assert(r.headers[1] == "relkind" && r.rows[0][1] == "r");
        for (size_t i = 2; i <= 6; ++i) assert(r.rows[0][i] == "f");
        for (size_t i = 0; i < r.headers.size(); ++i) {
            if (r.headers[i] == "relhasoids") assert(r.rows[0][i] == "f");
            if (r.headers[i] == "relpersistence") assert(r.rows[0][i] == "p");
            if (r.headers[i] == "amname") assert(r.rows[0][i] == "metaldb");
        }

        r = ask(e, psql(cmd + ": columns").sql);
        assert(r.rows.size() == 3);
        assert(r.rows[0][0] == "c0" && r.rows[0][1] == "bigint");
        assert(r.rows[1][0] == "c1" && r.rows[1][1] == "text");
        assert(r.rows[2][0] == "c2" && r.rows[2][1] == "double precision");
        assert(r.rows[0][2] == N);   // no default
        assert(r.rows[0][3] == "t"); // NOT NULL (MetalDB has no NULLs)
        if (cmd == "\\d+ people") {
            for (size_t i = 0; i < r.headers.size(); ++i) {
                if (r.headers[i] == "attstorage") assert(r.rows[1][i] == "x" && r.rows[0][i] == "p");
                if (r.headers[i] == "col_description") assert(r.rows[0][i] == N);
            }
        }

        for (const char* empty : {": policies", ": statistics", ": publications", ": inherits", ": partitions"}) {
            r = ask(e, psql(cmd + empty).sql);
            assert(r.rows.empty() && !r.headers.empty());
        }
    }

    // \l and \dn
    r = ask(e, psql("\\l").sql);
    assert(r.rows.size() == 1 && r.rows[0][0] == "metaldb" && r.rows[0][1] == "alice" && r.rows[0][2] == "UTF8");
    r = ask(e, psql("\\dn").sql);
    assert((r.rows == Rows{{"public", "alice"}}));

    // Nested-path table lookups (\d sales/orders) and non-matching patterns.
    r = ask(e, "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n "
               "ON n.oid = c.relnamespace WHERE c.relname OPERATOR(pg_catalog.~) '^(sales/orders)$' "
               "COLLATE pg_catalog.default AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3");
    assert((r.rows == Rows{{"16385", "public", "sales/orders"}}));
    r = ask(e, "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname OPERATOR(pg_catalog.~) '^(nope)$'");
    assert(r.rows.empty());

    // information_schema
    r = ask(e, "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns "
               "WHERE table_schema = 'public' AND table_name = 'sales/orders'");
    assert((r.rows == Rows{{"sales/orders", "c0", "bigint", "NO"},
                           {"sales/orders", "c1", "text", "NO"},
                           {"sales/orders", "c2", "real", "NO"}}));
    r = ask(e, "SELECT * FROM information_schema.tables");
    assert(r.rows.size() == 2 && r.headers[2] == "table_name" && r.rows[1][3] == "BASE TABLE");
    r = ask(e, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'pg_catalog'");
    assert(r.rows.empty());

    // Probes
    assert(ask(e, "SELECT version()").rows[0][0].find("MetalDB") != std::string::npos);
    assert(ask(e, "select pg_catalog.version()").rows[0][0].rfind("PostgreSQL 14.0", 0) == 0);
    assert(ask(e, "SELECT current_database()").rows[0][0] == "metaldb");
    assert(ask(e, "SELECT current_schema()").rows[0][0] == "public");
    assert(ask(e, "SELECT current_user").rows[0][0] == "alice");
    r = ask(e, "SHOW server_version_num");
    assert(r.headers[0] == "server_version_num" && r.rows[0][0] == "140000");
    assert(ask(e, "SHOW TRANSACTION ISOLATION LEVEL").rows[0][0] == "serializable");

    // Driver type lookups and unknown catalog relations: shaped, sensible.
    r = ask(e, "SELECT t.typname FROM pg_catalog.pg_type t WHERE t.oid = '701'");
    assert((r.rows == Rows{{"float8"}}));
    r = ask(e, "SELECT foo, bar AS \"Baz\" FROM pg_catalog.pg_whatever");
    assert((r.headers == std::vector<std::string>{"foo", "Baz"}) && r.rows.empty());

    // Ordinary MetalDB SQL is not intercepted.
    MiniSQLResult unused;
    assert(!pgcat::answer("SELECT * FROM people", e, "alice", unused));
    assert(!pgcat::answer("INSERT INTO people VALUES (3, 'c', 0)", e, "alice", unused));

    removeTable(kDir + "/people", 1);
    removeTable(kDir + "/sales/orders", 1);
    std::puts("test_pg_catalog: passed");
    return 0;
}
