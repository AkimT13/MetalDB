// SQL joins: SELECT ... FROM a [AS x] [INNER] JOIN b [AS y] ON x.cN = y.cM ...
//
// Covers name resolution (aliases, qualified / unqualified / ambiguous columns,
// alias.*), projection / aggregates / GROUP BY / DISTINCT / ORDER BY / LIMIT over
// joined rows, multi-key and three-way joins, self joins, mixed key types, WHERE
// pushdown (checked through EXPLAIN), transactions (pending rows join), COPY of a
// join, describe, a randomized join against a brute-force reference with the
// hybrid scan forced to the GPU path (CPU-only builds run the CPU path), and
// concurrent joins over the same tables in opposite order (lock ordering).
#include "../Engine.hpp"
#include "../MiniSQL.hpp"
#include "../Session.hpp"
#include "../Table.hpp"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

const char* kDir = "/tmp/mdb_test_sql_join";

using Rows = std::vector<std::vector<std::string>>;

int failures = 0;

void check(bool ok, const std::string& what) {
    if (!ok) {
        std::fprintf(stderr, "FAIL: %s\n", what.c_str());
        ++failures;
    }
}

std::string show(const Rows& rows) {
    std::string out;
    for (const auto& r : rows) {
        out += "  [";
        for (size_t i = 0; i < r.size(); ++i) out += (i ? ", " : "") + r[i];
        out += "]\n";
    }
    return out;
}

void expectRows(Engine& e, const std::string& sql, const Rows& want) {
    Rows got;
    try {
        got = executeMiniSQL(e, sql).rows;
    } catch (const std::exception& ex) {
        check(false, sql + " threw: " + ex.what());
        return;
    }
    if (got != want) check(false, sql + "\n got:\n" + show(got) + " want:\n" + show(want));
}

void expectHeaders(Engine& e, const std::string& sql, const std::vector<std::string>& want) {
    const auto r = executeMiniSQL(e, sql);
    check(r.headers == want, "headers of " + sql);
    const auto d = describeMiniSQL(e, sql);
    check(d.headers == want, "describe headers of " + sql);
    check(d.types == r.types, "describe types of " + sql);
}

void expectError(Engine& e, const std::string& sql, const std::string& needle) {
    try {
        executeMiniSQL(e, sql);
        check(false, sql + " should fail with '" + needle + "'");
    } catch (const std::invalid_argument& ex) {
        check(std::string(ex.what()).find(needle) != std::string::npos,
              sql + ": error '" + ex.what() + "' lacks '" + needle + "'");
    }
}

bool planHas(Engine& e, const std::string& sql, const std::string& needle) {
    for (const auto& row : executeMiniSQL(e, "EXPLAIN " + sql).rows)
        if (row[0].find(needle) != std::string::npos) return true;
    return false;
}

void run(Engine& e, const std::string& sql) { executeMiniSQL(e, sql); }

void setup(Engine& e) {
    std::system((std::string("rm -rf ") + kDir + " && mkdir -p " + kDir).c_str());
    e.setDataDir(kDir);
    run(e, "CREATE TABLE users (c0 UINT32, c1 STRING, c2 INT64)");
    run(e, "CREATE TABLE orders (c0 UINT32, c1 UINT32, c2 DOUBLE, c3 STRING)");
    run(e, "CREATE TABLE items (c0 UINT32, c1 STRING)");
    run(e, "INSERT INTO users VALUES (1, 'ada', 30), (2, 'bob', 41), (3, 'cyd', 25), (4, 'dee', 41)");
    run(e, "INSERT INTO orders VALUES (10, 1, 5.5, 'pen'), (11, 1, 2.5, 'ink'), (12, 2, 9, 'pen'),"
           " (13, 9, 1, 'cap'), (14, 4, 4, 'pad')");
    run(e, "INSERT INTO items VALUES (1, 'pen'), (2, 'ink'), (3, 'pad'), (4, 'pen')");
}

void testBasics(Engine& e) {
    expectRows(e, "SELECT u.c1, o.c0, o.c2 FROM users u JOIN orders o ON u.c0 = o.c1 ORDER BY o.c0",
               {{"ada", "10", "5.5"}, {"ada", "11", "2.5"}, {"bob", "12", "9"}, {"dee", "14", "4"}});
    // ON operands in either order, INNER and AS, table names as default qualifiers.
    expectRows(e, "SELECT users.c1, orders.c0 FROM users INNER JOIN orders ON orders.c1 = users.c0 ORDER BY 2",
               {{"ada", "10"}, {"ada", "11"}, {"bob", "12"}, {"dee", "14"}});
    expectRows(e, "SELECT x.c1 FROM users AS x JOIN orders AS y ON (x.c0 = y.c1) WHERE y.c0 = 12", {{"bob"}});
    // Unqualified columns that exist in only one table resolve to it (orders has c3).
    expectRows(e, "SELECT c3 FROM users u JOIN orders o ON u.c0 = o.c1 ORDER BY c3", {{"ink"}, {"pad"}, {"pen"}, {"pen"}});
    // Rows without a partner (cyd, order 13) are dropped.
    expectRows(e, "SELECT COUNT(*) FROM users u JOIN orders o ON u.c0 = o.c1", {{"4"}});

    expectHeaders(e, "SELECT * FROM users u JOIN items i ON u.c0 = i.c0",
                  {"u.c0", "u.c1", "u.c2", "i.c0", "i.c1"});
    expectHeaders(e, "SELECT o.*, u.c1 FROM users u JOIN orders o ON u.c0 = o.c1",
                  {"o.c0", "o.c1", "o.c2", "o.c3", "u.c1"});
    expectHeaders(e, "SELECT u.c1, COUNT(*), SUM(o.c2) FROM users u JOIN orders o ON u.c0 = o.c1 GROUP BY u.c1",
                  {"u.c1", "count(*)", "sum(o.c2)"});
    expectRows(e, "SELECT i.* FROM users u JOIN items i ON u.c0 = i.c0 WHERE u.c1 = 'cyd'", {{"3", "pad"}});
}

void testShapes(Engine& e) {
    // GROUP BY + aggregates + ORDER BY an aggregate.
    expectRows(e,
               "SELECT u.c1, COUNT(*), SUM(o.c2), MIN(o.c3), MAX(o.c0), AVG(o.c2) FROM users u "
               "JOIN orders o ON u.c0 = o.c1 GROUP BY u.c1 ORDER BY sum(o.c2) DESC",
               {{"bob", "1", "9", "pen", "12", "9"},
                {"ada", "2", "8", "ink", "11", "4"},
                {"dee", "1", "4", "pad", "14", "4"}});
    // Grouping on a column of the other table; ORDER BY a position.
    expectRows(e, "SELECT u.c2, COUNT(o.c0) FROM users u JOIN orders o ON u.c0 = o.c1 GROUP BY u.c2 ORDER BY 1",
               {{"30", "2"}, {"41", "2"}});
    // Scalar aggregates over a join.
    expectRows(e, "SELECT COUNT(*), SUM(u.c2), MAX(o.c2) FROM users u JOIN orders o ON u.c0 = o.c1",
               {{"4", "142", "9"}});
    // DISTINCT across tables.
    expectRows(e, "SELECT DISTINCT o.c3, u.c2 FROM users u JOIN orders o ON u.c0 = o.c1 ORDER BY 1, 2",
               {{"ink", "30"}, {"pad", "41"}, {"pen", "30"}, {"pen", "41"}});
    // ORDER BY a column that is not selected; LIMIT / OFFSET.
    expectRows(e, "SELECT u.c1 FROM users u JOIN orders o ON u.c0 = o.c1 ORDER BY o.c2 DESC LIMIT 2",
               {{"bob"}, {"ada"}});
    expectRows(e, "SELECT o.c0 FROM users u JOIN orders o ON u.c0 = o.c1 ORDER BY o.c0 LIMIT 2 OFFSET 1",
               {{"11"}, {"12"}});
    // A join that matches nothing.
    expectRows(e, "SELECT u.c1 FROM users u JOIN orders o ON u.c0 = o.c1 WHERE o.c2 > 100", {});
    expectRows(e, "SELECT COUNT(*) FROM users u JOIN orders o ON u.c0 = o.c1 WHERE o.c2 > 100", {{"0"}});
}

void testMultiWay(Engine& e) {
    // Two ON conditions (multi-column key): user id and a string column.
    run(e, "CREATE TABLE prefs (c0 UINT32, c1 STRING, c2 UINT32)");
    run(e, "INSERT INTO prefs VALUES (1, 'pen', 7), (1, 'ink', 8), (2, 'cap', 9), (4, 'pad', 6)");
    expectRows(e,
               "SELECT o.c0, p.c2 FROM orders o JOIN prefs p ON o.c1 = p.c0 AND o.c3 = p.c1 ORDER BY o.c0",
               {{"10", "7"}, {"11", "8"}, {"14", "6"}});
    // Three tables; the third joins on a column of the second.
    expectRows(e,
               "SELECT u.c1, o.c0, i.c0 FROM users u JOIN orders o ON u.c0 = o.c1 JOIN items i ON i.c1 = o.c3 "
               "ORDER BY o.c0, i.c0",
               {{"ada", "10", "1"}, {"ada", "10", "4"}, {"ada", "11", "2"}, {"bob", "12", "1"},
                {"bob", "12", "4"}, {"dee", "14", "3"}});
    // UINT32 = INT64 keys compare as integers.
    expectRows(e, "SELECT u.c1 FROM orders o JOIN users u ON u.c2 = o.c0", {});
    run(e, "CREATE TABLE ages (c0 UINT32, c1 STRING)");
    run(e, "INSERT INTO ages VALUES (41, 'forty-one'), (25, 'twenty-five')");
    expectRows(e, "SELECT u.c1, a.c1 FROM users u JOIN ages a ON u.c2 = a.c0 ORDER BY u.c1",
               {{"bob", "forty-one"}, {"cyd", "twenty-five"}, {"dee", "forty-one"}});
    // FLOAT = DOUBLE keys; -0 equals 0.
    run(e, "CREATE TABLE fa (c0 FLOAT)");
    run(e, "CREATE TABLE fb (c0 DOUBLE, c1 STRING)");
    run(e, "INSERT INTO fa VALUES (0.5), (-0), (3)");
    run(e, "INSERT INTO fb VALUES (0.5, 'half'), (0, 'zero'), (2, 'two')");
    expectRows(e, "SELECT b.c1 FROM fa a JOIN fb b ON a.c0 = b.c0 ORDER BY 1", {{"half"}, {"zero"}});
}

void testPushdown(Engine& e) {
    const std::string sql =
        "SELECT u.c1 FROM users u JOIN orders o ON u.c0 = o.c1 WHERE o.c2 > 3 AND u.c2 = 41 AND (u.c1 = 'bob' OR o.c3 = 'pad')";
    expectRows(e, sql, {{"bob"}, {"dee"}});
    // Single-table conjuncts are filters of that table's scan; the OR spanning both
    // tables runs on joined rows.
    check(planHas(e, sql, "Filter: c2 = 41"), "users filter pushed down");
    check(planHas(e, sql, "Filter: c2 > 3"), "orders filter pushed down");
    check(planHas(e, sql, "Filter (joined rows): (u.c1 = 'bob') OR (o.c3 = 'pad')"), "residual filter on joined rows");
    check(planHas(e, sql, "Join: CPU hash join ON u.c0 = o.c1"), "hash join step");
}

void testColumnCompare(Engine& e) {
    // column <op> column works on one table too (CPU scan of both columns)...
    expectRows(e, "SELECT c1 FROM users WHERE c0 < c2 AND c2 <= 30 ORDER BY c1", {{"ada"}, {"cyd"}});
    expectRows(e, "SELECT COUNT(*) FROM orders WHERE c0 = c1", {{"0"}});
    expectRows(e, "SELECT c0 FROM orders WHERE c2 > c1 ORDER BY 1", {{"10"}, {"11"}, {"12"}});  // DOUBLE vs UINT32
    check(planHas(e, "SELECT c1 FROM users WHERE c0 < c2", "c0 < c2: CPU scan comparing two columns"),
          "column compare access path");
    // ...across joined tables (it stays on joined rows), and in a pushed-down conjunct.
    expectRows(e, "SELECT u.c1, o.c0 FROM users u JOIN orders o ON u.c0 = o.c1 WHERE o.c2 < u.c0 ORDER BY 2", {});
    expectRows(e, "SELECT u.c1, o.c0 FROM users u JOIN orders o ON u.c0 = o.c1 WHERE o.c2 >= u.c0 AND o.c0 > o.c2 ORDER BY 2",
               {{"ada", "10"}, {"ada", "11"}, {"bob", "12"}, {"dee", "14"}});
    check(planHas(e, "SELECT u.c1 FROM users u JOIN orders o ON u.c0 = o.c1 WHERE o.c2 >= u.c0 AND o.c0 > o.c2",
                  "Filter: c0 > c2"),
          "single-table column compare pushed down");
    expectRows(e, "SELECT a.c1, b.c1 FROM users a JOIN users b ON a.c2 = b.c2 WHERE a.c0 < b.c0", {{"bob", "dee"}});
    expectError(e, "SELECT c0 FROM users WHERE c1 = c0", "cannot compare STRING c1 with UINT32 c0");
    expectError(e, "SELECT c0 FROM users WHERE c0 = c7", "out of bounds");
    // Pending rows in a transaction are matched row-wise.
    Session s(e);
    s.execute("BEGIN");
    s.execute("INSERT INTO users VALUES (50, 'old', 60)");
    check(s.execute("SELECT c1 FROM users WHERE c0 < c2 AND c2 > 50").rows == Rows({{"old"}}), "column compare on pending row");
    s.execute("ROLLBACK");
}

void testErrors(Engine& e) {
    expectError(e, "SELECT c1 FROM users u JOIN orders o ON u.c0 = o.c1", "ambiguous");
    expectError(e, "SELECT z.c1 FROM users u JOIN orders o ON u.c0 = o.c1", "missing FROM-clause entry for table \"z\"");
    expectError(e, "SELECT users.c1 FROM users u JOIN orders o ON u.c0 = o.c1", "missing FROM-clause entry");
    expectError(e, "SELECT u.c9 FROM users u JOIN orders o ON u.c0 = o.c1", "column u.c9 does not exist");
    expectError(e, "SELECT c9 FROM users u JOIN orders o ON u.c0 = o.c1", "column c9 does not exist");
    expectError(e, "SELECT * FROM users JOIN users ON users.c0 = users.c0", "specified more than once");
    expectError(e, "SELECT * FROM users u LEFT JOIN orders o ON u.c0 = o.c1", "only INNER JOIN");
    expectError(e, "SELECT * FROM users u CROSS JOIN orders o", "only INNER JOIN");
    expectError(e, "SELECT * FROM users u, orders o", "comma-separated");
    expectError(e, "SELECT * FROM users u JOIN orders o USING (c0)", "USING");
    expectError(e, "SELECT * FROM users u JOIN orders o ON u.c0 < o.c1", "joins compare columns for equality");
    expectError(e, "SELECT * FROM users u JOIN orders o ON u.c0 = u.c2", "must compare a column of \"o\"");
    expectError(e, "SELECT * FROM users u JOIN orders o ON u.c1 = o.c1", "cannot join STRING u.c1 with UINT32 o.c1");
    expectError(e, "SELECT * FROM users u JOIN nope n ON u.c0 = n.c0", "table 'nope' does not exist");
    expectError(e, "SELECT u.c1, COUNT(*) FROM users u JOIN orders o ON u.c0 = o.c1", "cannot be mixed");
    expectError(e, "SELECT u.c1 FROM users u JOIN orders o ON u.c0 = o.c1 WHERE o.c3 = 5", "is STRING");
    expectError(e, "SELECT SUM(u.c1) FROM users u JOIN orders o ON u.c0 = o.c1", "requires a numeric column");
    // Single-table statements accept their own qualifier and reject others.
    expectRows(e, "SELECT t.c1 FROM users t WHERE t.c0 = 2 ORDER BY t.c1", {{"bob"}});
    expectRows(e, "SELECT users.c1 FROM users WHERE users.c2 = 25", {{"cyd"}});
    expectError(e, "SELECT x.c1 FROM users t", "missing FROM-clause entry for table \"x\"");
    expectRows(e, "SELECT c1 FROM users LIMIT 1", {{"ada"}});  // LIMIT is not an alias
}

void testTransactionsAndCopy(Engine& e) {
    Session s(e);
    s.execute("BEGIN");
    s.execute("INSERT INTO orders VALUES (20, 3, 7, 'new')");
    s.execute("DELETE FROM users WHERE c0 = 1");
    const auto inTxn = s.execute("SELECT u.c1, o.c0 FROM users u JOIN orders o ON u.c0 = o.c1 ORDER BY o.c0").rows;
    check(inTxn == Rows({{"bob", "12"}, {"dee", "14"}, {"cyd", "20"}}), "join sees pending insert and delete:\n" + show(inTxn));
    // Another session only sees committed rows.
    expectRows(e, "SELECT COUNT(*) FROM users u JOIN orders o ON u.c0 = o.c1", {{"4"}});
    s.execute("ROLLBACK");
    expectRows(e, "SELECT COUNT(*) FROM users u JOIN orders o ON u.c0 = o.c1", {{"4"}});

    run(e, "COPY (SELECT u.c1, o.c0 FROM users u JOIN orders o ON u.c0 = o.c1 ORDER BY o.c0 LIMIT 2) TO 'j.csv' WITH HEADER");
    std::ifstream in(std::string(kDir) + "/j.csv");
    std::stringstream ss;
    ss << in.rdbuf();
    check(ss.str() == "u.c1,o.c0\nada,10\nada,11\n", "COPY of a join: " + ss.str());
}

// Randomized: A(key UINT32, val INT64, tag STRING) x B(key INT64, flag UINT32),
// filtered on both sides, checked against nested loops, with the scan forced onto
// the GPU path (threshold 0) and onto the CPU.
void testRandomized(Engine& e) {
    run(e, "CREATE TABLE ra (c0 UINT32, c1 INT64, c2 STRING)");
    run(e, "CREATE TABLE rb (c0 INT64, c1 UINT32)");
    std::mt19937 rng(42);
    struct A { uint32_t key; int64_t val; std::string tag; };
    struct B { int64_t key; uint32_t flag; };
    std::vector<A> as;
    std::vector<B> bs;
    for (int i = 0; i < 6000; ++i) as.push_back({rng() % 500, static_cast<int64_t>(rng() % 1000) - 500, "t" + std::to_string(rng() % 7)});
    for (int i = 0; i < 1500; ++i) bs.push_back({static_cast<int64_t>(rng() % 600), rng() % 10});
    auto insertAll = [&](const std::string& table, const std::vector<std::string>& values) {
        for (size_t i = 0; i < values.size(); i += 500) {
            std::string sql = "INSERT INTO " + table + " VALUES ";
            for (size_t j = i; j < values.size() && j < i + 500; ++j) sql += (j > i ? "," : "") + values[j];
            run(e, sql);
        }
    };
    std::vector<std::string> va, vb;
    for (const auto& a : as) va.push_back("(" + std::to_string(a.key) + "," + std::to_string(a.val) + ",'" + a.tag + "')");
    for (const auto& b : bs) vb.push_back("(" + std::to_string(b.key) + "," + std::to_string(b.flag) + ")");
    insertAll("ra", va);
    insertAll("rb", vb);

    std::map<std::string, std::pair<uint64_t, int64_t>> want;  // tag -> (count, sum of val)
    for (const auto& a : as) {
        if (a.key < 100) continue;
        for (const auto& b : bs)
            if (b.key == static_cast<int64_t>(a.key) && b.flag >= 3) {
                auto& w = want[a.tag];
                ++w.first;
                w.second += a.val;
            }
    }
    Rows wantRows;
    for (const auto& [tag, cw] : want) wantRows.push_back({tag, std::to_string(cw.first), std::to_string(cw.second)});

    const std::string sql =
        "SELECT a.c2, COUNT(*), SUM(a.c1) FROM ra a JOIN rb b ON a.c0 = b.c0 WHERE a.c0 >= 100 AND b.c1 >= 3 GROUP BY a.c2";
    for (int mode = 0; mode < 2; ++mode) {
        for (const char* t : {"ra", "rb"}) {
            Table& table = e.openTable(t);
            table.setUseGPU(mode == 0);
            table.setGPUThreshold(0);
        }
        expectRows(e, sql, wantRows);
    }
}

// Joins over the same two tables in opposite FROM order, plus writers, from several
// threads: table locks are taken in a fixed order, so this must not deadlock.
void testConcurrency(Engine& e) {
    std::atomic<bool> failed{false};
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            try {
                for (int i = 0; i < 150; ++i) {
                    if (t % 2 == 0) executeMiniSQL(e, "SELECT COUNT(*) FROM users u JOIN items i ON u.c0 = i.c0");
                    else executeMiniSQL(e, "SELECT COUNT(*) FROM items i JOIN users u ON i.c0 = u.c0");
                    if (i % 25 == 0) executeMiniSQL(e, t % 2 ? "INSERT INTO items VALUES (99, 'x')" : "INSERT INTO users VALUES (99, 'x', 1)");
                }
            } catch (const std::exception& ex) {
                std::fprintf(stderr, "thread %d: %s\n", t, ex.what());
                failed = true;
            }
        });
    }
    for (auto& th : threads) th.join();
    check(!failed, "concurrent joins");
}

}  // namespace

int main() {
    Engine e;
    setup(e);
    testBasics(e);
    testShapes(e);
    testMultiWay(e);
    testPushdown(e);
    testColumnCompare(e);
    testErrors(e);
    testTransactionsAndCopy(e);
    testRandomized(e);
    testConcurrency(e);
    if (failures) {
        std::fprintf(stderr, "test_sql_join: %d failure(s)\n", failures);
        return 1;
    }
    std::system((std::string("rm -rf ") + kDir).c_str());
    std::printf("test_sql_join: passed\n");
    return 0;
}
