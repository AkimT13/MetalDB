#include "../Engine.hpp"
#include "../MiniSQL.hpp"

#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {

std::string captureCommand(const std::string& command) {
    std::array<char, 256> buffer{};
    std::string output;
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) throw std::runtime_error("popen failed");
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe))
        output += buffer.data();
    const int rc = pclose(pipe);
    if (rc != 0) throw std::runtime_error("command failed: " + command);
    return output;
}

void assertThrows(const std::string& sql, Engine& engine) {
    bool threw = false;
    try {
        (void)executeMiniSQL(engine, sql);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
}

} // namespace

int main() {
    {
        Engine e;
        e.createTypedTable("/tmp/sql_main", {ColType::UINT32, ColType::UINT32, ColType::STRING});
        e.insertTyped("/tmp/sql_main", {ColValue(uint32_t(1)), ColValue(uint32_t(10)), ColValue(std::string("alice"))});
        e.insertTyped("/tmp/sql_main", {ColValue(uint32_t(2)), ColValue(uint32_t(20)), ColValue(std::string("bob"))});
        e.insertTyped("/tmp/sql_main", {ColValue(uint32_t(2)), ColValue(uint32_t(30)), ColValue(std::string("alice"))});
        e.insertTyped("/tmp/sql_main", {ColValue(uint32_t(3)), ColValue(uint32_t(40)), ColValue(std::string("carol"))});

        {
            auto r = executeMiniSQL(e, "SELECT * FROM '/tmp/sql_main'");
            assert((r.headers == std::vector<std::string>{"c0", "c1", "c2"}));
            assert(r.rows.size() == 4);
            assert((r.rows[0] == std::vector<std::string>{"1", "10", "alice"}));
        }

        {
            auto r = executeMiniSQL(e, "SELECT c1, c2 FROM '/tmp/sql_main' WHERE c0 = 2");
            assert((r.headers == std::vector<std::string>{"c1", "c2"}));
            assert(r.rows.size() == 2);
            assert((r.rows[0] == std::vector<std::string>{"20", "bob"}));
            assert((r.rows[1] == std::vector<std::string>{"30", "alice"}));
        }

        {
            auto r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_main' WHERE c2 = 'alice'");
            assert(r.rows.size() == 2);
            assert(r.rows[0][0] == "1");
            assert(r.rows[1][0] == "2");
        }

        {
            auto r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_main' WHERE c0 = 1 OR c1 BETWEEN 30 AND 40");
            assert(r.rows.size() == 3);
            assert(r.rows[0][0] == "1");
            assert(r.rows[1][0] == "2");
            assert(r.rows[2][0] == "3");
        }

        {
            auto r = executeMiniSQL(e, "SELECT count(*) FROM '/tmp/sql_main' WHERE c2 = 'alice'");
            assert((r.headers == std::vector<std::string>{"count(*)"}));
            assert(r.rows.size() == 1);
            assert(r.rows[0][0] == "2");
        }

        {
            auto r = executeMiniSQL(e, "SELECT sum(c1) FROM '/tmp/sql_main' WHERE c0 = 2");
            assert(r.rows.size() == 1);
            assert(r.rows[0][0] == "50");
        }

        {
            auto r = executeMiniSQL(e, "SELECT avg(c1) FROM '/tmp/sql_main'");
            assert(r.rows.size() == 1);
            assert(r.rows[0][0] == "25");
        }

        {
            auto r = executeMiniSQL(e, "SELECT c0, count(*) FROM '/tmp/sql_main' GROUP BY c0");
            assert((r.headers == std::vector<std::string>{"c0", "count(*)"}));
            assert(r.rows.size() == 3);
            assert((r.rows[0] == std::vector<std::string>{"1", "1"}));
            assert((r.rows[1] == std::vector<std::string>{"2", "2"}));
            assert((r.rows[2] == std::vector<std::string>{"3", "1"}));
        }

        {
            auto r = executeMiniSQL(e, "SELECT c0, sum(c1) FROM '/tmp/sql_main' GROUP BY c0");
            assert((r.headers == std::vector<std::string>{"c0", "sum(c1)"}));
            assert(r.rows.size() == 3);
            assert((r.rows[0] == std::vector<std::string>{"1", "10"}));
            assert((r.rows[1] == std::vector<std::string>{"2", "50"}));
            assert((r.rows[2] == std::vector<std::string>{"3", "40"}));
        }

        {
            // AND binds tighter than OR: (c0 = 1 AND c1 = 20) OR c2 = 'bob'
            auto r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_main' WHERE c0 = 1 AND c1 = 20 OR c2 = 'bob'");
            assert(r.rows.size() == 1 && r.rows[0][0] == "2");
        }
        assertThrows("SELECT c9 FROM '/tmp/sql_main'", e);
        assertThrows("SELECT c1, count(*) FROM '/tmp/sql_main' GROUP BY c0", e);  // c1 not grouped
        assertThrows("SELECT c0, count(*) FROM '/tmp/sql_main'", e);              // mixed without GROUP BY
        assertThrows("SELECT DISTINCT count(*) FROM '/tmp/sql_main'", e);

        // GROUP BY v2: key-only, WHERE, STRING keys, multiple keys and aggregates.
        {
            auto r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_main' GROUP BY c0");
            assert((r.rows == std::vector<std::vector<std::string>>{{"1"}, {"2"}, {"3"}}));
            r = executeMiniSQL(e, "SELECT c0, count(*) FROM '/tmp/sql_main' WHERE c1 >= 20 GROUP BY c0");
            assert((r.rows == std::vector<std::vector<std::string>>{{"2", "2"}, {"3", "1"}}));
            r = executeMiniSQL(e, "SELECT c2, count(*), sum(c1), min(c0), max(c1), avg(c1) FROM '/tmp/sql_main' GROUP BY c2");
            assert((r.headers == std::vector<std::string>{"c2", "count(*)", "sum(c1)", "min(c0)", "max(c1)", "avg(c1)"}));
            assert((r.rows == std::vector<std::vector<std::string>>{
                {"alice", "2", "40", "1", "30", "20"}, {"bob", "1", "20", "2", "20", "20"}, {"carol", "1", "40", "3", "40", "40"}}));
            r = executeMiniSQL(e, "SELECT c0, c2, count(*) FROM '/tmp/sql_main' GROUP BY c0, c2 ORDER BY c0 DESC, c2");
            assert((r.rows == std::vector<std::vector<std::string>>{
                {"3", "carol", "1"}, {"2", "alice", "1"}, {"2", "bob", "1"}, {"1", "alice", "1"}}));
            r = executeMiniSQL(e, "SELECT count(*), c2 FROM '/tmp/sql_main' GROUP BY c2 ORDER BY 1 DESC, c2 LIMIT 1");
            assert((r.rows == std::vector<std::vector<std::string>>{{"2", "alice"}}));
            r = executeMiniSQL(e, "SELECT DISTINCT c2 FROM '/tmp/sql_main' ORDER BY c2 DESC");
            assert((r.rows == std::vector<std::vector<std::string>>{{"carol"}, {"bob"}, {"alice"}}));
            r = executeMiniSQL(e, "SELECT DISTINCT c0 FROM '/tmp/sql_main' WHERE c2 != 'carol'");
            assert((r.rows == std::vector<std::vector<std::string>>{{"1"}, {"2"}}));
            r = executeMiniSQL(e, "SELECT c0, sum(c1), avg(c1) FROM '/tmp/sql_main' GROUP BY c0");  // fast path
            assert((r.rows == std::vector<std::vector<std::string>>{{"1", "10", "10"}, {"2", "50", "25"}, {"3", "40", "40"}}));
            r = executeMiniSQL(e, "SELECT min(c2), max(c2), count(c2) FROM '/tmp/sql_main'");
            assert((r.rows[0] == std::vector<std::string>{"alice", "carol", "4"}));
        }
    }

    {
        Engine e;
        e.createTypedTable("/tmp/sql_typed", {ColType::UINT32, ColType::FLOAT, ColType::INT64});
        e.insertTyped("/tmp/sql_typed", {ColValue(uint32_t(7)), ColValue(1.5f), ColValue(int64_t(2000))});
        auto r = executeMiniSQL(e, "SELECT c0, c1, c2 FROM '/tmp/sql_typed'");
        assert(r.rows.size() == 1);
        assert(r.rows[0][0] == "7");
        assert(r.rows[0][1] == "1.5");
        assert(r.rows[0][2] == "2000");
    }

    {
        Engine e;
        e.createTypedTable("/tmp/sql_cli", {ColType::UINT32, ColType::UINT32});
        e.insertTyped("/tmp/sql_cli", {ColValue(uint32_t(1)), ColValue(uint32_t(10))});
        e.insertTyped("/tmp/sql_cli", {ColValue(uint32_t(2)), ColValue(uint32_t(20))});
        const std::string out = captureCommand("./mdb query \"SELECT c0, c1 FROM '/tmp/sql_cli' WHERE c0 = 2\"");
        assert(out == "c0\tc1\n2\t20\n");

        const std::string replOut = captureCommand(
            "printf \".help\nSELECT c0,\n c1 FROM '/tmp/sql_cli' WHERE c0 = 2;\n.quit\n\" | ./mdb repl 2>&1"
        );
        assert(replOut.find("Mini-SQL REPL\n") != std::string::npos);
        assert(replOut.find("Queries must end with ';'") != std::string::npos);
        assert(replOut.find("c0\tc1\n2\t20\n") != std::string::npos);
    }

    {
        Engine e;
        e.createTypedTable("/tmp/sql_flush", {ColType::UINT32});
        e.insertTyped("/tmp/sql_flush", {ColValue(uint32_t(77))});
        const std::string out = captureCommand("./mdb flush /tmp/sql_flush");
        assert(out == "flushed /tmp/sql_flush\n");
    }
    {
        Engine e;
        auto& t = e.openTable("/tmp/sql_flush");
        auto row = t.fetchTypedRow(0);
        assert(row.size() == 1);
        assert(row[0] && row[0]->u32 == 77);
    }

    // ── DDL / DML ────────────────────────────────────────────────────────────
    std::remove("/tmp/sql_dml.mdb");
    std::remove("/tmp/sql_dml.mdb.idx");
    std::remove("/tmp/sql_dml.mdb.wal");
    std::remove("/tmp/sql_dml.mdb.3.str");
    {
        Engine e;
        {
            auto r = executeMiniSQL(e, "CREATE TABLE '/tmp/sql_dml' (c0 UINT32, c1 INT64, c2 DOUBLE, c3 STRING)");
            assert((r.headers == std::vector<std::string>{"created"}));
            assert(r.rows[0][0] == "/tmp/sql_dml");
        }
        assertThrows("CREATE TABLE '/tmp/sql_dml' (UINT32)", e);          // already exists
        assertThrows("CREATE TABLE '/tmp/sql_bad' (c1 UINT32)", e);       // out-of-order name
        assertThrows("CREATE TABLE '/tmp/sql_bad' (VARCHAR)", e);         // unknown type

        {
            auto r = executeMiniSQL(e, "DESCRIBE '/tmp/sql_dml'");
            assert((r.headers == std::vector<std::string>{"column", "type"}));
            assert(r.rows.size() == 4);
            assert((r.rows[1] == std::vector<std::string>{"c1", "INT64"}));
            assert((r.rows[3] == std::vector<std::string>{"c3", "STRING"}));
        }

        {
            auto r = executeMiniSQL(e,
                "INSERT INTO '/tmp/sql_dml' VALUES "
                "(1, -5, 2.5, 'alice'), (2, 7, -0.25, 'bob'), (3, 9000000000, 1e3, 'o''brien'), "
                "(4, 0, 0, 'dave'), (5, 3, 4.75, 'erin')");
            assert((r.headers == std::vector<std::string>{"rows_affected"}));
            assert(r.rows[0][0] == "5");
        }
        // Type/arity errors are rejected before any row is written.
        assertThrows("INSERT INTO '/tmp/sql_dml' VALUES (6, 1, 1.0, 'x'), (-1, 1, 1.0, 'y')", e);
        assertThrows("INSERT INTO '/tmp/sql_dml' VALUES (6, 1.5, 1.0, 'x')", e);
        assertThrows("INSERT INTO '/tmp/sql_dml' VALUES (6, 1, 'nope', 'x')", e);
        assertThrows("INSERT INTO '/tmp/sql_dml' VALUES (6, 1, 1.0)", e);
        assertThrows("INSERT INTO '/tmp/sql_dml' VALUES (4294967296, 1, 1.0, 'x')", e);
        assertThrows("INSERT INTO '/tmp/sql_missing' VALUES (1)", e);
        assertThrows("INSERT INTO '/tmp/sql_dml' VALUES (6, 1, 1e999, 'x')", e);
        {
            auto r = executeMiniSQL(e, "SELECT count(*) FROM '/tmp/sql_dml'");
            assert(r.rows[0][0] == "5");
        }
        {
            auto r = executeMiniSQL(e, "SELECT c1, c2, c3 FROM '/tmp/sql_dml' WHERE c0 = 3");
            assert((r.rows[0] == std::vector<std::string>{"9000000000", "1000", "o'brien"}));
        }

        // Comparison operators
        {
            auto r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 > 3");
            assert(r.rows.size() == 2 && r.rows[0][0] == "4" && r.rows[1][0] == "5");
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 >= 2 AND c0 < 4");
            assert(r.rows.size() == 2 && r.rows[0][0] == "2" && r.rows[1][0] == "3");
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 <= 1 OR c0 > 4");
            assert(r.rows.size() == 2 && r.rows[0][0] == "1" && r.rows[1][0] == "5");
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 < 0");
            assert(r.rows.empty());
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 < 0 OR c0 = 2");
            assert(r.rows.size() == 1 && r.rows[0][0] == "2");
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 > 4294967295");
            assert(r.rows.empty());
        }
        assertThrows("SELECT c0 FROM '/tmp/sql_dml' WHERE c3 < 5", e);   // non-UINT32 column
        assertThrows("SELECT c0 FROM '/tmp/sql_dml' WHERE c9 < 0", e);   // bad column even if empty range
        // A non-integer literal can never equal a UINT32 value.
        assert(executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 = 1.5").rows.empty());
        assert(executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' WHERE c0 <> 1").rows.size() == 4);

        // ORDER BY / LIMIT / OFFSET
        {
            auto r = executeMiniSQL(e, "SELECT c0, c2 FROM '/tmp/sql_dml' ORDER BY c2 DESC");
            assert(r.rows.size() == 5);
            assert(r.rows[0][0] == "3" && r.rows[1][0] == "5" && r.rows[4][0] == "2");
            r = executeMiniSQL(e, "SELECT c3 FROM '/tmp/sql_dml' ORDER BY c3 LIMIT 2");
            assert((r.rows == std::vector<std::vector<std::string>>{{"alice"}, {"bob"}}));
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' ORDER BY 1 DESC LIMIT 2 OFFSET 1");
            assert((r.rows == std::vector<std::vector<std::string>>{{"4"}, {"3"}}));
            r = executeMiniSQL(e, "SELECT * FROM '/tmp/sql_dml' LIMIT 0");
            assert(r.rows.empty() && r.headers.size() == 4);
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml' LIMIT 10 OFFSET 99");
            assert(r.rows.empty());
        }
        assertThrows("SELECT c0 FROM '/tmp/sql_dml' ORDER BY c1", e);   // not projected
        assertThrows("SELECT c0 FROM '/tmp/sql_dml' ORDER BY 2", e);    // position out of range
        assertThrows("SELECT c0 FROM '/tmp/sql_dml' LIMIT -1", e);

        // DELETE
        {
            auto r = executeMiniSQL(e, "DELETE FROM '/tmp/sql_dml' WHERE c3 = 'bob' OR c0 >= 5");
            assert((r.headers == std::vector<std::string>{"rows_affected"}));
            assert(r.rows[0][0] == "2");
            r = executeMiniSQL(e, "SELECT c0 FROM '/tmp/sql_dml'");
            assert((r.rows == std::vector<std::vector<std::string>>{{"1"}, {"3"}, {"4"}}));
            r = executeMiniSQL(e, "DELETE FROM '/tmp/sql_dml' WHERE c0 < 0");
            assert(r.rows[0][0] == "0");
        }
    }

    // UPDATE (copy-on-write, one WAL transaction)
    std::remove("/tmp/sql_upd.mdb");
    std::remove("/tmp/sql_upd.mdb.idx");
    std::remove("/tmp/sql_upd.mdb.wal");
    std::remove("/tmp/sql_upd.mdb.2.str");
    {
        Engine e;
        executeMiniSQL(e, "CREATE TABLE '/tmp/sql_upd' (UINT32, DOUBLE, STRING)");
        executeMiniSQL(e, "INSERT INTO '/tmp/sql_upd' VALUES (1, 1.5, 'a'), (2, 2.5, 'b'), (3, 3.5, 'c')");
        auto r = executeMiniSQL(e, "UPDATE '/tmp/sql_upd' SET c1 = 9.25, c2 = 'z' WHERE c0 >= 2");
        assert((r.headers == std::vector<std::string>{"rows_affected"}) && r.rows[0][0] == "2");
        r = executeMiniSQL(e, "SELECT * FROM '/tmp/sql_upd' ORDER BY c0");
        assert((r.rows == std::vector<std::vector<std::string>>{{"1", "1.5", "a"}, {"2", "9.25", "z"}, {"3", "9.25", "z"}}));
        r = executeMiniSQL(e, "UPDATE '/tmp/sql_upd' SET c0 = 100");
        assert(r.rows[0][0] == "3");
        r = executeMiniSQL(e, "SELECT count(*), min(c0), max(c0) FROM '/tmp/sql_upd'");
        assert((r.rows[0] == std::vector<std::string>{"3", "100", "100"}));
        r = executeMiniSQL(e, "UPDATE '/tmp/sql_upd' SET c2 = 'none' WHERE c0 = 7");
        assert(r.rows[0][0] == "0");
        assertThrows("UPDATE '/tmp/sql_upd' SET c1 = 'text'", e);       // type mismatch
        assertThrows("UPDATE '/tmp/sql_upd' SET c0 = -1", e);           // UINT32 range
        assertThrows("UPDATE '/tmp/sql_upd' SET c0 = 1, c0 = 2", e);    // duplicate column
        assertThrows("UPDATE '/tmp/sql_upd' SET c7 = 1", e);            // bad column
        assertThrows("UPDATE '/tmp/sql_upd' c0 = 1", e);                // missing SET
        r = executeMiniSQL(e, "SELECT c2 FROM '/tmp/sql_upd' WHERE c2 = 'z'");
        assert(r.rows.size() == 2);  // failed UPDATEs changed nothing
    }
    {
        Engine e;  // survives reopen (WAL / base files consistent)
        auto r = executeMiniSQL(e, "SELECT c0, c2 FROM '/tmp/sql_upd' ORDER BY c2");
        assert((r.rows == std::vector<std::vector<std::string>>{{"100", "a"}, {"100", "z"}, {"100", "z"}}));
    }
    std::remove("/tmp/sql_upd.mdb");
    std::remove("/tmp/sql_upd.mdb.idx");
    std::remove("/tmp/sql_upd.mdb.wal");
    std::remove("/tmp/sql_upd.mdb.2.str");

    // EXPLAIN
    {
        Engine e;
        auto plan = executeMiniSQL(e, "EXPLAIN SELECT c2, count(*) FROM '/tmp/sql_main' WHERE c0 >= 2 AND c2 != 'bob' GROUP BY c2");
        assert((plan.headers == std::vector<std::string>{"plan"}));
        std::string text;
        for (const auto& row : plan.rows) text += row[0] + "\n";
        assert(text.find("Statement: SELECT") != std::string::npos);
        assert(text.find("range scan [2, 4294967295]") != std::string::npos);
        assert(text.find("string equality scan") != std::string::npos);
        assert(text.find("CPU hash aggregation on (c2), 2 groups") != std::string::npos);
        assert(text.find("Output: 2 rows") != std::string::npos);

        plan = executeMiniSQL(e, "EXPLAIN DELETE FROM '/tmp/sql_main' WHERE c0 = 2");
        text.clear();
        for (const auto& row : plan.rows) text += row[0] + "\n";
        assert(text.find("would delete 2 rows") != std::string::npos);
        auto r = executeMiniSQL(e, "SELECT count(*) FROM '/tmp/sql_main'");
        assert(r.rows[0][0] == "4");  // EXPLAIN DELETE wrote nothing

        const std::string replOut = captureCommand(
            "printf \".timer on\nSELECT count(*) FROM '/tmp/sql_main';\n.quit\n\" | ./mdb repl 2>&1");
        assert(replOut.find("count(*)\n4\n") != std::string::npos);
        assert(replOut.find("Time: ") != std::string::npos);
    }

    // ORDER BY over GROUP BY output
    {
        Engine e;
        auto r = executeMiniSQL(e, "SELECT c0, count(*) FROM '/tmp/sql_main' GROUP BY c0 ORDER BY count(*) DESC, c0 LIMIT 2");
        assert((r.rows == std::vector<std::vector<std::string>>{{"2", "2"}, {"1", "1"}}));
    }

    // DML through the CLI across separate processes (exercises WAL replay on reopen)
    {
        std::remove("/tmp/sql_cli_dml.mdb");
        std::remove("/tmp/sql_cli_dml.mdb.idx");
        std::remove("/tmp/sql_cli_dml.mdb.wal");
        std::remove("/tmp/sql_cli_dml.mdb.1.str");
        assert(captureCommand("./mdb query \"CREATE TABLE '/tmp/sql_cli_dml' (UINT32, STRING)\"") ==
               "created\n/tmp/sql_cli_dml\n");
        assert(captureCommand("./mdb query \"INSERT INTO '/tmp/sql_cli_dml' VALUES (2, 'b'), (1, 'a'), (3, 'c')\"") ==
               "rows_affected\n3\n");
        assert(captureCommand("./mdb query \"DELETE FROM '/tmp/sql_cli_dml' WHERE c0 = 3\"") ==
               "rows_affected\n1\n");
        assert(captureCommand("./mdb query \"SELECT * FROM '/tmp/sql_cli_dml' ORDER BY c0\"") ==
               "c0\tc1\n1\ta\n2\tb\n");
    }

    std::remove("/tmp/sql_dml.mdb");
    std::remove("/tmp/sql_dml.mdb.idx");
    std::remove("/tmp/sql_dml.mdb.wal");
    std::remove("/tmp/sql_dml.mdb.3.str");
    std::remove("/tmp/sql_cli_dml.mdb");
    std::remove("/tmp/sql_cli_dml.mdb.idx");
    std::remove("/tmp/sql_cli_dml.mdb.wal");
    std::remove("/tmp/sql_cli_dml.mdb.1.str");
    std::remove("/tmp/sql_main.mdb");
    std::remove("/tmp/sql_main.mdb.idx");
    std::remove("/tmp/sql_main.mdb.wal");
    std::remove("/tmp/sql_main.mdb.2.str");
    std::remove("/tmp/sql_typed.mdb");
    std::remove("/tmp/sql_typed.mdb.idx");
    std::remove("/tmp/sql_typed.mdb.wal");
    std::remove("/tmp/sql_cli.mdb");
    std::remove("/tmp/sql_cli.mdb.idx");
    std::remove("/tmp/sql_cli.mdb.wal");
    std::remove("/tmp/sql_flush.mdb");
    std::remove("/tmp/sql_flush.mdb.idx");
    std::remove("/tmp/sql_flush.mdb.wal");

    std::puts("test_mini_sql: passed");
    return 0;
}
