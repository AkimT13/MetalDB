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

        assertThrows("SELECT c0 FROM '/tmp/sql_main' WHERE c0 = 1 AND c1 = 20 OR c2 = 'bob'", e);
        assertThrows("SELECT c9 FROM '/tmp/sql_main'", e);
        assertThrows("SELECT c0 FROM '/tmp/sql_main' GROUP BY c0", e);
        assertThrows("SELECT c0, count(*) FROM '/tmp/sql_main' WHERE c0 = 1 GROUP BY c0", e);
        assertThrows("SELECT c2, count(*) FROM '/tmp/sql_main' GROUP BY c2", e);
        assertThrows("SELECT c1, count(*) FROM '/tmp/sql_main' GROUP BY c0", e);
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

    std::remove("/tmp/sql_main.mdb");
    std::remove("/tmp/sql_main.mdb.idx");
    std::remove("/tmp/sql_main.mdb.2.str");
    std::remove("/tmp/sql_typed.mdb");
    std::remove("/tmp/sql_typed.mdb.idx");
    std::remove("/tmp/sql_cli.mdb");
    std::remove("/tmp/sql_cli.mdb.idx");

    std::puts("test_mini_sql: passed");
    return 0;
}
