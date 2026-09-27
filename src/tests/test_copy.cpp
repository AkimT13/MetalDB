// COPY import / export: RFC 4180 quoting, all column types, headers, lossless
// float round-trip, atomic rejection of bad files, COPY (SELECT ...) TO.
#include "../Engine.hpp"
#include "../MiniSQL.hpp"

#include <cassert>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using Rows = std::vector<std::vector<std::string>>;

void removeTable(const std::string& base, int strCol) {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal"}) std::remove((base + ext).c_str());
    std::remove((base + ".mdb." + std::to_string(strCol) + ".str").c_str());
}

std::string slurp(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

void writeFile(const std::string& path, const std::string& body) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << body;
}

bool throwsInvalid(Engine& e, const std::string& sql) {
    try {
        (void)executeMiniSQL(e, sql);
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

}  // namespace

int main() {
    const std::string src = "/tmp/copy_src", dst = "/tmp/copy_dst";
    const std::string csvPath = "/tmp/copy_test.csv";
    removeTable(src, 3);
    removeTable(dst, 3);

    Engine e;
    executeMiniSQL(e, "CREATE TABLE '" + src + "' (UINT32, INT64, DOUBLE, STRING, FLOAT)");
    executeMiniSQL(e, "INSERT INTO '" + src + "' VALUES "
                      "(1, -9000000000000000000, 0.1, 'plain', 1.5), "
                      "(2, 42, 0.3333333333333333, 'comma, inside', 0.1), "
                      "(3, 0, -2.5e-10, 'quote \"here\"', -3), "
                      "(4, 7, 1e300, '', 0), "
                      "(5, 8, 3.0, '  padded  ', 2.25)");
    // A value with an embedded newline (inserted through the API: the SQL lexer keeps it too).
    e.openTable(src).applyAtomic({}, {{ColValue(uint32_t(6)), ColValue(int64_t(9)), ColValue(0.5),
                                        ColValue(std::string("line1\nline2\r\nline3")), ColValue(1.0f)}});

    // Export with header, re-import into a fresh table, compare exactly.
    auto r = executeMiniSQL(e, "COPY '" + src + "' TO '" + csvPath + "' WITH HEADER");
    assert(r.rows[0][0] == "6");
    const std::string exported = slurp(csvPath);
    assert(exported.rfind("c0,c1,c2,c3,c4\n", 0) == 0);
    assert(exported.find("\"comma, inside\"") != std::string::npos);
    assert(exported.find("\"quote \"\"here\"\"\"") != std::string::npos);
    assert(exported.find("\"  padded  \"") != std::string::npos);

    executeMiniSQL(e, "CREATE TABLE '" + dst + "' (UINT32, INT64, DOUBLE, STRING, FLOAT)");
    r = executeMiniSQL(e, "COPY '" + dst + "' FROM '" + csvPath + "' WITH HEADER");
    assert(r.rows[0][0] == "6");
    {
        Table& a = e.openTable(src);
        Table& b = e.openTable(dst);
        for (uint32_t id = 0; id < 6; ++id) {
            auto x = a.fetchTypedRow(id);
            auto y = b.fetchTypedRow(id);
            for (size_t c = 0; c < x.size(); ++c) {
                assert(x[c] && y[c]);
                assert(x[c]->type == y[c]->type);
                if (x[c]->type == ColType::STRING) assert(x[c]->str == y[c]->str);
                else if (x[c]->type == ColType::DOUBLE) assert(x[c]->f64 == y[c]->f64);  // bit-exact round trip
                else if (x[c]->type == ColType::FLOAT) assert(x[c]->f32 == y[c]->f32);
                else if (x[c]->type == ColType::INT64) assert(x[c]->i64 == y[c]->i64);
                else assert(x[c]->u32 == y[c]->u32);
            }
        }
    }

    // COPY (SELECT ...) TO with ORDER BY / WHERE, no header.
    r = executeMiniSQL(e, "COPY (SELECT c3, c0 FROM '" + src + "' WHERE c0 >= 4 ORDER BY c0 DESC) TO '" + csvPath + "'");
    assert(r.rows[0][0] == "3");
    assert(slurp(csvPath) == "\"line1\nline2\r\nline3\",6\n\"  padded  \",5\n,4\n");

    // CRLF input, blank lines skipped, quoted empty string kept.
    removeTable(dst, 3);
    executeMiniSQL(e, "CREATE TABLE '" + dst + "' (UINT32, INT64, DOUBLE, STRING, FLOAT)");
    writeFile(csvPath, "10,1,1.25,\"\",0.5\r\n\r\n11,-2,2,x,1\r\n");
    r = executeMiniSQL(e, "COPY '" + dst + "' FROM '" + csvPath + "'");
    assert(r.rows[0][0] == "2");
    r = executeMiniSQL(e, "SELECT c0, c3 FROM '" + dst + "' ORDER BY c0");
    assert((r.rows == Rows{{"10", ""}, {"11", "x"}}));

    // Bad files are rejected atomically: nothing from them is inserted.
    const std::vector<std::string> badFiles = {
        "12,1,1,a,1\n13,1,oops,b,1\n",   // non-numeric DOUBLE
        "12,1,1,a,1\n13,1,1,b\n",        // wrong field count
        "12,1,1,a,1\n-1,1,1,b,1\n",      // negative UINT32
        "12,1,1,\"unterminated,1\n",     // unterminated quote
        "12,1,1,a\"b,1\n",               // quote inside unquoted field
    };
    for (const auto& body : badFiles) {
        writeFile(csvPath, body);
        assert(throwsInvalid(e, "COPY '" + dst + "' FROM '" + csvPath + "'"));
        assert(executeMiniSQL(e, "SELECT count(*) FROM '" + dst + "'").rows[0][0] == "2");
    }
    assert(throwsInvalid(e, "COPY '" + dst + "' FROM '/tmp/definitely_missing_file.csv'"));
    assert(throwsInvalid(e, "COPY (SELECT c0 FROM '" + dst + "') FROM '" + csvPath + "'"));
    assert(throwsInvalid(e, "COPY '/tmp/copy_no_such_table' TO '" + csvPath + "'"));

    // Imported data survives reopen (COPY FROM checkpoints).
    {
        Engine e2;
        auto rows = executeMiniSQL(e2, "SELECT count(*) FROM '" + dst + "'").rows;
        assert(rows[0][0] == "2");
    }

    std::remove(csvPath.c_str());
    removeTable(src, 3);
    removeTable(dst, 3);
    std::puts("test_copy: passed");
    return 0;
}
