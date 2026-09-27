// WHERE expression engine vs. brute-force reference.
//
// Builds a table with every column type, runs a battery of WHERE clauses through
// mini-SQL, and compares the returned row set with a C++ predicate evaluated over
// the generated data. Run twice: once CPU-only, once with the GPU threshold at 0
// so on Apple Silicon the UINT32 / string leaves take the Metal kernels.
#include "../Engine.hpp"
#include "../MiniSQL.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

extern "C" bool metalIsAvailable();

namespace {

const char* kBase = "/tmp/sql_where_tbl";

struct Row {
    uint32_t id;     // c0 UINT32 (unique, == insertion order)
    uint32_t bucket; // c1 UINT32 (0..9)
    int64_t big;     // c2 INT64 (signed, includes values beyond 2^53)
    double ratio;    // c3 DOUBLE
    std::string tag; // c4 STRING
    float f;         // c5 FLOAT
};

Row makeRow(uint32_t i) {
    static const char* tags[] = {"red", "green", "blue", "", "o'neil", "red-ish"};
    Row r;
    r.id = i;
    r.bucket = i % 10;
    r.big = (i % 3 == 0) ? -static_cast<int64_t>(i) * 1000 : (int64_t(1) << 60) + i;
    r.ratio = (static_cast<double>(i % 17) - 8.0) / 4.0;  // -2.0 .. 2.0 step .25
    r.tag = tags[i % 6];
    r.f = static_cast<float>(i % 5) * 0.5f;
    return r;
}

void cleanup() {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.4.str"})
        std::remove((std::string(kBase) + ext).c_str());
}

struct Case {
    std::string where;
    std::function<bool(const Row&)> expect;
};

void runCases(Engine& e, const std::vector<Row>& rows, const std::vector<bool>& live,
              const std::vector<Case>& cases) {
    for (const auto& c : cases) {
        const std::string sql = std::string("SELECT c0 FROM '") + kBase + "' WHERE " + c.where;
        const auto result = executeMiniSQL(e, sql);
        std::vector<std::string> expected;
        for (size_t i = 0; i < rows.size(); ++i)
            if (live[i] && c.expect(rows[i])) expected.push_back(std::to_string(rows[i].id));

        std::vector<std::string> got;
        for (const auto& r : result.rows) got.push_back(r[0]);
        if (got != expected) {
            std::fprintf(stderr, "MISMATCH: %s\n  expected %zu rows, got %zu\n", c.where.c_str(),
                         expected.size(), got.size());
            std::exit(1);
        }
    }
}

void expectInvalid(Engine& e, const std::string& where) {
    bool threw = false;
    try {
        (void)executeMiniSQL(e, std::string("SELECT c0 FROM '") + kBase + "' WHERE " + where);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    if (!threw) {
        std::fprintf(stderr, "expected error for WHERE %s\n", where.c_str());
        std::exit(1);
    }
}

}  // namespace

int main() {
    cleanup();
    const uint32_t kRows = 5000;
    std::vector<Row> rows;
    std::vector<bool> live(kRows, true);

    Engine e;
    Table& t = e.createTypedTable(kBase, {ColType::UINT32, ColType::UINT32, ColType::INT64,
                                          ColType::DOUBLE, ColType::STRING, ColType::FLOAT});
    std::vector<std::vector<ColValue>> batch;
    for (uint32_t i = 0; i < kRows; ++i) {
        rows.push_back(makeRow(i));
        const Row& r = rows.back();
        batch.push_back({ColValue(r.id), ColValue(r.bucket), ColValue(r.big), ColValue(r.ratio),
                         ColValue(r.tag), ColValue(r.f)});
    }
    t.applyAtomic({}, batch);
    std::vector<uint32_t> toDelete;
    for (uint32_t i = 0; i < kRows; i += 11) {
        toDelete.push_back(i);
        live[i] = false;
    }
    t.applyAtomic(toDelete, {});

    const int64_t big60 = int64_t(1) << 60;
    const std::string big60s = std::to_string(big60);
    const std::vector<Case> cases = {
        // UINT32 comparisons (GPU-eligible leaves)
        {"c1 = 3", [](const Row& r) { return r.bucket == 3; }},
        {"c1 != 3", [](const Row& r) { return r.bucket != 3; }},
        {"c1 <> 3", [](const Row& r) { return r.bucket != 3; }},
        {"c1 < 3", [](const Row& r) { return r.bucket < 3; }},
        {"c1 <= 3", [](const Row& r) { return r.bucket <= 3; }},
        {"c1 > 7", [](const Row& r) { return r.bucket > 7; }},
        {"c1 >= 7", [](const Row& r) { return r.bucket >= 7; }},
        {"c1 < 2.5", [](const Row& r) { return r.bucket < 2.5; }},
        {"c1 > -4", [](const Row&) { return true; }},
        {"c1 < -4", [](const Row&) { return false; }},
        {"c1 = 3.5", [](const Row&) { return false; }},
        {"c1 BETWEEN 2 AND 4", [](const Row& r) { return r.bucket >= 2 && r.bucket <= 4; }},
        {"c1 NOT BETWEEN 2 AND 4", [](const Row& r) { return !(r.bucket >= 2 && r.bucket <= 4); }},
        {"c1 BETWEEN 4 AND 2", [](const Row&) { return false; }},
        {"c1 IN (1, 3, 99)", [](const Row& r) { return r.bucket == 1 || r.bucket == 3; }},
        {"c1 NOT IN (1, 3)", [](const Row& r) { return r.bucket != 1 && r.bucket != 3; }},
        {"c0 >= 4990", [](const Row& r) { return r.id >= 4990; }},

        // Boolean structure, precedence, parentheses, NOT
        {"c1 = 1 OR c1 = 2 AND c0 < 100",
         [](const Row& r) { return r.bucket == 1 || (r.bucket == 2 && r.id < 100); }},
        {"(c1 = 1 OR c1 = 2) AND c0 < 100",
         [](const Row& r) { return (r.bucket == 1 || r.bucket == 2) && r.id < 100; }},
        {"NOT c1 = 1", [](const Row& r) { return r.bucket != 1; }},
        {"NOT (c1 < 5 OR c0 > 2500)", [](const Row& r) { return !(r.bucket < 5 || r.id > 2500); }},
        {"NOT NOT c1 = 4", [](const Row& r) { return r.bucket == 4; }},
        {"c1 = 1 AND c1 = 2", [](const Row&) { return false; }},
        {"((c0 < 10))", [](const Row& r) { return r.id < 10; }},

        // INT64 (exact beyond 2^53)
        {"c2 < 0", [](const Row& r) { return r.big < 0; }},
        {"c2 = " + big60s + "7", [](const Row&) { return false; }},
        {"c2 = " + std::to_string(big60 + 7), [&](const Row& r) { return r.big == big60 + 7; }},
        {"c2 > " + std::to_string(big60 + 4990), [&](const Row& r) { return r.big > big60 + 4990; }},
        {"c2 BETWEEN -3000 AND 0", [](const Row& r) { return r.big >= -3000 && r.big <= 0; }},
        {"c2 IN (-3000, -6000, 5)", [](const Row& r) { return r.big == -3000 || r.big == -6000; }},

        // DOUBLE / FLOAT
        {"c3 > 1.5", [](const Row& r) { return r.ratio > 1.5; }},
        {"c3 = -0.25", [](const Row& r) { return r.ratio == -0.25; }},
        {"c3 BETWEEN -0.5 AND 0.5", [](const Row& r) { return r.ratio >= -0.5 && r.ratio <= 0.5; }},
        {"c5 >= 1.5", [](const Row& r) { return r.f >= 1.5f; }},
        {"c5 != 0", [](const Row& r) { return r.f != 0.0f; }},

        // STRING
        {"c4 = 'red'", [](const Row& r) { return r.tag == "red"; }},
        {"c4 != 'red'", [](const Row& r) { return r.tag != "red"; }},
        {"c4 = ''", [](const Row& r) { return r.tag.empty(); }},
        {"c4 = 'o''neil'", [](const Row& r) { return r.tag == "o'neil"; }},
        {"c4 < 'green'", [](const Row& r) { return r.tag < "green"; }},
        {"c4 >= 'red'", [](const Row& r) { return r.tag >= "red"; }},
        {"c4 BETWEEN 'blue' AND 'red'", [](const Row& r) { return r.tag >= "blue" && r.tag <= "red"; }},
        {"c4 IN ('red', 'blue')", [](const Row& r) { return r.tag == "red" || r.tag == "blue"; }},
        {"c4 NOT IN ('red', 'blue', '')",
         [](const Row& r) { return r.tag != "red" && r.tag != "blue" && !r.tag.empty(); }},

        // Mixed types in one expression
        {"c4 = 'green' AND c3 < 0 OR c2 < -4900000",
         [](const Row& r) { return (r.tag == "green" && r.ratio < 0) || r.big < -4900000; }},
    };

    // CPU-only
    t.setUseGPU(false);
    runCases(e, rows, live, cases);

    // GPU-eligible (threshold 0). On machines without Metal this re-runs the CPU path.
    t.setUseGPU(true);
    t.setGPUThreshold(0);
    runCases(e, rows, live, cases);

    // Errors
    expectInvalid(e, "c9 = 1");
    expectInvalid(e, "c4 = 5");          // STRING vs numeric literal
    expectInvalid(e, "c1 = 'x'");        // numeric vs string literal
    expectInvalid(e, "c1 = 1 AND");      // dangling connective
    expectInvalid(e, "(c1 = 1");         // unbalanced paren
    expectInvalid(e, "c1 NOT = 1");      // NOT must precede BETWEEN / IN
    expectInvalid(e, "c1 IN ()");

    // DELETE shares the evaluator.
    {
        auto r = executeMiniSQL(e, std::string("DELETE FROM '") + kBase + "' WHERE c4 = 'blue' AND NOT c1 IN (2)");
        size_t expected = 0;
        for (size_t i = 0; i < rows.size(); ++i)
            if (live[i] && rows[i].tag == "blue" && rows[i].bucket != 2) {
                live[i] = false;
                ++expected;
            }
        assert(r.rows[0][0] == std::to_string(expected));
        runCases(e, rows, live, {{"c4 = 'blue'", [](const Row& r) { return r.tag == "blue" && r.bucket == 2; }}});
    }

    cleanup();
    std::printf("test_sql_where: passed (%zu cases x 2 dispatch modes, %s)\n", cases.size(),
                metalIsAvailable() ? "GPU exercised" : "CPU-only");
    return 0;
}
