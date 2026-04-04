#include "../Engine.hpp"
#include <cassert>
#include <unistd.h>
#include <cstdio>

int main() {
    Engine e;
    auto& t = e.createTable("eng_tbl", 3, 4096);
    t.setGPUThreshold(1024);

    // col0 = key (0..9), col1 = value (0..999), col2 = value+1
    for (uint32_t i = 0; i < 1000; ++i) {
        e.insert("eng_tbl", {i % 10, i, i + 1});
    }

    // Existing scan / sum assertions
    auto eq = e.whereEq("eng_tbl", 0, 3);
    auto rng = e.whereBetween("eng_tbl", 1, 100, 199);
    auto s   = e.sum("eng_tbl", 2);
    assert(!eq.empty());
    assert(!rng.empty());
    assert(s > 0);

    // minColumn / maxColumn
    ValueType mn = e.minColumn("eng_tbl", 1);
    ValueType mx = e.maxColumn("eng_tbl", 1);
    assert(mn == 0);
    assert(mx == 999);

    // groupCount: 10 keys × 100 rows each
    auto cnt = e.groupCount("eng_tbl", 0);
    assert(cnt.size() == 10);
    uint64_t total = 0;
    for (auto& [k, v] : cnt) total += v;
    assert(total == 1000);

    // groupSum: sum of col1 per key
    auto gsum = e.groupSum("eng_tbl", 0, 1);
    assert(gsum.size() == 10);

    // groupAvg
    auto gavg = e.groupAvg("eng_tbl", 0, 1);
    assert(gavg.size() == 10);

    // groupMin / groupMax on col1: key 0 has values {0,10,20,...,990}
    auto gmin = e.groupMin("eng_tbl", 0, 1);
    auto gmax = e.groupMax("eng_tbl", 0, 1);
    assert(gmin[0] == 0);
    assert(gmax[0] == 990);

    // join: two tables on col0
    e.createTable("eng_A", 2, 4096);
    e.createTable("eng_B", 2, 4096);
    for (uint32_t i = 0; i < 20; ++i) e.insert("eng_A", {i % 5, i});
    for (uint32_t i = 0; i < 10; ++i) e.insert("eng_B", {i % 5, i * 2});
    auto pairs = e.join("eng_A", 0, "eng_B", 0);
    assert(!pairs.empty());

    std::remove("eng_tbl.mdb");  std::remove("eng_tbl.mdb.idx");
    std::remove("eng_A.mdb");    std::remove("eng_A.mdb.idx");
    std::remove("eng_B.mdb");    std::remove("eng_B.mdb.idx");
    std::puts("test_engine: passed");
    return 0;
}
