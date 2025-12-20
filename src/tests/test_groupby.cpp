#include "../Engine.hpp"
#include "../GroupBy.hpp"
#include <cassert>
#include <cstdio>

int main() {
    Engine e;
    auto& t = e.createTable("gb_tbl", 2, 4096);
    for (uint32_t i = 0; i < 1000; ++i) {
        e.insert("gb_tbl", {i % 5, 1});
    }
    auto cnt = GroupBy::countByKey(t, 0);
    auto sum = GroupBy::sumByKey(t, 0, 1);

    uint64_t totalCount = 0, totalSum = 0;
    for (auto& kv : cnt) totalCount += kv.second;
    for (auto& kv : sum) totalSum += kv.second;

    assert(totalCount == 1000);
    assert(totalSum == 1000);

    std::remove("gb_tbl.mdb");
    std::remove("gb_tbl.mdb.idx");
    std::puts("test_groupby: passed");
    return 0;
}