#include "../Engine.hpp"
#include "../GroupBy.hpp"
#include <cassert>
#include <cstdio>
#include <cmath>

int main() {
    Engine e;
    auto& t = e.createTable("gb_tbl", 2, 4096);

    // 1000 rows: key = i%5, val = 1
    for (uint32_t i = 0; i < 1000; ++i) {
        e.insert("gb_tbl", {i % 5, i});
    }

    // count / sum
    auto cnt = GroupBy::countByKey(t, 0);
    auto sum = GroupBy::sumByKey(t, 0, 1);
    uint64_t totalCount = 0, totalSum = 0;
    for (auto& kv : cnt) totalCount += kv.second;
    for (auto& kv : sum) totalSum += kv.second;
    assert(totalCount == 1000);
    assert(totalSum == (999 * 1000 / 2)); // sum 0..999

    // avg: key 0 has values {0,5,10,...,995}, avg = 497.5
    auto avg = GroupBy::avgByKey(t, 0, 1);
    assert(avg.size() == 5);
    assert(std::fabs(avg[0] - 497.5) < 1.0);

    // min / max per key
    auto mn = GroupBy::minByKey(t, 0, 1);
    auto mx = GroupBy::maxByKey(t, 0, 1);
    assert(mn[0] == 0);    // key 0: smallest value is 0
    assert(mx[0] == 995);  // key 0: largest value is 995

    std::remove("gb_tbl.mdb");
    std::remove("gb_tbl.mdb.idx");
    std::puts("test_groupby: passed");
    return 0;
}
