#include "../Engine.hpp"
#include <cassert>
#include <unistd.h>
#include <cstdio>

int main() {
    Engine e;
    auto& t = e.createTable("eng_tbl", 3, 4096);
    t.setGPUThreshold(1024);

    for (uint32_t i = 0; i < 1000; ++i) {
        e.insert("eng_tbl", {i % 10, i, i+1});
    }

    auto eq = e.whereEq("eng_tbl", 0, 3);
    for (auto rid : eq) (void)rid; // presence check only
    auto rng = e.whereBetween("eng_tbl", 1, 100, 199);
    auto s   = e.sum("eng_tbl", 2);

    assert(!eq.empty());
    assert(!rng.empty());
    assert(s > 0);

    std::remove("eng_tbl.mdb");
    std::remove("eng_tbl.mdb.idx");
    std::puts("test_    ngine: passed");
    return 0;
}