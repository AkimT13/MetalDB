#include "../Engine.hpp"
#include "../Join.hpp"
#include <cassert>
#include <cstdio>

int main() {
    Engine e;
    auto& A = e.createTable("A", 2, 4096);
    auto& B = e.createTable("B", 2, 4096);

    for (uint32_t i = 0; i < 100; ++i) e.insert("A", {i % 10, i});
    for (uint32_t i = 0; i < 50;  ++i) e.insert("B", {i % 10, i*2});

    auto pairs = Join::hashJoinEq(A, 0, B, 0); // join on col0
    assert(!pairs.empty());

    std::remove("A.mdb"); std::remove("A.mdb.idx");
    std::remove("B.mdb"); std::remove("B.mdb.idx");
    std::puts("test_join: passed");
    return 0;
}