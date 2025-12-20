// tests/test_where_range.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"
#include <cassert>
#include <iostream>
#include <algorithm>
#include <unistd.h>

int main() {
    char tmpl[] = "/tmp/table_rangeXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0); close(fd);

    Table t(tmpl, /*pageSize=*/4096, /*numColumns=*/2);
    t.setUseGPU(true);
    t.setGPUThreshold(2048);

    const uint32_t N = 20000;
    for (uint32_t i = 0; i < N; ++i) {
        t.insertRow({ static_cast<ValueType>(i % 200),
                      static_cast<ValueType>(i) });
    }

    // CPU baseline from materialized + filter
    auto m = t.materializeColumnWithRowIDs(0);
    std::vector<uint32_t> cpu;
    for (size_t i = 0; i < m.values.size(); ++i) {
        if (m.values[i] >= 50 && m.values[i] <= 120) cpu.push_back(m.rowIDs[i]);
    }
    std::sort(cpu.begin(), cpu.end());

    // DB path with pruning + GPU
    auto got = t.whereBetween(0, 50, 120);
    std::sort(got.begin(), got.end());

    assert(cpu == got);
    std::cout << "test_where_range: passed (zone-map prune + GPU)\n";

    unlink(tmpl);
    unlink((std::string(tmpl) + ".idx").c_str());
    return 0;
}