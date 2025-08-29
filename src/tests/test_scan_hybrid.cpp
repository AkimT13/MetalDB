// tests/test_scan_hybrid.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"
#include <cassert>
#include <algorithm>
#include <iostream>
#include <unistd.h>

extern "C" bool metalIsAvailable();

int main() {
    char tmpl[] = "/tmp/table_hybridXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0); close(fd);

    Table t(tmpl, 4096, 2);
    for (uint32_t i = 0; i < 10000; ++i) {
        t.insertRow({i % 5, i});  // every 5th value matches when scanning for 2
    }

    // Force CPU
    t.setUseGPU(true);
    t.setGPUThreshold(1 << 30); // very large threshold
    auto cpuRows = t.scanEquals(0, 2);

    // Force GPU if available
    std::vector<uint32_t> gpuRows = cpuRows;
    if (metalIsAvailable()) {
        t.setGPUThreshold(1); // tiny threshold → use GPU
        gpuRows = t.scanEquals(0, 2);
        std::sort(cpuRows.begin(), cpuRows.end());
        std::sort(gpuRows.begin(), gpuRows.end());
        assert(cpuRows == gpuRows);
        std::cout << "test_scan_hybrid: passed (CPU==GPU)\n";
    } else {
        std::cout << "test_scan_hybrid: skipped (no Metal device)\n";
    }

    unlink(tmpl);
    unlink((std::string(tmpl) + ".idx").c_str());
    return 0;
}