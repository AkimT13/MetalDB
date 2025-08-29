// tests/test_gpu_sum.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"
#include <cassert>
#include <iostream>
#include <unistd.h>

extern "C" bool metalIsAvailable();
uint64_t gpuSumU32(const std::vector<uint32_t>& values);

int main() {
    char tmpl[] = "/tmp/table_sumXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0); close(fd);

    std::string idx = std::string(tmpl) + ".idx";
    std::cout << "[TEST] base: " << tmpl << "\n";

    uint64_t expect = 0;

    {
        std::cout << "[TEST] build table & rows\n";
        Table t(tmpl, 4096, 1);
        const uint32_t N = 10000;
        for (uint32_t i = 0; i < N; ++i) { t.insertRow({i}); expect += i; }

        // CPU baseline
        auto cpu = t.sumColumn(0);
        assert(static_cast<uint64_t>(cpu) == expect);

        // Hybrid – force GPU if available
        t.setUseGPU(true);
        t.setGPUThreshold(1);
        auto hybrid = t.sumColumnHybrid(0);

        if (metalIsAvailable()) {
            assert(static_cast<uint64_t>(hybrid) == expect);
            std::cout << "test_gpu_sum: passed (GPU==CPU)\n";
        } else {
            std::cout << "test_gpu_sum: skipped (no Metal device)\n";
        }
    } // Table destructs here

    std::cout << "[TEST] unlink files\n";
    unlink(tmpl);
    unlink(idx.c_str());
    std::cout << "[TEST] done\n";
    return 0;
}