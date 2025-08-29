// tests/bench_scan_sum.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <unistd.h>

extern "C" bool metalIsAvailable();

using Clock = std::chrono::high_resolution_clock;

static void fillTable(Table& t, uint32_t N, uint32_t modForSelectivity) {
    // col0 = i % modForSelectivity   (controls selectivity for value==needle)
    // col1 = i                       (payload for later)
    for (uint32_t i = 0; i < N; ++i) {
        t.insertRow({ static_cast<ValueType>(i % modForSelectivity),
                      static_cast<ValueType>(i) });
    }
}

static double secSince(Clock::time_point t0, Clock::time_point t1) {
    return std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
}

int main() {
    std::cout << "Metal available: " << (metalIsAvailable() ? "yes" : "no") << "\n";

    struct Case { uint32_t N; uint32_t mod; uint32_t needle; };
    std::vector<Case> cases = {
        {100'000, 2, 0},     // ~50% match
        {100'000, 10, 0},    // ~10% match
        {1'000'000, 10, 0},  // 1M rows ~10% match
        {5'000'000, 10, 0},  // 5M rows ~10% match
    };

    for (auto c : cases) {
        char tmpl[] = "/tmp/bench_tableXXXXXX";
        int fd = mkstemp(tmpl);
        assert(fd >= 0); close(fd);
        std::string idx = std::string(tmpl) + ".idx";

        Table t(tmpl, 4096, 2);
        t.setUseGPU(true);
        t.setGPUThreshold(4096); // pick a threshold; tweak as needed

        fillTable(t, c.N, c.mod);

        // --- scanEquals CPU
        t.setGPUThreshold(1u << 30); // force CPU
        auto t0 = Clock::now();
        auto cpuRows = t.scanEquals(0, c.needle);
        auto t1 = Clock::now();
        double cpuScanSec = secSince(t0, t1);

        // --- scanEquals GPU (if available)
        double gpuScanSec = -1.0;
        if (metalIsAvailable()) {
            t.setGPUThreshold(1); // force GPU
            t0 = Clock::now();
            auto gpuRows = t.scanEquals(0, c.needle);
            t1 = Clock::now();
            gpuScanSec = secSince(t0, t1);

            // basic correctness
            std::sort(cpuRows.begin(), cpuRows.end());
            std::sort(gpuRows.begin(), gpuRows.end());
            assert(cpuRows == gpuRows);
        }

        // --- sum CPU
        t.setGPUThreshold(1u << 30); // force CPU
        t0 = Clock::now();
        auto cpuSum = t.sumColumn(1);
        t1 = Clock::now();
        double cpuSumSec = secSince(t0, t1);

        // --- sum GPU (hybrid)
        double gpuSumSec = -1.0;
        if (metalIsAvailable()) {
            t.setGPUThreshold(1); // force GPU
            t0 = Clock::now();
            auto gpuSum = t.sumColumnHybrid(1);
            t1 = Clock::now();
            gpuSumSec = secSince(t0, t1);
            assert(static_cast<uint64_t>(gpuSum) == static_cast<uint64_t>(cpuSum));
        }

        auto rowsPerSec = [&](double s) {
            return (s > 0.0) ? (double)c.N / s : 0.0;
        };

        std::cout << "\nN=" << c.N << "  selectivity≈" << (100 / c.mod) << "%\n";
        std::cout << "scanEquals CPU: " << cpuScanSec << "s  (" << rowsPerSec(cpuScanSec) << " rows/s)\n";
        if (gpuScanSec >= 0.0)
            std::cout << "scanEquals GPU: " << gpuScanSec << "s  (" << rowsPerSec(gpuScanSec) << " rows/s)\n";
        else
            std::cout << "scanEquals GPU: skipped\n";

        std::cout << "sum CPU:        " << cpuSumSec << "s  (" << rowsPerSec(cpuSumSec) << " rows/s)\n";
        if (gpuSumSec >= 0.0)
            std::cout << "sum GPU:        " << gpuSumSec << "s  (" << rowsPerSec(gpuSumSec) << " rows/s)\n";
        else
            std::cout << "sum GPU:        skipped\n";

        unlink(tmpl);
        unlink(idx.c_str());
    }

    return 0;
}