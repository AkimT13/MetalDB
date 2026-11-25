// GPU vs CPU benchmark + cross-check for every hybrid operation.
//
// For each table size, times each operation on the CPU path (setUseGPU(false))
// and the GPU-eligible path (setUseGPU(true), threshold 0), takes the median of
// several runs, and verifies both paths return identical results. Prints a
// Markdown table; exits 1 on any mismatch. Without Metal both columns are CPU
// timings and the cross-check still runs.
//
//   ./bench_gpu                 # sizes 10k, 100k, 1M
//   BENCH_MAX_ROWS=4000000 ./bench_gpu
#include "../GroupBy.hpp"
#include "../Table.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" bool metalIsAvailable();

namespace {

using Clock = std::chrono::steady_clock;

double medianMs(const std::function<void()>& fn, int runs) {
    std::vector<double> t;
    for (int i = 0; i < runs; ++i) {
        const auto start = Clock::now();
        fn();
        t.push_back(std::chrono::duration<double, std::milli>(Clock::now() - start).count());
    }
    std::sort(t.begin(), t.end());
    return t[t.size() / 2];
}

void removeTable(const std::string& base) {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.2.str"}) std::remove((base + ext).c_str());
}

int g_mismatches = 0;

template <typename Result>
void bench(Table& t, size_t rows, const char* name, const std::function<Result()>& op) {
    const int runs = rows >= 1'000'000 ? 3 : 5;
    Result cpu{}, gpu{};
    t.setUseGPU(false);
    const double cpuMs = medianMs([&] { cpu = op(); }, runs);
    t.setUseGPU(true);
    t.setGPUThreshold(0);
    const double gpuMs = medianMs([&] { gpu = op(); }, runs);
    const bool same = cpu == gpu;
    if (!same) ++g_mismatches;
    std::printf("| %9zu | %-28s | %10.2f | %10.2f | %6.2fx | %s |\n", rows, name, cpuMs, gpuMs,
                gpuMs > 0 ? cpuMs / gpuMs : 0.0, same ? "ok" : "**MISMATCH**");
    std::fflush(stdout);
}

std::vector<uint32_t> sorted(std::vector<uint32_t> v) {
    std::sort(v.begin(), v.end());
    return v;
}

}  // namespace

int main() {
    size_t maxRows = 1'000'000;
    if (const char* env = std::getenv("BENCH_MAX_ROWS")) maxRows = std::strtoull(env, nullptr, 10);
    std::vector<size_t> sizes;
    for (size_t n : {10'000ul, 100'000ul, 1'000'000ul, 4'000'000ul})
        if (n <= maxRows) sizes.push_back(n);

    std::printf("Metal available: %s\n\n", metalIsAvailable() ? "yes" : "no (both columns are CPU timings)");
    std::printf("| rows      | operation                    | CPU ms     | GPU ms     | speedup | result |\n");
    std::printf("|-----------|------------------------------|------------|------------|---------|--------|\n");

    const std::string base = "/tmp/bench_gpu_tbl";
    for (size_t n : sizes) {
        removeTable(base);
        Table t(base + ".mdb", 16384, std::vector<ColType>{ColType::UINT32, ColType::UINT32, ColType::STRING});
        static const char* tags[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
        std::vector<std::vector<ColValue>> batch;
        for (size_t i = 0; i < n; ++i) {
            batch.push_back({ColValue(static_cast<uint32_t>(i % 100)),                         // c0: 100 groups
                             ColValue(static_cast<uint32_t>((i * 2654435761u) % 1'000'000)),  // c1: payload
                             ColValue(std::string(tags[i % 5]))});                            // c2: tag
            if (batch.size() == 50'000) {
                t.applyAtomic({}, batch);
                batch.clear();
            }
        }
        if (!batch.empty()) t.applyAtomic({}, batch);
        t.flushDurable();

        bench<std::vector<uint32_t>>(t, n, "scan c0 = 7", [&] { return sorted(t.scanEquals(0, 7)); });
        bench<std::vector<uint32_t>>(t, n, "range c1 in [0, 100000]",
                                     [&] { return sorted(t.whereBetween(1, 0, 100'000)); });
        bench<uint64_t>(t, n, "sum(c1)", [&] { return t.sumColumn64(1); });
        bench<std::unordered_map<ValueType, uint64_t>>(
            t, n, "group by c0: count", [&] { return GroupBy::countByKey(t, 0, t.useGPU(), t.gpuThreshold()); });
        bench<std::unordered_map<ValueType, uint64_t>>(
            t, n, "group by c0: sum(c1)", [&] { return GroupBy::sumByKey(t, 0, 1, t.useGPU(), t.gpuThreshold()); });
        bench<std::vector<uint32_t>>(t, n, "string c2 = 'gamma'",
                                     [&] { return sorted(t.scanEqualsString(2, "gamma")); });
    }
    removeTable(base);

    if (g_mismatches) {
        std::printf("\n%d MISMATCH(ES): GPU and CPU paths disagree\n", g_mismatches);
        return 1;
    }
    std::printf("\nAll GPU results match the CPU path.\n");
    return 0;
}
