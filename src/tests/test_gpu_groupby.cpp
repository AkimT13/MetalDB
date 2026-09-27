// GPU group-by / sum correctness vs. the CPU path. Targets the bugs fixed in the
// Metal kernels:
//   - per-group SUM beyond 2^32 (was 32-bit atomics: silently wrapped)
//   - key 0xFFFFFFFF (collided with the kernel's empty-slot sentinel)
//   - more distinct keys than the initial hash table (rows were dropped when
//     probing failed; now the host retries with a larger table)
//   - whole-column SUM beyond 2^32 through the hybrid path
// On machines without Metal both sides take the CPU path and the test still
// checks the expected values computed independently below.
#include "../GroupBy.hpp"
#include "../Table.hpp"

#include <cassert>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" bool metalIsAvailable();

namespace {

void removeTable(const std::string& base) {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal"}) std::remove((base + ext).c_str());
}

template <typename Map>
void expectSame(const Map& gpu, const Map& cpu, const char* what) {
    if (gpu == cpu) return;
    std::fprintf(stderr, "MISMATCH (%s): gpu has %zu groups, cpu has %zu\n", what, gpu.size(), cpu.size());
    size_t shown = 0;
    for (const auto& [k, v] : cpu) {
        auto it = gpu.find(k);
        if (it == gpu.end() || it->second != v) {
            std::fprintf(stderr, "  key %u: gpu=%llu cpu=%llu\n", k,
                         it == gpu.end() ? 0ull : static_cast<unsigned long long>(it->second),
                         static_cast<unsigned long long>(v));
            if (++shown == 5) break;
        }
    }
    std::exit(1);
}

}  // namespace

int main() {
    const std::string base = "/tmp/gpu_groupby_tbl";

    // ── Low cardinality, huge values: sums far beyond 2^32 ──────────────────
    removeTable(base);
    {
        Table t(base + ".mdb", 16384, 2);
        std::unordered_map<uint32_t, uint64_t> expCount, expSum;
        std::vector<std::vector<ColValue>> batch;
        const uint32_t n = 60'000;
        for (uint32_t i = 0; i < n; ++i) {
            const uint32_t key = (i % 7 == 0) ? 0xFFFFFFFFu : (i % 13);  // includes the sentinel value
            const uint32_t val = 0xFFFFFFF0u - (i % 1000);                // near UINT32_MAX
            batch.push_back({ColValue(key), ColValue(val)});
            ++expCount[key];
            expSum[key] += val;
        }
        t.applyAtomic({}, batch);
        for (const auto& [k, s] : expSum) assert(k == 0xFFFFFFFFu || s > (1ull << 32));  // really overflows 32 bits

        const auto cpuCount = GroupBy::countByKey(t, 0, /*useGPU=*/false);
        const auto cpuSum = GroupBy::sumByKey(t, 0, 1, /*useGPU=*/false);
        expectSame(cpuCount, expCount, "cpu count vs expected");
        expectSame(cpuSum, expSum, "cpu sum vs expected");

        const auto gpuCount = GroupBy::countByKey(t, 0, /*useGPU=*/true, /*gpuThreshold=*/0);
        const auto gpuSum = GroupBy::sumByKey(t, 0, 1, /*useGPU=*/true, /*gpuThreshold=*/0);
        expectSame(gpuCount, expCount, "gpu count (low cardinality + sentinel key)");
        expectSame(gpuSum, expSum, "gpu sum (64-bit per group)");

        // Whole-column sum: ~5e14, far beyond 2^32.
        uint64_t total = 0;
        for (const auto& [k, s] : expSum) total += s;
        t.setUseGPU(false);
        assert(t.sumColumn64(1) == total);
        t.setUseGPU(true);
        t.setGPUThreshold(0);
        assert(t.sumColumn64(1) == total);
    }

    // ── High cardinality: more distinct keys than the initial GPU table ─────
    removeTable(base);
    {
        Table t(base + ".mdb", 16384, 2);
        std::vector<std::vector<ColValue>> batch;
        // 100k distinct keys (> the GPU table's initial 65536 buckets), shuffled
        // by a bijection mod 100k, plus 20k repeats.
        const uint32_t n = 120'000;
        for (uint32_t i = 0; i < n; ++i)
            batch.push_back({ColValue(static_cast<uint32_t>((uint64_t(i) * 7919u) % 100'000u)), ColValue(i)});
        t.applyAtomic({}, batch);

        const auto cpuCount = GroupBy::countByKey(t, 0, false);
        const auto cpuSum = GroupBy::sumByKey(t, 0, 1, false);
        assert(cpuCount.size() == 100'000);
        expectSame(GroupBy::countByKey(t, 0, true, 0), cpuCount, "gpu count (high cardinality)");
        expectSame(GroupBy::sumByKey(t, 0, 1, true, 0), cpuSum, "gpu sum (high cardinality)");
    }

    // ── Deleted rows are excluded on both paths ─────────────────────────────
    removeTable(base);
    {
        Table t(base + ".mdb", 4096, 2);
        std::vector<std::vector<ColValue>> batch;
        for (uint32_t i = 0; i < 20'000; ++i) batch.push_back({ColValue(i % 5), ColValue(1u)});
        t.applyAtomic({}, batch);
        std::vector<uint32_t> dels;
        for (uint32_t i = 0; i < 20'000; i += 5) dels.push_back(i);  // every key-0 row
        t.applyAtomic(dels, {});
        const auto gpu = GroupBy::countByKey(t, 0, true, 0);
        assert(gpu.count(0) == 0 && gpu.at(1) == 4000);
        expectSame(gpu, GroupBy::countByKey(t, 0, false), "gpu count after deletes");
    }

    removeTable(base);
    std::printf("test_gpu_groupby: passed (%s)\n", metalIsAvailable() ? "GPU exercised" : "CPU-only");
    return 0;
}
