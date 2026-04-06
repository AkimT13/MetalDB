#include "../Engine.hpp"
#include "../GroupBy.hpp"
#include <cassert>
#include <cstdio>
#include <cmath>
#include <chrono>

int main() {
    // ── Small dataset: correctness of all aggregations ────────────────────────
    {
        Engine e;
        auto& t = e.createTable("gb_tbl", 2, 4096);

        // 1000 rows: key = i%5, val = i
        for (uint32_t i = 0; i < 1000; ++i)
            e.insert("gb_tbl", {i % 5, i});

        // count / sum
        auto cnt = GroupBy::countByKey(t, 0, /*useGPU=*/false);
        auto sum = GroupBy::sumByKey(t, 0, 1, /*useGPU=*/false);
        uint64_t totalCount = 0, totalSum = 0;
        for (auto& kv : cnt) totalCount += kv.second;
        for (auto& kv : sum) totalSum  += kv.second;
        assert(totalCount == 1000);
        assert(totalSum == (999ULL * 1000ULL / 2ULL));

        // avg: key 0 has values {0,5,10,...,995}, avg = 497.5
        auto avg = GroupBy::avgByKey(t, 0, 1);
        assert(avg.size() == 5);
        assert(std::fabs(avg[0] - 497.5) < 1.0);

        // min / max
        auto mn = GroupBy::minByKey(t, 0, 1);
        auto mx = GroupBy::maxByKey(t, 0, 1);
        assert(mn[0] == 0);
        assert(mx[0] == 995);

        std::remove("gb_tbl.mdb");
        std::remove("gb_tbl.mdb.idx");
    }

    // ── Large dataset: CPU vs GPU path agreement ──────────────────────────────
    {
        constexpr uint32_t N    = 100'000;
        constexpr uint32_t KEYS = 10;

        Engine e;
        auto& t = e.createTable("gb_large", 2, 4096);
        t.setGPUThreshold(0); // Force GPU path when available

        for (uint32_t i = 0; i < N; ++i)
            e.insert("gb_large", {i % KEYS, i % 1000});

        // CPU reference
        auto cpuCnt = GroupBy::countByKey(t, 0, /*useGPU=*/false);
        auto cpuSum = GroupBy::sumByKey(t, 0, 1, /*useGPU=*/false);

        std::printf("  cpuCnt.size()=%zu  (expect %u)\n", cpuCnt.size(), KEYS);
        assert(cpuCnt.size() == KEYS);
        uint64_t totalCPU = 0;
        for (auto& [k,v] : cpuCnt) totalCPU += v;
        std::printf("  totalCPU=%llu  (expect %u)\n", (unsigned long long)totalCPU, N);
        assert(totalCPU == N);

        // GPU path — measure cold (first call, warms pipeline cache) and hot (cached)
        auto t0 = std::chrono::steady_clock::now();
        auto gpuCnt = GroupBy::countByKey(t, 0, /*useGPU=*/true, /*threshold=*/0);
        auto gpuSum = GroupBy::sumByKey(t, 0, 1, /*useGPU=*/true, /*threshold=*/0);
        auto t1 = std::chrono::steady_clock::now();
        double msCold = std::chrono::duration<double,std::milli>(t1-t0).count();

        // Results must match CPU
        assert(gpuCnt.size() == cpuCnt.size());
        for (auto& [k,v] : cpuCnt) assert(gpuCnt.count(k) && gpuCnt[k] == v);
        for (auto& [k,v] : cpuSum) assert(gpuSum.count(k) && gpuSum[k] == v);

        // Hot call — pipeline already cached
        auto t2 = std::chrono::steady_clock::now();
        auto gpuCnt2 = GroupBy::countByKey(t, 0, /*useGPU=*/true, /*threshold=*/0);
        auto gpuSum2 = GroupBy::sumByKey(t, 0, 1, /*useGPU=*/true, /*threshold=*/0);
        auto t3 = std::chrono::steady_clock::now();
        double msHot = std::chrono::duration<double,std::milli>(t3-t2).count();
        (void)gpuCnt2; (void)gpuSum2;

        std::printf("  GPU group-by (%u rows, %u keys): cold=%.2f ms  hot=%.2f ms\n",
                    N, KEYS, msCold, msHot);

        std::remove("gb_large.mdb");
        std::remove("gb_large.mdb.idx");
    }

    std::puts("test_groupby: passed");
    return 0;
}
