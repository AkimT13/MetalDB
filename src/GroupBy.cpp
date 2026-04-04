#include "GroupBy.hpp"
#include "gpu_groupby.h"

extern "C" bool metalIsAvailable();

// Choose a hash-table size: next power-of-two >= 4 * distinct keys estimate.
// We use 4× the row count as a conservative upper bound (all keys distinct).
static uint32_t chooseBuckets(size_t n) {
    uint32_t b = 1u;
    while (b < uint32_t(n) * 4u) b <<= 1u;
    return b;
}

// ── CPU helpers ──────────────────────────────────────────────────────────────

static std::unordered_map<ValueType, uint64_t>
cpuCountByKey(Table& t, uint16_t keyCol) {
    std::unordered_map<ValueType, uint64_t> agg;
    t.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>& slots){
        auto v = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        if (v) agg[*v] += 1;
    });
    return agg;
}

static void cpuCountSumByKey(Table& t, uint16_t keyCol, uint16_t valCol,
                              std::unordered_map<ValueType, uint64_t>& cnt,
                              std::unordered_map<ValueType, uint64_t>& sum) {
    t.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>& slots){
        auto k = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        auto v = t.columnFile(valCol).fetchSlot(slots[valCol]);
        if (k && v) { cnt[*k]++; sum[*k] += *v; }
    });
}

// ── Public API ───────────────────────────────────────────────────────────────

std::unordered_map<ValueType, uint64_t>
GroupBy::countByKey(Table& t, uint16_t keyCol, bool useGPU, size_t gpuThreshold) {
    // Materialize key column to decide GPU vs CPU and obtain data.
    auto m = t.materializeColumnWithRowIDs(keyCol);
    const size_t n = m.values.size();

    if (useGPU && n >= gpuThreshold && metalIsAvailable()) {
        std::unordered_map<uint32_t, uint64_t> cnt, sum;
        // Dummy value column (all zeros) — we only need counts.
        std::vector<uint32_t> dummy(n, 0u);
        if (gpuGroupByCountSum(m.values, dummy, chooseBuckets(n), cnt, sum))
            return cnt;
    }

    // CPU fallback
    return cpuCountByKey(t, keyCol);
}

std::unordered_map<ValueType, uint64_t>
GroupBy::sumByKey(Table& t, uint16_t keyCol, uint16_t valCol,
                  bool useGPU, size_t gpuThreshold) {
    auto mk = t.materializeColumnWithRowIDs(keyCol);
    const size_t n = mk.values.size();

    if (useGPU && n >= gpuThreshold && metalIsAvailable()) {
        auto mv = t.materializeColumn(valCol);
        if (mv.size() == n) {
            std::unordered_map<uint32_t, uint64_t> cnt, sum;
            if (gpuGroupByCountSum(mk.values, mv, chooseBuckets(n), cnt, sum))
                return sum;
        }
    }

    // CPU fallback
    std::unordered_map<ValueType, uint64_t> agg;
    t.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>& slots){
        auto k = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        auto v = t.columnFile(valCol).fetchSlot(slots[valCol]);
        if (k && v) agg[*k] += *v;
    });
    return agg;
}

std::unordered_map<ValueType, double>
GroupBy::avgByKey(Table& t, uint16_t keyCol, uint16_t valCol) {
    std::unordered_map<ValueType, uint64_t> sum, cnt;
    cpuCountSumByKey(t, keyCol, valCol, cnt, sum);
    std::unordered_map<ValueType, double> out;
    for (auto& [k, s] : sum) out[k] = double(s) / double(cnt[k]);
    return out;
}

std::unordered_map<ValueType, ValueType>
GroupBy::minByKey(Table& t, uint16_t keyCol, uint16_t valCol) {
    std::unordered_map<ValueType, ValueType> agg;
    t.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>& slots){
        auto k = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        auto v = t.columnFile(valCol).fetchSlot(slots[valCol]);
        if (k && v) {
            auto it = agg.find(*k);
            if (it == agg.end() || *v < it->second) agg[*k] = *v;
        }
    });
    return agg;
}

std::unordered_map<ValueType, ValueType>
GroupBy::maxByKey(Table& t, uint16_t keyCol, uint16_t valCol) {
    std::unordered_map<ValueType, ValueType> agg;
    t.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>& slots){
        auto k = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        auto v = t.columnFile(valCol).fetchSlot(slots[valCol]);
        if (k && v) {
            auto it = agg.find(*k);
            if (it == agg.end() || *v > it->second) agg[*k] = *v;
        }
    });
    return agg;
}
