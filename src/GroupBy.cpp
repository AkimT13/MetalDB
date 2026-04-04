#include "GroupBy.hpp"

std::unordered_map<ValueType, uint64_t>
GroupBy::countByKey(Table& t, uint16_t keyCol) {
    std::unordered_map<ValueType, uint64_t> agg;
    t.rowIndexForEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        auto v = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        if (v) agg[*v] += 1;
    });
    return agg;
}

std::unordered_map<ValueType, uint64_t>
GroupBy::sumByKey(Table& t, uint16_t keyCol, uint16_t valCol) {
    std::unordered_map<ValueType, uint64_t> agg;
    t.rowIndexForEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        auto k = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        auto v = t.columnFile(valCol).fetchSlot(slots[valCol]);
        if (k && v) agg[*k] += *v;
    });
    return agg;
}

std::unordered_map<ValueType, double>
GroupBy::avgByKey(Table& t, uint16_t keyCol, uint16_t valCol) {
    std::unordered_map<ValueType, uint64_t> sum;
    std::unordered_map<ValueType, uint64_t> cnt;
    t.rowIndexForEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        auto k = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        auto v = t.columnFile(valCol).fetchSlot(slots[valCol]);
        if (k && v) { sum[*k] += *v; cnt[*k]++; }
    });
    std::unordered_map<ValueType, double> out;
    for (auto& [k, s] : sum) out[k] = double(s) / double(cnt[k]);
    return out;
}

std::unordered_map<ValueType, ValueType>
GroupBy::minByKey(Table& t, uint16_t keyCol, uint16_t valCol) {
    std::unordered_map<ValueType, ValueType> agg;
    t.rowIndexForEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
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
    t.rowIndexForEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        auto k = t.columnFile(keyCol).fetchSlot(slots[keyCol]);
        auto v = t.columnFile(valCol).fetchSlot(slots[valCol]);
        if (k && v) {
            auto it = agg.find(*k);
            if (it == agg.end() || *v > it->second) agg[*k] = *v;
        }
    });
    return agg;
}