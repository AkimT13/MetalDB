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