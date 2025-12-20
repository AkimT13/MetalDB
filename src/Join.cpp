#include "Join.hpp"

std::vector<std::pair<uint32_t,uint32_t>>
Join::hashJoinEq(Table& left, uint16_t leftCol, Table& right, uint16_t rightCol) {
    std::unordered_map<ValueType, std::vector<uint32_t>> ht;

    right.rowIndexForEachLive([&](uint32_t rRow, const std::vector<uint32_t>& rSlots){
        auto v = right.columnFile(rightCol).fetchSlot(rSlots[rightCol]);
        if (v) ht[*v].push_back(rRow);
    });

    std::vector<std::pair<uint32_t,uint32_t>> out;
    left.rowIndexForEachLive([&](uint32_t lRow, const std::vector<uint32_t>& lSlots){
        auto v = left.columnFile(leftCol).fetchSlot(lSlots[leftCol]);
        if (!v) return;
        auto it = ht.find(*v);
        if (it == ht.end()) return;
        for (auto rr : it->second) out.emplace_back(lRow, rr);
    });
    return out;
}