// ColumnPage.hpp
#pragma once
#include <cstdint>
#include <vector>
#include <limits>
#include "ValueTypes.hpp"

class ColumnPage {
public:
    uint16_t pageID;
    uint16_t capacity;       // number of slots
    uint16_t count;          // used slots
    uint16_t nextFreePage;   // unused here, for free-page list

    std::vector<Number> values;
    std::vector<bool>       tombstone;  // false=free, true=used

    // Constructor: initialize an empty page given pageID and slot count
    ColumnPage(uint16_t pid, uint16_t slotCount)
      : pageID(pid), capacity(slotCount), count(0), nextFreePage(std::numeric_limits<uint16_t>::max()),
        values(slotCount), tombstone(slotCount, false) {}

    // Find first free slot (returns index or -1 if none)
    int16_t findFreeSlot() const {
        for (uint16_t i = 0; i < capacity; ++i) {
            if (!tombstone[i]) return i;
        }
        return -1;
    }

    // Mark a slot as used
    void markUsed(int slotIdx) {
        if (slotIdx < 0 || slotIdx >= capacity) return;
        if (!tombstone[slotIdx]) {
            tombstone[slotIdx] = true;
            ++count;
        }
    }

    // Mark a slot as deleted
    void markDeleted(int slotIdx) {
        if (slotIdx < 0 || slotIdx >= capacity) return;
        if (tombstone[slotIdx]) {
            tombstone[slotIdx] = false;
            --count;
        }
    }

    // Read value from a slot (assumes slot used)
    ValueType readValue(int slotIdx) const {
        return values[slotIdx];
    }

    // Write value into a slot (does not alter tombstone/count)
    void writeValue(int slotIdx, ValueType val) {
        values[slotIdx] = val;
    }
};