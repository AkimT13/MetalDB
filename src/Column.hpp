// Column.hpp — ColumnPage with typed raw-byte slot storage.
#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include <limits>
#include "ValueTypes.hpp"

class ColumnPage
{
public:
    uint16_t pageID;
    uint16_t capacity;      // number of slots
    uint16_t count;         // used slots
    uint16_t nextFreePage;  // free-page list (UINT16_MAX = none)
    uint16_t valueBytes;    // bytes per slot: 4 (UINT32/FLOAT) or 8 (INT64/DOUBLE)

    // Raw value bytes: capacity * valueBytes
    std::vector<uint8_t> rawValues;
    std::vector<bool>    tombstone;  // false=free, true=used

    // Zone-map (stored as int64_t to cover all types)
    int64_t minValue64 = std::numeric_limits<int64_t>::max();
    int64_t maxValue64 = std::numeric_limits<int64_t>::min();

    // Legacy accessors for zone-map (uint32_t view, used by whereBetween)
    ValueType minValue = std::numeric_limits<ValueType>::max();
    ValueType maxValue = std::numeric_limits<ValueType>::min();

    ColumnPage(uint16_t pid, uint16_t slotCount, uint16_t vbytes = sizeof(ValueType))
        : pageID(pid), capacity(slotCount), count(0),
          nextFreePage(std::numeric_limits<uint16_t>::max()),
          valueBytes(vbytes),
          rawValues(size_t(slotCount) * vbytes, 0),
          tombstone(slotCount, false) {}

    // ── Raw slot I/O ─────────────────────────────────────────────────────────
    void writeRaw(int slot, const void* src, uint16_t n) {
        std::memcpy(rawValues.data() + slot * valueBytes, src, n);
    }
    void readRaw(int slot, void* dst, uint16_t n) const {
        std::memcpy(dst, rawValues.data() + slot * valueBytes, n);
    }

    // Legacy uint32_t helpers (used by existing UINT32 code)
    void writeValue(int slotIdx, ValueType val) {
        writeRaw(slotIdx, &val, sizeof(val));
    }
    ValueType readValue(int slotIdx) const {
        ValueType v = 0;
        readRaw(slotIdx, &v, sizeof(v));
        return v;
    }

    // ── Slot management ──────────────────────────────────────────────────────
    int16_t findFreeSlot() const {
        for (uint16_t i = 0; i < capacity; ++i)
            if (!tombstone[i]) return static_cast<int16_t>(i);
        return -1;
    }

    void markUsed(int slotIdx) {
        if (slotIdx < 0 || slotIdx >= capacity) return;
        if (!tombstone[slotIdx]) { tombstone[slotIdx] = true; ++count; }
    }

    void markDeleted(int slotIdx) {
        if (slotIdx < 0 || slotIdx >= capacity) return;
        if (tombstone[slotIdx]) { tombstone[slotIdx] = false; --count; }
    }

    // ── Zone-map ─────────────────────────────────────────────────────────────
    void recomputeMinMax() {
        bool any = false;
        int64_t lo = std::numeric_limits<int64_t>::max();
        int64_t hi = std::numeric_limits<int64_t>::min();
        for (uint16_t i = 0; i < capacity; ++i) {
            if (!tombstone[i]) continue;  // skip free slots
            int64_t v = 0;
            std::memcpy(&v, rawValues.data() + i * valueBytes, valueBytes);
            if (!any) { lo = hi = v; any = true; }
            else { if (v < lo) lo = v; if (v > hi) hi = v; }
        }
        if (any) {
            minValue64 = lo;  maxValue64 = hi;
            // Legacy uint32 view (safe for UINT32 columns)
            minValue = static_cast<ValueType>(lo);
            maxValue = static_cast<ValueType>(hi);
        } else {
            minValue64 = std::numeric_limits<int64_t>::max();
            maxValue64 = std::numeric_limits<int64_t>::min();
            minValue   = std::numeric_limits<ValueType>::max();
            maxValue   = std::numeric_limits<ValueType>::min();
        }
    }
};
