// ColumnFile.hpp
#pragma once

#include <string>
#include <optional>
#include <cstdint>
#include <utility>
#include <unordered_map>
#include "MasterPage.hpp"
#include "ValueTypes.hpp"
#include "Column.hpp"

class ColumnFile
{
public:
    // path: the single file backing all columns
    // mp:    the in-memory MasterPage for page-0 metadata
    // colIdx: which column this instance manages
    ColumnFile(const std::string &path, MasterPage &mp, uint16_t colIdx);
    ~ColumnFile();

    // ── Legacy API (UINT32 columns) ──────────────────────────────────────────
    // Allocate a slot, write `val`, and return a 32-bit ID = (pageID<<16)|slotIdx
    uint32_t allocSlot(ValueType val);

    // Read back a slot; returns std::nullopt if it was deleted/tombstoned
    std::optional<ValueType> fetchSlot(uint32_t id);

    // ── Typed API ────────────────────────────────────────────────────────────
    uint32_t             allocTypedSlot(const ColValue& val);
    std::optional<ColValue> fetchTypedSlot(uint32_t id) const;

    // Delete (tombstone) a slot, returning its space to the free-page list
    void deleteSlot(uint32_t id);

    // Persist any changes to the MasterPage (e.g. updated head-pointer)
    void flushMaster();

    // Number of pages = file_size / pageSize_
    uint16_t pageCount() const;

    // Cheap zone-map read (header-only): returns {minValue, maxValue} as uint32_t
    std::pair<ValueType, ValueType> zoneMap(uint16_t pageID) const;

    ColType colType() const { return colType_; }

    // Decode composite slotID
    static inline uint16_t pageIdFromSlotId(uint32_t id) { return uint16_t(id >> 16); }
    static inline uint16_t slotIdxFromSlotId(uint32_t id) { return uint16_t(id & 0xFFFF); }

private:
    int fd_;            // OS file descriptor for this column file
    MasterPage &mp_;    // reference to the page-0 metadata
    uint16_t colIdx_;   // which column (0 <= colIdx_ < mp_.numColumns)
    uint16_t pageSize_; // copy of mp_.pageSize for convenience
    ColType  colType_;  // type tag for this column
    uint16_t valueBytes_; // bytes per slot: 4 or 8

    // In-memory page cache: avoids re-reading pages from disk on every fetchSlot
    mutable std::unordered_map<uint16_t, ColumnPage> pageCache_;

    // Load or create a page with free slots; returns its pageID
    uint16_t allocateOrFetchPage();

    // Read / write a typed page (loadPage populates pageCache_)
    ColumnPage loadPage(uint16_t pageID) const;
    void flushPage(const ColumnPage &page);

    // Ensure a page is in the cache and return a reference to it (no copy).
    // Only safe while no mutation of pageCache_ occurs.
    const ColumnPage& pageRef(uint16_t pageID) const;

    // Helpers to get/set the head of our free-page list
    uint16_t headPageID() const { return mp_.headPageIDs[colIdx_]; }
    void setHeadPageID(uint16_t p) { mp_.headPageIDs[colIdx_] = p; }
};