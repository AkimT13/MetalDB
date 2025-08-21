// ColumnFile.hpp
#pragma once

#include <string>
#include <optional>
#include <cstdint>
#include "MasterPage.hpp"
#include "ValueTypes.hpp"
#include "Column.hpp"

class ColumnFile {
public:
    // path: the single file backing all columns
    // mp:    the in-memory MasterPage for page-0 metadata
    // colIdx: which column this instance manages
    ColumnFile(const std::string &path, MasterPage &mp, uint16_t colIdx);
    ~ColumnFile();

    // Allocate a slot, write `val`, and return a 32-bit ID = (pageID<<16)|slotIdx
    uint32_t            allocSlot(ValueType val);

    // Read back a slot; returns std::nullopt if it was deleted/tombstoned
    std::optional<ValueType> fetchSlot(uint32_t id);

    // Delete (tombstone) a slot, returning its space to the free-page list
    void                deleteSlot(uint32_t id);

    // Persist any changes to the MasterPage (e.g. updated head-pointer)
    void                flushMaster();

private:
    int                 fd_;        // OS file descriptor for this column file
    MasterPage&         mp_;        // reference to the page-0 metadata
    uint16_t            colIdx_;    // which column (0 <= colIdx_ < mp_.numColumns)
    uint16_t            pageSize_;  // copy of mp_.pageSize for convenience

    // Helpers to extract pageID/slotIdx from a uint32_t slot ID
    static uint16_t     pageIdFromSlotId(uint32_t id)  { return uint16_t(id >> 16); }
    static uint16_t     slotIdxFromSlotId(uint32_t id) { return uint16_t(id & 0xFFFF); }

    // Load or create a page with free slots; returns its pageID
    uint16_t            allocateOrFetchPage();

    // Read a ColumnPage from disk into memory
    ColumnPage          loadPage(uint16_t pageID);

    // Write a ColumnPage back to disk
    void                flushPage(const ColumnPage &page);

    // Helpers to get/set the head of our free-page list
    uint16_t            headPageID() const       { return mp_.headPageIDs[colIdx_]; }
    void                setHeadPageID(uint16_t p){ mp_.headPageIDs[colIdx_]=p; }
};
