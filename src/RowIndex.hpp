// RowIndex.hpp
#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <functional>

class RowIndex {
public:
    // pathBase is the table file path; the index lives at pathBase + ".idx"
    RowIndex(const std::string& pathBase, uint16_t numColumns);

    // Open existing (.idx) or create new if missing
    void openOrCreate();

    // Append a new row’s slotIDs (size must equal numColumns). Returns rowID.
    uint32_t appendRow(const std::vector<uint32_t>& slotIDs);

    // Mark a rowID as deleted (status = 0)
    void markDeleted(uint32_t rowID);

    // Fetch the slotIDs for a row. Returns nullopt if deleted or out of range.
    std::optional<std::vector<uint32_t>> fetch(uint32_t rowID) const;

    // Number of rows recorded (includes deleted)
    uint32_t rowsRecorded() const { return static_cast<uint32_t>(entries_.size()); }

    // Number of live rows (cheap estimate: rowsRecorded - deletedCount)
    uint32_t liveRows() const { return rowsRecorded() - deletedCount_; }
    void forEachLive(const std::function<void(uint32_t, const std::vector<uint32_t>&)>& fn) const;

   

    // Load all rows from disk (called by openOrCreate)
    void loadAll();

private:
    struct Entry {
        uint8_t status;                  // 1=live, 0=deleted
        std::vector<uint32_t> slots;     // one slotID per column
    };

    std::string idxPath_;
    uint16_t    numColumns_;
    int         fd_;

    // In-memory cache of all entries for simplicity
    std::vector<Entry> entries_;
    uint32_t           deletedCount_ = 0;

    // On-disk format:
    // Header:
    //   uint32_t magic = 0x52494458 ('R','I','D','X')
    //   uint16_t numColumns
    //   uint16_t reserved = 0
    //
    // Entries (repeated):
    //   uint8_t  status (1=live, 0=deleted)
    //   uint8_t  pad[3] = {0,0,0}
    //   uint32_t slotIDs[numColumns]
    //
    // RowID = entry index (0-based) in this file.

    void ensureHeaderOnCreate();
    void writeEntry(uint32_t rowID, const Entry& e);
    void readEntryAt(uint32_t rowID, Entry& out);
};
