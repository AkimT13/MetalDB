// Table.hpp
#pragma once
#include "MasterPage.hpp"
#include "ColumnFile.hpp"
#include "ValueTypes.hpp"

#include <string>
#include <vector>
#include <optional>
#include <cstdint>

class Table {
public:
    // path: the single file backing this table’s columns
    // numColumns: number of columns in this table
    Table(const std::string& path, uint16_t pageSize, uint16_t numColumns);

    // reopen using an existing master page in that file
    explicit Table(const std::string& path);

    // Insert a full row (values.size() must equal numColumns)
    // Returns a new rowID (0-based)
    uint32_t insertRow(const std::vector<ValueType>& values);

    // Fetch a row: vector<optional<ValueType>> so tombstoned cells show as nullopt
    std::vector<std::optional<ValueType>> fetchRow(uint32_t rowID);

    // Delete a row across all columns (tombstone)
    void deleteRow(uint32_t rowID);

    uint16_t numColumns() const { return static_cast<uint16_t>(cols_.size()); }
    uint32_t rowCount() const   { return static_cast<uint32_t>(rowToSlots_.size()); }

private:
    std::string path_;
    int         fd_;           // underlying file descriptor for MasterPage I/O
    MasterPage  mp_;           // page-0 metadata (in memory)

    // one ColumnFile per column
    std::vector<ColumnFile> cols_;

    // row-to-slot mapping: for each row, store the 32-bit slot id per column
    // rowToSlots_[rowID][colIdx] -> (pageID<<16)|slotIdx
    std::vector<std::vector<uint32_t>> rowToSlots_;

    void openOrCreate(uint16_t pageSize, uint16_t numColumns, bool create);
};
