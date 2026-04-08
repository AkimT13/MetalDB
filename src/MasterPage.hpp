#pragma once
#include <cstdint>
#include <vector>
#include "ValueTypes.hpp"

struct MasterPage {
    uint32_t              magic;        // file identifier (0x4D445042)
    uint16_t              pageSize;     // bytes per page
    uint16_t              numColumns;   // how many columns in this file
    std::vector<uint16_t> headPageIDs;  // free-page head per column
    std::vector<ColType>  colTypes;     // per-column type tag (defaults UINT32)

    // Create a brand-new MasterPage (all-UINT32 columns):
    static MasterPage initnew(int fd, uint16_t pageSize, int numColumns);

    // Create a brand-new MasterPage with explicit column types:
    static MasterPage initnew(int fd, uint16_t pageSize,
                              const std::vector<ColType>& types);

    // Load an existing MasterPage from disk (page 0):
    static MasterPage load(int fd);

    // Write the in-memory MasterPage back to page 0:
    void flush(int fd) const;
    void sync(int fd) const;
};



