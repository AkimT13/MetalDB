#pragma once
#include <cstdint>
#include <vector>

struct MasterPage {
    uint32_t             magic;         // file identifier
    uint16_t             pageSize;      // bytes per page
    uint16_t             numColumns;    // how many columns in this file
    std::vector<uint16_t> headPageIDs;  // free‐page head per column

    // Create a brand‐new MasterPage in an empty (or newly truncated) file:
    static MasterPage initnew(int fd, uint16_t pageSize, int numColumns);

    // Load an existing MasterPage from disk (page 0):
    static MasterPage load(int fd);

    // Write the in-memory MasterPage back to page 0:
    void flush(int fd) const;
};




