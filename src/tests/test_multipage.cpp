// tests/test_multipage.cpp
#include "../MasterPage.hpp"
#include "../ColumnFile.hpp"
#include "../ValueTypes.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <set>
#include <iostream>
#include <vector>
#include <cstdio>

// helpers to unpack the 32-bit ID format = (pageID<<16) | slotIdx
static inline uint16_t pageIdFromSlotId(uint32_t id)  { return uint16_t(id >> 16); }
static inline uint16_t slotIdxFromSlotId(uint32_t id) { return uint16_t(id & 0xFFFF); }

int main() {
    // create a temp file path
    char tmpl[] = "/tmp/cf_multipageXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);

    // init master page for one column
    const uint16_t pageSize   = 4096;
    const int      numColumns = 1;
    MasterPage mp = MasterPage::initnew(fd, pageSize, numColumns);
    close(fd); // ColumnFile will reopen by path

    // open a ColumnFile on the same path
    ColumnFile cf(tmpl, mp, /*colIdx=*/0);

    // compute how many slots fit per page (must match ColumnFile’s formula)
    const uint16_t capacity = (pageSize - 8) / (VALUE_SIZE + 1);

    // insert more than one page worth of rows
    const size_t rows = capacity * 2 + capacity / 2; // 2.5 pages
    std::vector<uint32_t> ids;
    ids.reserve(rows);

    for (size_t i = 0; i < rows; ++i) {
        uint32_t id = cf.allocSlot(static_cast<ValueType>(1000 + i));
        ids.push_back(id);
    }

    // verify: at least two distinct page IDs got used
    std::set<uint16_t> pagesUsed;
    for (auto id : ids) pagesUsed.insert(pageIdFromSlotId(id));
    assert(pagesUsed.size() >= 2);

    // verify: round-trip every inserted value
    for (size_t i = 0; i < rows; ++i) {
        auto v = cf.fetchSlot(ids[i]);
        assert(v.has_value());
        assert(*v == static_cast<ValueType>(1000 + i));
    }

    std::cout << "test_multipage: passed ("
              << rows << " rows across " << pagesUsed.size() << " pages)\n";

    // cleanup
    unlink(tmpl);
    return 0;
}
