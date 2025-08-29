// tests/test_table.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"

#include <cassert>
#include <iostream>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

static inline uint16_t pageIdFromSlotId(uint32_t id)  { return uint16_t(id >> 16); }
static inline uint16_t slotIdxFromSlotId(uint32_t id) { return uint16_t(id & 0xFFFF); }

int main() {
    // temp file path for this table
    char tmpl[] = "/tmp/table_testXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);
    close(fd); // Table will open it itself

    // build a 3-column table
    Table t(tmpl, /*pageSize*/4096, /*numColumns*/3);

    // insert a few rows
    uint32_t r0 = t.insertRow({10, 20, 30});
    uint32_t r1 = t.insertRow({11, 21, 31});
    uint32_t r2 = t.insertRow({12, 22, 32});

    assert(t.rowCount() == 3);
    assert(t.numColumns() == 3);

    // fetch and verify
    auto row1 = t.fetchRow(r1);
    assert(row1.size() == 3);
    assert(row1[0] && *row1[0] == 11);
    assert(row1[1] && *row1[1] == 21);
    assert(row1[2] && *row1[2] == 31);

    // delete middle row
    t.deleteRow(r1);
    auto row1b = t.fetchRow(r1);
    assert(!row1b[0].has_value());
    assert(!row1b[1].has_value());
    assert(!row1b[2].has_value());

    // insert another row; should reuse space in the same pages
    uint32_t r3 = t.insertRow({101, 201, 301});
    auto row3 = t.fetchRow(r3);
    assert(row3[0] && *row3[0] == 101);
    assert(row3[1] && *row3[1] == 201);
    assert(row3[2] && *row3[2] == 301);

    std::cout << "test_table: passed (insert/fetch/delete across 3 columns)\n";
    unlink(tmpl);
    return 0;
}
