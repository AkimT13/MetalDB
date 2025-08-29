// tests/test_table_persist.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"
#include <cassert>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>

int main() {
    char tmpl[] = "/tmp/table_idxXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);
    close(fd);

    // Create table and insert rows
    {
        Table t(tmpl, /*pageSize*/4096, /*numColumns*/3);
        uint32_t r0 = t.insertRow({10,20,30});
        uint32_t r1 = t.insertRow({11,21,31});
        uint32_t r2 = t.insertRow({12,22,32});
        auto row1 = t.fetchRow(r1);
        assert(row1[0] && *row1[0] == 11);
        assert(row1[1] && *row1[1] == 21);
        assert(row1[2] && *row1[2] == 31);
    }

    // Reopen and fetch the same rows by rowID (sidecar index persists)
    {
        Table t2(tmpl); // reopen existing
        auto row0 = t2.fetchRow(0);
        auto row1 = t2.fetchRow(1);
        auto row2 = t2.fetchRow(2);
        assert(row0[0] && *row0[0] == 10);
        assert(row0[1] && *row0[1] == 20);
        assert(row0[2] && *row0[2] == 30);
        assert(row1[0] && *row1[0] == 11);
        assert(row2[2] && *row2[2] == 32);

        // Delete a row and verify tombstoned
        t2.deleteRow(1);
        auto row1gone = t2.fetchRow(1);
        assert(!row1gone[0].has_value() && !row1gone[1].has_value() && !row1gone[2].has_value());
    }

    std::cout << "test_table_persist: passed (row index survived restart)\n";
    unlink(tmpl);
    unlink((std::string(tmpl)+".idx").c_str());
    return 0;
}
