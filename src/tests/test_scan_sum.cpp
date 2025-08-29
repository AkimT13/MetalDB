// tests/test_scan_sum.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"

#include <cassert>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>

int main() {
    char tmpl[] = "/tmp/table_scanXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);
    close(fd);

    // build a 2-column table
    Table t(tmpl, /*pageSize*/4096, /*numColumns*/2);

    // insert rows with some repeats
    t.insertRow({1, 10});
    t.insertRow({2, 20});
    t.insertRow({3, 30});
    t.insertRow({2, 40});
    t.insertRow({5, 50});
    t.insertRow({2, 60});

    // materialize col 0 and col 1
    auto col0 = t.materializeColumn(0);
    auto col1 = t.materializeColumn(1);
    assert(col0.size() == 6);
    assert(col1.size() == 6);

    // scanEquals on col 0 for value 2 → expect 3 matches (rows 1,3,5)
    auto rowsEq2 = t.scanEquals(0, 2);
    assert(rowsEq2.size() == 3);

    // sumColumn on col 1 → 10+20+30+40+50+60 = 210
    auto sum = t.sumColumn(1);
    assert(sum == 210);

    // delete one row with 2 and verify scan re-counts
    t.deleteRow(rowsEq2[0]);
    auto rowsEq2b = t.scanEquals(0, 2);
    assert(rowsEq2b.size() == 2);

    std::cout << "test_scan_sum: passed (materialize/scan/sum)\n";
    unlink(tmpl);
    unlink((std::string(tmpl) + ".idx").c_str());
    return 0;
}