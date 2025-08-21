// test_columnfile.cpp
#include "MasterPage.hpp"
#include "ColumnFile.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <iostream>

int main() {
    // 1) Create temp file and init page 0 for 1 column
    char tmpl[] = "/tmp/cf_testXXXXXX";
    int fd = mkstemp(tmpl);
    MasterPage mp = MasterPage::initnew(fd, /*pageSize=*/4096, /*numColumns=*/1);
    close(fd);

    // 2) Open ColumnFile on that path
    ColumnFile cf(tmpl, mp, /*colIdx=*/0);

    // 3) Insert a few values
    auto id1 = cf.allocSlot(123);
    auto id2 = cf.allocSlot(456);

    // 4) Fetch them back
    auto v1 = cf.fetchSlot(id1);
    auto v2 = cf.fetchSlot(id2);
    assert(v1 && *v1 == 123);
    assert(v2 && *v2 == 456);

    // 5) Delete one and verify nullopt
    cf.deleteSlot(id1);
    auto v1b = cf.fetchSlot(id1);
    assert(!v1b);

    std::cout << "ColumnFile tests passed!\n";
    unlink(tmpl);
    return 0;
}
