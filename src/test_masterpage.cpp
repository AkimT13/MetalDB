// test_masterpage.cpp
#include "MasterPage.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <iostream>

int main() {
    // 1) Create a temp file
    char tmpl[] = "/tmp/mp_testXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);

    // 2) Call the static initnew method
    MasterPage mp1 = MasterPage::initnew(fd, /*pageSize=*/4096, /*numColumns=*/3);
    
    // 3) Use mp1.headPageIDs (not headPageIDs alone!)
    assert(mp1.magic      == 0x4D445042);
    assert(mp1.pageSize   == 4096);
    assert(mp1.numColumns == 3);
    assert(mp1.headPageIDs.size() == 3);
    for (uint16_t h : mp1.headPageIDs) {
        assert(h == UINT16_MAX);
    }

    // 4) Load it back and compare via mp2.headPageIDs
    MasterPage mp2 = MasterPage::load(fd);
    assert(mp2.headPageIDs == mp1.headPageIDs);

    // 5) Mutate and flush
    mp2.headPageIDs[1] = 42;
    mp2.flush(fd);
    MasterPage mp3 = MasterPage::load(fd);
    assert(mp3.headPageIDs[1] == 42);

    std::cout << "MasterPage tests passed!\n";
    close(fd);
    unlink(tmpl);
    return 0;
}
