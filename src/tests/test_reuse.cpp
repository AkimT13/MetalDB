// tests/test_reuse.cpp
#include "../MasterPage.hpp"
#include "../ColumnFile.hpp"
#include "../ValueTypes.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <iostream>
#include <cstdio>

static inline uint16_t pageIdFromSlotId(uint32_t id)  { return uint16_t(id >> 16); }
static inline uint16_t slotIdxFromSlotId(uint32_t id) { return uint16_t(id & 0xFFFF); }

int main() {
    char tmpl[] = "/tmp/cf_reuseXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);

    MasterPage mp = MasterPage::initnew(fd, 4096, 1);
    close(fd);

    ColumnFile cf(tmpl, mp, 0);

    // allocate two values, then delete the first
    uint32_t id1 = cf.allocSlot(111);
    uint32_t id2 = cf.allocSlot(222);
    auto v1 = cf.fetchSlot(id1);
    auto v2 = cf.fetchSlot(id2);
    assert(v1 && *v1 == 111);
    assert(v2 && *v2 == 222);

    cf.deleteSlot(id1);
    auto v1gone = cf.fetchSlot(id1);
    assert(!v1gone.has_value());

    // allocate new value; expect it to reuse the same page, maybe even same slot
    uint32_t id3 = cf.allocSlot(333);
    auto v3 = cf.fetchSlot(id3);
    assert(v3 && *v3 == 333);

    // at least ensure it used the same page (since page had become not-full)
    assert(pageIdFromSlotId(id3) == pageIdFromSlotId(id2));

    std::cout << "test_reuse: passed (delete + reuse works)\n";
    unlink(tmpl);
    return 0;
}
