// test_columnpage.cpp
#include "Column.hpp"
#include <iostream>
#include <cassert>

int main() {
    const uint16_t slotCount = 8;
    ColumnPage page(1, slotCount);

    // Initially, count=0 and all slots free
    assert(page.count == 0);
    for (int i = 0; i < slotCount; ++i) {
        assert(page.tombstone[i] == false);
    }

    // Allocate all slots
    for (int i = 0; i < slotCount; ++i) {
        int16_t slot = page.findFreeSlot();
        assert(slot != -1);
        page.writeValue(slot, static_cast<ValueType>(100 + i));
        page.markUsed(slot);
        assert(page.tombstone[slot] == true);
    }
    // Now no free slots remain
    assert(page.findFreeSlot() == -1);
    assert(page.count == slotCount);

    // Delete every second slot
    for (int i = 0; i < slotCount; i += 2) {
        page.markDeleted(i);
        assert(page.tombstone[i] == false);
    }
    assert(page.count == slotCount - (slotCount/2));

    // Ensure free slots are reused
    int16_t reused = page.findFreeSlot();
    assert(reused != -1 && reused % 2 == 0);
    page.writeValue(reused, static_cast<ValueType>(200 + reused));
    page.markUsed(reused);
    assert(page.tombstone[reused] == true);
    assert(page.count == slotCount - (slotCount/2) + 1);

    std::cout << "ColumnPage in-memory test passed!" << std::endl;
    return 0;
}
