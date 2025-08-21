// ColumnFile.cpp  (replace the stubbed methods with these)
#include "ColumnFile.hpp"
#include "ValueTypes.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <sys/stat.h>
#include <cstdio>
#include <vector>

// I store one byte per tombstone to keep things simple (0 = free, 1 = used).
// On-disk layout (little-endian):
//   [0..1]   uint16_t pageID
//   [2..3]   uint16_t capacity
//   [4..5]   uint16_t count
//   [6..7]   uint16_t nextFreePage
//   [8..8+cap*VALUE_SIZE-1]          values[]
//   [8+cap*VALUE_SIZE .. +cap-1]     tombstone bytes (cap bytes)

static uint16_t computeCapacity(uint16_t pageSize) {
    // header=8 bytes; each slot = VALUE_SIZE + 1 tombstone byte
    // ensure at least one slot fits
    uint32_t usable = pageSize >= 8 ? (pageSize - 8) : 0;
    uint32_t per    = VALUE_SIZE + 1;
    uint32_t cap    = per ? (usable / per) : 0;
    if (cap > 0xFFFF) cap = 0xFFFF;
    return static_cast<uint16_t>(cap);
}

ColumnFile::ColumnFile(const std::string &path, MasterPage &mp, uint16_t colIdx)
  : fd_(-1), mp_(mp), colIdx_(colIdx), pageSize_(mp.pageSize)
{
    fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0666);
    assert(fd_ >= 0);
}

ColumnFile::~ColumnFile() {
    if (fd_ >= 0) close(fd_);
}

uint16_t ColumnFile::allocateOrFetchPage() {
    uint16_t pid = headPageID();
    if (pid == UINT16_MAX) {
        // Append a new zeroed page and initialize its header + arrays.
        off_t end = lseek(fd_, 0, SEEK_END);
        assert(end >= 0);
        pid = static_cast<uint16_t>(end / pageSize_);

        // Grow file by one page
        if (ftruncate(fd_, end + pageSize_) == -1) {
            perror("ftruncate");
        }

        // Build an in-memory empty page and flush it to disk
        uint16_t cap = computeCapacity(pageSize_);
        ColumnPage page(pid, cap); // count=0, tombstone=false
        page.nextFreePage = UINT16_MAX;
        flushPage(page);

        // Set head to this new page
        setHeadPageID(pid);
        flushMaster();
    }
    return pid;
}

ColumnPage ColumnFile::loadPage(uint16_t pageID) {
    off_t offset = static_cast<off_t>(pageID) * pageSize_;
    if (lseek(fd_, offset, SEEK_SET) == (off_t)-1) {
        perror("lseek(loadPage)");
    }

    // Read header (4 x uint16_t)
    uint16_t hdr[4];
    ssize_t n = read(fd_, hdr, sizeof(hdr));
    if (n != (ssize_t)sizeof(hdr)) {
        perror("read(page header)");
    }

    uint16_t pid          = hdr[0];
    uint16_t capacity     = hdr[1];
    uint16_t count        = hdr[2];
    uint16_t nextFreePage = hdr[3];

    // If this looks uninitialized (all zeros), initialize logically in memory.
    if (pid == 0 && capacity == 0) {
        capacity = computeCapacity(pageSize_);
        pid      = pageID;
        count    = 0;
        nextFreePage = UINT16_MAX;
    }

    ColumnPage page(pid, capacity);
    page.count        = count;
    page.nextFreePage = nextFreePage;

    // Read values array
    size_t valuesBytes = static_cast<size_t>(capacity) * VALUE_SIZE;
    if (valuesBytes) {
        n = read(fd_, page.values.data(), valuesBytes);
        if (n != (ssize_t)valuesBytes) {
            perror("read(values)");
        }
    }

    // Read tombstones
    if (capacity) {
        std::vector<uint8_t> bits(capacity);
        n = read(fd_, bits.data(), capacity);
        if (n != (ssize_t)capacity) {
            perror("read(tombstones)");
        }
        for (uint16_t i = 0; i < capacity; ++i) {
            page.tombstone[i] = (bits[i] != 0);
        }
    }

    return page;
}

void ColumnFile::flushPage(const ColumnPage &page) {
    off_t offset = static_cast<off_t>(page.pageID) * pageSize_;
    if (lseek(fd_, offset, SEEK_SET) == (off_t)-1) {
        perror("lseek(flushPage)");
    }

    // Write header
    uint16_t hdr[4] = {
        page.pageID,
        page.capacity,
        page.count,
        page.nextFreePage
    };
    ssize_t n = write(fd_, hdr, sizeof(hdr));
    if (n != (ssize_t)sizeof(hdr)) {
        perror("write(page header)");
    }

    // Write values
    size_t valuesBytes = static_cast<size_t>(page.capacity) * VALUE_SIZE;
    if (valuesBytes) {
        n = write(fd_, page.values.data(), valuesBytes);
        if (n != (ssize_t)valuesBytes) {
            perror("write(values)");
        }
    }

    // Write tombstones (1 byte per slot)
    if (page.capacity) {
        std::vector<uint8_t> bits(page.capacity);
        for (uint16_t i = 0; i < page.capacity; ++i) {
            bits[i] = page.tombstone[i] ? 1 : 0;
        }
        n = write(fd_, bits.data(), page.capacity);
        if (n != (ssize_t)page.capacity) {
            perror("write(tombstones)");
        }
    }

    if (fsync(fd_) == -1) {
        perror("fsync(flushPage)");
    }
}

uint32_t ColumnFile::allocSlot(ValueType val) {
    // Get a page with free slots (init on demand)
    uint16_t pid = allocateOrFetchPage();

    // Load and allocate in-page
    ColumnPage page = loadPage(pid);
    int16_t slot = page.findFreeSlot();
    assert(slot >= 0);
    page.writeValue(slot, val);
    page.markUsed(slot);

    // If page is now full, I remove the head (no more free slots there)
    if (page.count == page.capacity) {
        setHeadPageID(UINT16_MAX);
        flushMaster();
    } else {
        // keep pid as head; for multi-page free lists I would link via nextFreePage
    }

    flushPage(page);

    return (uint32_t(pid) << 16) | uint32_t(slot);
}

std::optional<ValueType> ColumnFile::fetchSlot(uint32_t id) {
    uint16_t pid  = pageIdFromSlotId(id);
    uint16_t slot = slotIdxFromSlotId(id);
    ColumnPage page = loadPage(pid);
    if (slot >= page.capacity) return std::nullopt;
    if (!page.tombstone[slot]) return std::nullopt;
    return page.readValue(slot);
}

void ColumnFile::deleteSlot(uint32_t id) {
    uint16_t pid  = pageIdFromSlotId(id);
    uint16_t slot = slotIdxFromSlotId(id);
    ColumnPage page = loadPage(pid);
    if (slot >= page.capacity) return;

    bool wasFull = (page.count == page.capacity);
    if (page.tombstone[slot]) {
        page.markDeleted(slot);
    }

    // If it was full and now has space, I re-expose it by setting head to this page
    if (wasFull) {
        setHeadPageID(pid);
        flushMaster();
    }

    flushPage(page);
}

void ColumnFile::flushMaster() {
    mp_.flush(fd_);
}
