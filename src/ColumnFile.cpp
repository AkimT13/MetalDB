// ColumnFile.cpp
#include "ColumnFile.hpp"
#include "ValueTypes.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <sys/stat.h>
#include <cstdio>
#include <vector>
#include <cstring>
#include <limits>

// On-disk layout (little-endian):
//   [0..1]   uint16_t pageID
//   [2..3]   uint16_t capacity
//   [4..5]   uint16_t count
//   [6..7]   uint16_t nextFreePage
//   [8..11]  uint32_t minValue
//   [12..15] uint32_t maxValue
//   [16 .. 16+cap*sizeof(ValueType)-1]          values[]
//   [16+cap*sizeof(ValueType) .. +cap-1]        tombstone bytes (cap bytes; 0=free, 1=used)

#pragma pack(push, 1)
struct DiskPageHeader {
    uint16_t pageID;
    uint16_t capacity;
    uint16_t count;
    uint16_t nextFreePage; // UINT16_MAX = none
    uint32_t minValue;     // zone map min
    uint32_t maxValue;     // zone map max
};
#pragma pack(pop)
static_assert(sizeof(DiskPageHeader) == 16, "DiskPageHeader must be 16 bytes");

// How many slots fit in a page: header + cap*(value bytes + 1 tombstone byte)
static uint16_t computeCapacity(uint16_t pageSize) {
    if (pageSize < sizeof(DiskPageHeader)) return 0;
    const uint32_t usable = pageSize - uint32_t(sizeof(DiskPageHeader));
    const uint32_t per    = uint32_t(sizeof(ValueType)) + 1u;
    const uint32_t cap    = per ? (usable / per) : 0u;
    return static_cast<uint16_t>(cap > 0xFFFF ? 0xFFFF : cap);
}

uint16_t ColumnFile::pageCount() const {
    struct stat st{};
    if (fstat(fd_, &st) != 0) return 0;
    if (st.st_size <= 0) return 0;
    return static_cast<uint16_t>(st.st_size / pageSize_);
}

std::pair<ValueType, ValueType> ColumnFile::zoneMap(uint16_t pageID) const {
    DiskPageHeader hdr{};
    const off_t base = off_t(pageID) * off_t(pageSize_);
    ssize_t n = pread(fd_, &hdr, sizeof(hdr), base);
    if (n != ssize_t(sizeof(hdr))) {
        // Return an “empty/invalid” range that won’t pass overlap tests
        return { std::numeric_limits<ValueType>::max(),
                 std::numeric_limits<ValueType>::min() };
    }
    return { static_cast<ValueType>(hdr.minValue), static_cast<ValueType>(hdr.maxValue) };
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

        // Build an empty page and flush to disk
        const uint16_t cap = computeCapacity(pageSize_);
        ColumnPage page(pid, cap); // count=0, tombstone=false
        page.nextFreePage = UINT16_MAX;
        // min/max will be recomputed inside flushPage
        flushPage(page);

        // Set head to this new page
        setHeadPageID(pid);
        flushMaster();
    }
    return pid;
}

ColumnPage ColumnFile::loadPage(uint16_t pageID) {
    const off_t base = off_t(pageID) * off_t(pageSize_);

    DiskPageHeader hdr{};
    ssize_t n = pread(fd_, &hdr, sizeof(hdr), base);
    if (n != ssize_t(sizeof(hdr))) {
        std::perror("ColumnFile::loadPage pread(header)");
        const uint16_t cap = computeCapacity(pageSize_);
        ColumnPage page(pageID, cap);
        page.count = 0;
        page.nextFreePage = UINT16_MAX;
        // zone map stays as sentinel (empty)
        return page;
    }

    const uint16_t maxCap = computeCapacity(pageSize_);
    const uint16_t cap    = (hdr.capacity > maxCap) ? maxCap : hdr.capacity;

    ColumnPage page(pageID, cap);
    page.count        = (hdr.count > cap) ? cap : hdr.count;
    page.nextFreePage = hdr.nextFreePage;

    // Read values
    const size_t valuesBytes = size_t(cap) * sizeof(ValueType);
    const off_t  valuesOff   = base + sizeof(DiskPageHeader);
    if (valuesBytes) {
        n = pread(fd_, page.values.data(), valuesBytes, valuesOff);
        if (n != ssize_t(valuesBytes)) {
            std::perror("ColumnFile::loadPage pread(values)");
        }
    }

    // Read tombstones via a temporary byte buffer (vector<bool> is packed)
    const size_t tombBytes = size_t(cap);
    const off_t  tombOff   = valuesOff + off_t(valuesBytes);
    if (tombBytes) {
        std::vector<uint8_t> tmp(tombBytes, 0);
        n = pread(fd_, tmp.data(), tombBytes, tombOff);
        if (n != ssize_t(tombBytes)) {
            std::perror("ColumnFile::loadPage pread(tombstone)");
        }
        for (size_t i = 0; i < cap; ++i) {
            page.tombstone[i] = (tmp[i] != 0);
        }
    }

    // Zone map from header; if obviously invalid, recompute defensively
    page.minValue = static_cast<ValueType>(hdr.minValue);
    page.maxValue = static_cast<ValueType>(hdr.maxValue);
    if (page.count == 0 || page.minValue > page.maxValue) {
        page.recomputeMinMax();
    }

    // Optional safety: clamp count
    if (page.count > cap) page.count = cap;

    return page;
}

void ColumnFile::flushPage(const ColumnPage &page) {
    const off_t base = off_t(page.pageID) * off_t(pageSize_);

    // Recompute min/max on a local copy so I don't mutate caller
    ColumnPage copy = page;
    copy.recomputeMinMax();

    DiskPageHeader hdr{};
    hdr.pageID       = copy.pageID;
    hdr.capacity     = copy.capacity;
    hdr.count        = copy.count;
    hdr.nextFreePage = copy.nextFreePage;
    hdr.minValue     = static_cast<uint32_t>(copy.minValue);
    hdr.maxValue     = static_cast<uint32_t>(copy.maxValue);

    ssize_t n = pwrite(fd_, &hdr, sizeof(hdr), base);
    if (n != ssize_t(sizeof(hdr))) {
        std::perror("ColumnFile::flushPage pwrite(header)");
        return;
    }

    // Write values
    const size_t valuesBytes = size_t(copy.capacity) * sizeof(ValueType);
    const off_t  valuesOff   = base + sizeof(DiskPageHeader);
    if (valuesBytes) {
        n = pwrite(fd_, copy.values.data(), valuesBytes, valuesOff);
        if (n != ssize_t(valuesBytes)) {
            std::perror("ColumnFile::flushPage pwrite(values)");
            return;
        }
    }

    // Write tombstones via a temporary byte buffer (vector<bool> is packed)
    const size_t tombBytes = size_t(copy.capacity);
    const off_t  tombOff   = valuesOff + off_t(valuesBytes);
    if (tombBytes) {
        std::vector<uint8_t> tmp(tombBytes, 0);
        for (size_t i = 0; i < copy.capacity; ++i) {
            tmp[i] = copy.tombstone[i] ? 1u : 0u;
        }
        n = pwrite(fd_, tmp.data(), tombBytes, tombOff);
        if (n != ssize_t(tombBytes)) {
            std::perror("ColumnFile::flushPage pwrite(tombstone)");
            return;
        }
    }

    fsync(fd_); // tests want determinism; later we can batch
}

uint32_t ColumnFile::allocSlot(ValueType val) {
    const uint16_t pid = allocateOrFetchPage();

    ColumnPage page = loadPage(pid);
    int16_t slot = page.findFreeSlot();
    assert(slot >= 0);
    page.writeValue(slot, val);
    page.markUsed(slot);

    if (page.count == page.capacity) {
        setHeadPageID(UINT16_MAX);
        flushMaster();
    }
    flushPage(page);

    return (uint32_t(pid) << 16) | uint32_t(slot);
}

std::optional<ValueType> ColumnFile::fetchSlot(uint32_t id) {
    const uint16_t pid  = pageIdFromSlotId(id);
    const uint16_t slot = slotIdxFromSlotId(id);
    ColumnPage page = loadPage(pid);
    if (slot >= page.capacity) return std::nullopt;
    if (!page.tombstone[slot]) return std::nullopt;
    return page.readValue(slot);
}

void ColumnFile::deleteSlot(uint32_t id) {
    const uint16_t pid  = pageIdFromSlotId(id);
    const uint16_t slot = slotIdxFromSlotId(id);
    ColumnPage page = loadPage(pid);
    if (slot >= page.capacity) return;

    const bool wasFull = (page.count == page.capacity);
    if (page.tombstone[slot]) {
        page.markDeleted(slot);
    }

    if (wasFull) {
        setHeadPageID(pid);
        flushMaster();
    }

    flushPage(page);
}

void ColumnFile::flushMaster() {
    mp_.flush(fd_);
}