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
//   [8..11]  uint32_t minValue (low 32 bits of zone-map min, legacy compat)
//   [12..15] uint32_t maxValue (low 32 bits of zone-map max, legacy compat)
//   [16 .. 16 + cap*valueBytes - 1]                values[] (typed)
//   [16 + cap*valueBytes .. + cap - 1]              tombstone bytes (1 per slot)
//
// valueBytes is derived from the column's ColType stored in MasterPage.

#pragma pack(push, 1)
struct DiskPageHeader {
    uint16_t pageID;
    uint16_t capacity;
    uint16_t count;
    uint16_t nextFreePage;
    uint32_t minValue;
    uint32_t maxValue;
};
#pragma pack(pop)
static_assert(sizeof(DiskPageHeader) == 16, "DiskPageHeader must be 16 bytes");

static uint16_t computeCapacity(uint16_t pageSize, uint16_t vbytes) {
    if (pageSize < sizeof(DiskPageHeader)) return 0;
    const uint32_t usable = pageSize - uint32_t(sizeof(DiskPageHeader));
    const uint32_t per    = uint32_t(vbytes) + 1u; // value bytes + 1 tombstone byte
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
    if (pread(fd_, &hdr, sizeof(hdr), base) != ssize_t(sizeof(hdr))) {
        return { std::numeric_limits<ValueType>::max(),
                 std::numeric_limits<ValueType>::min() };
    }
    return { static_cast<ValueType>(hdr.minValue), static_cast<ValueType>(hdr.maxValue) };
}

ColumnFile::ColumnFile(const std::string &path, MasterPage &mp, uint16_t colIdx)
  : fd_(-1), mp_(mp), colIdx_(colIdx), pageSize_(mp.pageSize)
{
    // Determine column type from MasterPage (defaults UINT32 for old files)
    if (colIdx < mp.colTypes.size())
        colType_ = mp.colTypes[colIdx];
    else
        colType_ = ColType::UINT32;

    valueBytes_ = colValueBytes(colType_);

    fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0666);
    assert(fd_ >= 0);
}

ColumnFile::~ColumnFile() {
    if (fd_ >= 0) close(fd_);
}

uint16_t ColumnFile::allocateOrFetchPage() {
    uint16_t pid = headPageID();
    if (pid == UINT16_MAX) {
        off_t end = lseek(fd_, 0, SEEK_END);
        assert(end >= 0);
        pid = static_cast<uint16_t>(end / pageSize_);

        if (ftruncate(fd_, end + pageSize_) == -1) perror("ftruncate");

        const uint16_t cap = computeCapacity(pageSize_, valueBytes_);
        ColumnPage page(pid, cap, valueBytes_);
        page.nextFreePage = UINT16_MAX;
        flushPage(page);

        setHeadPageID(pid);
        flushMaster();
    }
    return pid;
}

ColumnPage ColumnFile::loadPage(uint16_t pageID) {
    const off_t base = off_t(pageID) * off_t(pageSize_);

    DiskPageHeader hdr{};
    if (pread(fd_, &hdr, sizeof(hdr), base) != ssize_t(sizeof(hdr))) {
        std::perror("ColumnFile::loadPage pread(header)");
        const uint16_t cap = computeCapacity(pageSize_, valueBytes_);
        ColumnPage page(pageID, cap, valueBytes_);
        page.count = 0;
        page.nextFreePage = UINT16_MAX;
        return page;
    }

    const uint16_t maxCap = computeCapacity(pageSize_, valueBytes_);
    const uint16_t cap    = (hdr.capacity > maxCap) ? maxCap : hdr.capacity;

    ColumnPage page(pageID, cap, valueBytes_);
    page.count        = (hdr.count > cap) ? cap : hdr.count;
    page.nextFreePage = hdr.nextFreePage;

    // Read raw values
    const size_t valuesBytes = size_t(cap) * valueBytes_;
    const off_t  valuesOff   = base + sizeof(DiskPageHeader);
    if (valuesBytes) {
        if (pread(fd_, page.rawValues.data(), valuesBytes, valuesOff)
                != ssize_t(valuesBytes))
            std::perror("ColumnFile::loadPage pread(values)");
    }

    // Read tombstones
    const size_t tombBytes = size_t(cap);
    const off_t  tombOff   = valuesOff + off_t(valuesBytes);
    if (tombBytes) {
        std::vector<uint8_t> tmp(tombBytes, 0);
        if (pread(fd_, tmp.data(), tombBytes, tombOff) != ssize_t(tombBytes))
            std::perror("ColumnFile::loadPage pread(tombstone)");
        for (size_t i = 0; i < cap; ++i)
            page.tombstone[i] = (tmp[i] != 0);
    }

    // Zone map (legacy 32-bit)
    page.minValue = static_cast<ValueType>(hdr.minValue);
    page.maxValue = static_cast<ValueType>(hdr.maxValue);
    page.minValue64 = static_cast<int64_t>(hdr.minValue);
    page.maxValue64 = static_cast<int64_t>(hdr.maxValue);

    if (page.count == 0 || page.minValue > page.maxValue)
        page.recomputeMinMax();

    return page;
}

void ColumnFile::flushPage(const ColumnPage &page) {
    const off_t base = off_t(page.pageID) * off_t(pageSize_);

    ColumnPage copy = page;
    copy.recomputeMinMax();

    DiskPageHeader hdr{};
    hdr.pageID       = copy.pageID;
    hdr.capacity     = copy.capacity;
    hdr.count        = copy.count;
    hdr.nextFreePage = copy.nextFreePage;
    hdr.minValue     = static_cast<uint32_t>(copy.minValue);
    hdr.maxValue     = static_cast<uint32_t>(copy.maxValue);

    if (pwrite(fd_, &hdr, sizeof(hdr), base) != ssize_t(sizeof(hdr))) {
        std::perror("ColumnFile::flushPage pwrite(header)"); return;
    }

    const size_t valuesBytes = size_t(copy.capacity) * valueBytes_;
    const off_t  valuesOff   = base + sizeof(DiskPageHeader);
    if (valuesBytes) {
        if (pwrite(fd_, copy.rawValues.data(), valuesBytes, valuesOff)
                != ssize_t(valuesBytes)) {
            std::perror("ColumnFile::flushPage pwrite(values)"); return;
        }
    }

    const size_t tombBytes = size_t(copy.capacity);
    const off_t  tombOff   = valuesOff + off_t(valuesBytes);
    if (tombBytes) {
        std::vector<uint8_t> tmp(tombBytes, 0);
        for (size_t i = 0; i < copy.capacity; ++i)
            tmp[i] = copy.tombstone[i] ? 1u : 0u;
        if (pwrite(fd_, tmp.data(), tombBytes, tombOff) != ssize_t(tombBytes))
            std::perror("ColumnFile::flushPage pwrite(tombstone)");
    }

    fsync(fd_);
}

// ── Legacy UINT32 API ────────────────────────────────────────────────────────

uint32_t ColumnFile::allocSlot(ValueType val) {
    ColValue cv(val);
    return allocTypedSlot(cv);
}

std::optional<ValueType> ColumnFile::fetchSlot(uint32_t id) {
    auto cv = fetchTypedSlot(id);
    if (!cv) return std::nullopt;
    return cv->asU32();
}

// ── Typed API ────────────────────────────────────────────────────────────────

uint32_t ColumnFile::allocTypedSlot(const ColValue& val) {
    const uint16_t pid = allocateOrFetchPage();

    ColumnPage page = loadPage(pid);
    int16_t slot = page.findFreeSlot();
    assert(slot >= 0);

    // Write the right number of bytes based on colType_
    switch (colType_) {
        case ColType::UINT32: { uint32_t v = val.asU32();        page.writeRaw(slot, &v, 4); break; }
        case ColType::INT64:  { int64_t  v = val.i64;            page.writeRaw(slot, &v, 8); break; }
        case ColType::FLOAT:  { float    v = val.f32;            page.writeRaw(slot, &v, 4); break; }
        case ColType::DOUBLE: { double   v = val.f64;            page.writeRaw(slot, &v, 8); break; }
    }
    page.markUsed(slot);

    if (page.count == page.capacity) {
        setHeadPageID(UINT16_MAX);
        flushMaster();
    }
    flushPage(page);
    return (uint32_t(pid) << 16) | uint32_t(slot);
}

std::optional<ColValue> ColumnFile::fetchTypedSlot(uint32_t id) const {
    const uint16_t pid  = pageIdFromSlotId(id);
    const uint16_t slot = slotIdxFromSlotId(id);
    // Cast away const to reuse loadPage (reads disk, doesn't mutate state)
    ColumnPage page = const_cast<ColumnFile*>(this)->loadPage(pid);
    if (slot >= page.capacity) return std::nullopt;
    if (!page.tombstone[slot]) return std::nullopt;

    switch (colType_) {
        case ColType::UINT32: { uint32_t v; page.readRaw(slot, &v, 4); return ColValue(v); }
        case ColType::INT64:  { int64_t  v; page.readRaw(slot, &v, 8); return ColValue(v); }
        case ColType::FLOAT:  { float    v; page.readRaw(slot, &v, 4); return ColValue(v); }
        case ColType::DOUBLE: { double   v; page.readRaw(slot, &v, 8); return ColValue(v); }
    }
    return std::nullopt;
}

void ColumnFile::deleteSlot(uint32_t id) {
    const uint16_t pid  = pageIdFromSlotId(id);
    const uint16_t slot = slotIdxFromSlotId(id);
    ColumnPage page = loadPage(pid);
    if (slot >= page.capacity) return;

    const bool wasFull = (page.count == page.capacity);
    if (page.tombstone[slot]) page.markDeleted(slot);

    if (wasFull) {
        setHeadPageID(pid);
        flushMaster();
    }
    flushPage(page);
}

void ColumnFile::flushMaster() {
    mp_.flush(fd_);
}
