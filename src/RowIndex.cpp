// RowIndex.cpp
#include "RowIndex.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cassert>
#include <cstdio>
#include <cstring>

static constexpr uint32_t RIDX_MAGIC = 0x52494458; // 'RIDX'

RowIndex::RowIndex(const std::string& pathBase, uint16_t numColumns)
  : idxPath_(pathBase + ".idx"), numColumns_(numColumns), fd_(-1) {}

void RowIndex::openOrCreate() {
    fd_ = open(idxPath_.c_str(), O_RDWR | O_CREAT, 0666);
    assert(fd_ >= 0);

    // Check if file is empty; if so, write header
    off_t end = lseek(fd_, 0, SEEK_END);
    if (end == 0) {
        ensureHeaderOnCreate();
    }
    // Load entries into memory
    loadAll();
}

void RowIndex::ensureHeaderOnCreate() {
    uint32_t magic = RIDX_MAGIC;
    uint16_t ncols = numColumns_;
    uint16_t zero  = 0;

    if (lseek(fd_, 0, SEEK_SET) == (off_t)-1) perror("lseek(header)");
    if (write(fd_, &magic, sizeof(magic)) != (ssize_t)sizeof(magic)) perror("write(magic)");
    if (write(fd_, &ncols, sizeof(ncols)) != (ssize_t)sizeof(ncols)) perror("write(numCols)");
    if (write(fd_, &zero,  sizeof(zero))  != (ssize_t)sizeof(zero))  perror("write(reserved)");
    if (fsync(fd_) == -1) perror("fsync(header)");
}

void RowIndex::loadAll() {
    entries_.clear();
    deletedCount_ = 0;

    // Read header
    if (lseek(fd_, 0, SEEK_SET) == (off_t)-1) perror("lseek(loadAll/0)");

    uint32_t magic = 0; uint16_t ncols = 0, reserved = 0;
    if (read(fd_, &magic, sizeof(magic)) != (ssize_t)sizeof(magic)) perror("read(magic)");
    if (read(fd_, &ncols, sizeof(ncols)) != (ssize_t)sizeof(ncols)) perror("read(ncols)");
    if (read(fd_, &reserved, sizeof(reserved)) != (ssize_t)sizeof(reserved)) perror("read(reserved)");

    if (magic != RIDX_MAGIC) {
        fprintf(stderr, "RowIndex: invalid magic\n");
        return;
    }
    if (ncols != numColumns_) {
        // For now, require exact match; could relax later
        fprintf(stderr, "RowIndex: numColumns mismatch (%u vs %u)\n", ncols, numColumns_);
        numColumns_ = ncols; // adopt file setting
    }

    // Read all entries
    const size_t entrySize = 1 + 3 + sizeof(uint32_t) * numColumns_;
    off_t pos = lseek(fd_, 0, SEEK_CUR);
    off_t fileEnd = lseek(fd_, 0, SEEK_END);
    for (; pos + (off_t)entrySize <= fileEnd; pos += entrySize) {
        if (lseek(fd_, pos, SEEK_SET) == (off_t)-1) perror("lseek(entry)");
        Entry e; e.status = 0; e.slots.resize(numColumns_);
        uint8_t pad[3];
        if (read(fd_, &e.status, 1) != 1) break;
        if (read(fd_, pad, 3) != 3) break;
        if (read(fd_, e.slots.data(), sizeof(uint32_t) * numColumns_) != (ssize_t)(sizeof(uint32_t) * numColumns_)) break;
        entries_.push_back(e);
        if (e.status == 0) ++deletedCount_;
    }
}

uint32_t RowIndex::appendRow(const std::vector<uint32_t>& slotIDs) {
    assert(slotIDs.size() == numColumns_);
    Entry e; e.status = 1; e.slots = slotIDs;

    uint32_t rowID = static_cast<uint32_t>(entries_.size());
    entries_.push_back(e);

    writeEntry(rowID, e);
    return rowID;
}

void RowIndex::forEachLive(const std::function<void(uint32_t, const std::vector<uint32_t>&)>& fn) const {
    for (uint32_t i = 0; i < entries_.size(); ++i) {
        const Entry& e = entries_[i];
        if (e.status == 1) fn(i, e.slots);
    }
}

void RowIndex::markDeleted(uint32_t rowID) {
    if (rowID >= entries_.size()) return;
    if (entries_[rowID].status == 0) return;
    entries_[rowID].status = 0;
    ++deletedCount_;
    writeEntry(rowID, entries_[rowID]);
}

void RowIndex::writeEntry(uint32_t rowID, const Entry& e) {
    const off_t base = 4 /*magic*/ + 2 /*ncols*/ + 2 /*res*/;
    const size_t entrySize = 1 + 3 + sizeof(uint32_t) * numColumns_;
    off_t pos = base + off_t(rowID) * off_t(entrySize);

    if (lseek(fd_, pos, SEEK_SET) == (off_t)-1) perror("lseek(writeEntry)");
    uint8_t pad[3] = {0,0,0};
    if (write(fd_, &e.status, 1) != 1) perror("write(status)");
    if (write(fd_, pad, 3) != 3) perror("write(pad)");
    if (write(fd_, e.slots.data(), sizeof(uint32_t) * numColumns_) != (ssize_t)(sizeof(uint32_t) * numColumns_)) perror("write(slots)");
    if (fsync(fd_) == -1) perror("fsync(entry)");
}

std::optional<std::vector<uint32_t>> RowIndex::fetch(uint32_t rowID) const {
    if (rowID >= entries_.size()) return std::nullopt;
    if (entries_[rowID].status == 0) return std::nullopt;
    return entries_[rowID].slots;
}
