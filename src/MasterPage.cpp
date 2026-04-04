// MasterPage.cpp
#include "MasterPage.hpp"
#include <unistd.h>
#include <cstdio>
#include <cerrno>
#include <cstring>

// On-disk layout of page 0:
//   uint32_t magic
//   uint16_t pageSize
//   uint16_t numColumns
//   uint16_t headPageIDs[numColumns]
//   uint8_t  colTypes[numColumns]        (ColType enum, 1 byte each)

static void writeAll(int fd, const void* buf, size_t n) {
    if (write(fd, buf, n) != ssize_t(n)) std::perror("MasterPage write");
}

MasterPage MasterPage::initnew(int fd, uint16_t pageSize, int numColumns) {
    std::vector<ColType> types(numColumns, ColType::UINT32);
    return initnew(fd, pageSize, types);
}

MasterPage MasterPage::initnew(int fd, uint16_t pageSize,
                               const std::vector<ColType>& types) {
    const int numColumns = static_cast<int>(types.size());
    if (ftruncate(fd, pageSize) == -1) std::perror("ftruncate");

    MasterPage mp;
    mp.magic      = 0x4D445042;
    mp.pageSize   = pageSize;
    mp.numColumns = static_cast<uint16_t>(numColumns);
    mp.headPageIDs.assign(numColumns, UINT16_MAX);
    mp.colTypes   = types;

    lseek(fd, 0, SEEK_SET);
    writeAll(fd, &mp.magic,      sizeof(mp.magic));
    writeAll(fd, &mp.pageSize,   sizeof(mp.pageSize));
    writeAll(fd, &mp.numColumns, sizeof(mp.numColumns));
    writeAll(fd, mp.headPageIDs.data(), numColumns * sizeof(uint16_t));
    // Write colTypes as raw uint8_t bytes
    for (auto t : mp.colTypes) {
        uint8_t b = static_cast<uint8_t>(t);
        writeAll(fd, &b, 1);
    }
    fsync(fd);
    return mp;
}

void MasterPage::flush(int fd) const {
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        std::perror("MasterPage::flush lseek"); return;
    }
    writeAll(fd, &magic,      sizeof(magic));
    writeAll(fd, &pageSize,   sizeof(pageSize));
    writeAll(fd, &numColumns, sizeof(numColumns));
    if (!headPageIDs.empty())
        writeAll(fd, headPageIDs.data(), headPageIDs.size() * sizeof(uint16_t));
    for (auto t : colTypes) {
        uint8_t b = static_cast<uint8_t>(t);
        writeAll(fd, &b, 1);
    }
    fsync(fd);
}

MasterPage MasterPage::load(int fd) {
    MasterPage mp;
    lseek(fd, 0, SEEK_SET);
    if (read(fd, &mp.magic,      sizeof(mp.magic))      != sizeof(mp.magic))      return mp;
    if (read(fd, &mp.pageSize,   sizeof(mp.pageSize))   != sizeof(mp.pageSize))   return mp;
    if (read(fd, &mp.numColumns, sizeof(mp.numColumns)) != sizeof(mp.numColumns)) return mp;

    mp.headPageIDs.resize(mp.numColumns);
    if (read(fd, mp.headPageIDs.data(),
             mp.numColumns * sizeof(uint16_t)) != ssize_t(mp.numColumns * sizeof(uint16_t))) {
        // Old format without colTypes - default all to UINT32
        mp.colTypes.assign(mp.numColumns, ColType::UINT32);
        return mp;
    }

    // Read colTypes (may not exist in very old files - fall back to UINT32)
    mp.colTypes.resize(mp.numColumns, ColType::UINT32);
    for (int i = 0; i < mp.numColumns; ++i) {
        uint8_t b = 0;
        if (read(fd, &b, 1) == 1)
            mp.colTypes[i] = static_cast<ColType>(b);
    }
    return mp;
}
