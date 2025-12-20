// MasterPage.cpp
#include "MasterPage.hpp"
#include <unistd.h>   // lseek, read, write, ftruncate, fsync
#include <cstdio>
#include <cerrno>
#include <cstring>

MasterPage MasterPage::initnew(int fd, uint16_t pageSize, int numColumns) {
    // Truncate file to exactly one page
    if (ftruncate(fd, pageSize) == -1) {
        perror("ftruncate");
    }

    MasterPage mp;
    mp.magic      = 0x4D445042;              
    mp.pageSize   = pageSize;
    mp.numColumns = numColumns;
    mp.headPageIDs.assign(numColumns, UINT16_MAX);

    // Write fixed fields + vector data to offset 0
    lseek(fd, 0, SEEK_SET);
    write(fd, &mp.magic,      sizeof(mp.magic));
    write(fd, &mp.pageSize,   sizeof(mp.pageSize));
    write(fd, &mp.numColumns, sizeof(mp.numColumns));
    write(fd, mp.headPageIDs.data(),
          numColumns * sizeof(uint16_t));
    fsync(fd);

    return mp;
}

void MasterPage::flush(int fd) const {
    off_t ok = lseek(fd, 0, SEEK_SET);
    if (ok == (off_t)-1) {
        std::perror("MasterPage::flush lseek");
        return;
    }

    if (write(fd, &magic, sizeof(magic)) != ssize_t(sizeof(magic))) {
        std::perror("MasterPage::flush write(magic)");
        return;
    }
    if (write(fd, &pageSize, sizeof(pageSize)) != ssize_t(sizeof(pageSize))) {
        std::perror("MasterPage::flush write(pageSize)");
        return;
    }
    if (write(fd, &numColumns, sizeof(numColumns)) != ssize_t(sizeof(numColumns))) {
        std::perror("MasterPage::flush write(numColumns)");
        return;
    }
    if (!headPageIDs.empty()) {
        const size_t bytes = headPageIDs.size() * sizeof(uint16_t);
        if (write(fd, headPageIDs.data(), bytes) != ssize_t(bytes)) {
            std::perror("MasterPage::flush write(headPageIDs)");
            return;
        }
    }
    fsync(fd);
}
MasterPage MasterPage::load(int fd) {
    MasterPage mp;

    lseek(fd, 0, SEEK_SET);
    read(fd, &mp.magic,      sizeof(mp.magic));
    read(fd, &mp.pageSize,   sizeof(mp.pageSize));
    read(fd, &mp.numColumns, sizeof(mp.numColumns));

    mp.headPageIDs.resize(mp.numColumns);
    read(fd, mp.headPageIDs.data(),
         mp.numColumns * sizeof(uint16_t));

    return mp;
}


