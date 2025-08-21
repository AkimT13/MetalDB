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

void MasterPage::flush(int fd) const {
    lseek(fd, 0, SEEK_SET);
    write(fd, &magic,      sizeof(magic));
    write(fd, &pageSize,   sizeof(pageSize));
    write(fd, &numColumns, sizeof(numColumns));
    write(fd, headPageIDs.data(),
          headPageIDs.size() * sizeof(uint16_t));
    fsync(fd);
}
