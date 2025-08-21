#include "MasterPage.hpp"
#include <unistd.h>   // lseek, read, write, ftruncate, fsync
#include <cstddef>    // size_t
#include <cstdint>    // uint32_t, uint16_t
#include <cstdio>     // perror
#include <cstring>    // strerror
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

//
// initNew: create a fresh MasterPage in an empty/truncated file.
//
MasterPage MasterPage::initNew(int fd, uint16_t pageSize, uint16_t numColumns) {
    // 1) Truncate the file to exactly one page (page 0).
    //    If the file was larger, everything after page 0 is discarded.
    if (ftruncate(fd, pageSize) == -1) {
        perror("ftruncate");
        // In production code, you might throw or return an error indicator.
    }

    // 2) Fill in the struct in memory.
    MasterPage mp;
    mp.magic      = 0x4D445042;         
    mp.pageSize   = pageSize;
    mp.numColumns = numColumns;
    // Initialize each column's free‐page head to “none” = UINT16_MAX.
    mp.headPageIDs.assign(numColumns, UINT16_MAX);

    // 3) Seek to the start of the file and write out:
    //    [magic][pageSize][numColumns][headPageIDs array]
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        perror("lseek(initNew)");
    }

    // Write fixed‐size fields first
    if (write(fd, &mp.magic,      sizeof(mp.magic))      != sizeof(mp.magic)) {
        perror("write(magic)");
    }
    if (write(fd, &mp.pageSize,   sizeof(mp.pageSize))   != sizeof(mp.pageSize)) {
        perror("write(pageSize)");
    }
    if (write(fd, &mp.numColumns, sizeof(mp.numColumns)) != sizeof(mp.numColumns)) {
        perror("write(numColumns)");
    }

    // Now write the vector's raw data (numColumns entries)
    size_t vecBytes = mp.numColumns * sizeof(uint16_t);
    if (write(fd, mp.headPageIDs.data(), vecBytes) != (ssize_t)vecBytes) {
        perror("write(headPageIDs)");
    }

    // Force changes to disk (optional but recommended for durability)
    if (fsync(fd) == -1) {
        perror("fsync(initNew)");
    }

    return mp;
}

//
// load: read an existing MasterPage from page 0 of the file.
//
MasterPage MasterPage::load(int fd) {
    MasterPage mp;

    // 1) Seek to offset 0
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        perror("lseek(load)");
        // Depending on your error‐handling strategy, you might exit or throw here.
    }

    // 2) Read fixed‐size fields: magic, pageSize, numColumns
    ssize_t n;
    n = read(fd, &mp.magic, sizeof(mp.magic));
    if (n < 0) {
        perror("read(magic)");
    } else if (n != sizeof(mp.magic)) {
        fprintf(stderr, "Unexpected EOF reading magic\n");
    }

    n = read(fd, &mp.pageSize, sizeof(mp.pageSize));
    if (n < 0) {
        perror("read(pageSize)");
    } else if (n != sizeof(mp.pageSize)) {
        fprintf(stderr, "Unexpected EOF reading pageSize\n");
    }

    n = read(fd, &mp.numColumns, sizeof(mp.numColumns));
    if (n < 0) {
        perror("read(numColumns)");
    } else if (n != sizeof(mp.numColumns)) {
        fprintf(stderr, "Unexpected EOF reading numColumns\n");
    }

    // 3) Resize the vector to numColumns, then read its contents
    mp.headPageIDs.resize(mp.numColumns);
    size_t vecBytes = mp.numColumns * sizeof(uint16_t);
    n = read(fd, mp.headPageIDs.data(), vecBytes);
    if (n < 0) {
        perror("read(headPageIDs)");
    } else if (n != (ssize_t)vecBytes) {
        fprintf(stderr, "Unexpected EOF reading headPageIDs\n");
    }

    // 4) (Optional) sanity‐check the magic number
    if (mp.magic != 0x4D445042) {
        fprintf(stderr, "MasterPage.load: invalid magic = 0x%08X\n", mp.magic);
        // You might choose to exit or throw here.
    }

    return mp;
}

//
// flush: overwrite page 0 with the current in-memory MasterPage
//
void MasterPage::flush(int fd) const {
    // 1) Seek back to offset 0
    if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
        perror("lseek(flush)");
    }

    // 2) Write fixed‐size fields
    if (write(fd, &magic,      sizeof(magic))      != sizeof(magic)) {
        perror("write(magic) in flush");
    }
    if (write(fd, &pageSize,   sizeof(pageSize))   != sizeof(pageSize)) {
        perror("write(pageSize) in flush");
    }
    if (write(fd, &numColumns, sizeof(numColumns)) != sizeof(numColumns)) {
        perror("write(numColumns) in flush");
    }

    // 3) Write the vector data
    size_t vecBytes = headPageIDs.size() * sizeof(uint16_t);
    if (write(fd, headPageIDs.data(), vecBytes) != (ssize_t)vecBytes) {
        perror("write(headPageIDs) in flush");
    }

    // 4) Sync to disk
    if (fsync(fd) == -1) {
        perror("fsync(flush)");
    }
}
