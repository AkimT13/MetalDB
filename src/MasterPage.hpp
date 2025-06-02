#pragma once

#include <stdio.h>
#include <cstdint>
#include <vector>
#include <unistd.h>

struct MasterPage {

    uint32_t magic;

    uint32_t ColSize;
    uint16_t pageSize;
    uint32_t numCols;
    uint32_t special;
    std::vector<uint16_t> headPageIDS;
    static MasterPage load(int fd);
    static MasterPage initnew(int fd, int pageSize, int numColoumns);
    
    void flush(int fd) const;




    






};






