#include "MasterPage.hpp"
#include <unistd.h>
#include <stdio.h>




MasterPage MasterPage::load(int fd){
    off_t seekRes = lseek(fd,0,SEEK_SET);
    if(seekRes == -1){
        printf("Error with lseek inside MasterPage");
        
    }

    MasterPage mp;
    ssize_t n = read(fd, &mp, sizeof(mp));
    if(n!=sizeof(mp)){
        printf("Error reading MasterPage from file");
    }

    return mp;

    




}

MasterPage MasterPage::initnew(int fd, int pageSize, int numCols){
    ftruncate(fd, pageSize);

    MasterPage mp;
    mp.magic = 0x4D445042;
    mp.pageSize = pageSize;
    mp.numCols = numCols;
    mp.headPageIDS.assign(numCols, UINT16_MAX); // no free pages yet

    // write to disk

    lseek(fd,0,SEEK_SET);
    write(fd, &mp.magic, sizeof(mp.magic) );
    write(fd, &mp.pageSize,sizeof(mp.pageSize));
    write(fd, &mp.numCols,sizeof(mp.numCols));
    write(fd, mp.headPageIDS.data(),numCols * sizeof(uint16_t));
    fsync(fd);
    return mp;

     
}

MasterPage MasterPage::load(int fd){
    MasterPage mp;
    lseek(fd,0,SEEK_SET);
    read(fd,&mp.magic,sizeof(mp.magic));
    read(fd,&mp.pageSize,sizeof(mp.pageSize));
    read(fd, &mp.numCols,sizeof(numCols));
    mp.headPageIDS.resize(mp.numCols);
    read(fd,&mp.headPageIDS,mp.numCols *sizeof(uint16_t));
    return mp;

}

void MasterPage::flush(int fd) const {
    off_t seekRes = lseek(fd,0,SEEK_SET);
    

    



}

