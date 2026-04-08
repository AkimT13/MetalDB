#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ValueTypes.hpp"

class Wal {
public:
    struct Operation {
        enum class Kind : uint8_t {
            Insert = 1,
            Delete = 2,
        };

        Kind kind = Kind::Insert;
        uint64_t opID = 0;
        uint32_t rowID = 0;
        std::vector<ColValue> values;
    };

    explicit Wal(const std::string& tablePath);
    ~Wal();

    void openOrCreate(bool create);
    uint64_t appendInsert(uint32_t rowID, const std::vector<ColValue>& values);
    uint64_t appendDelete(uint32_t rowID);
    void appendCommit(uint64_t opID);

    std::vector<Operation> committedOperations() const;

    void sync() const;
    void truncate();
    bool hasEntries() const;
    std::string path() const { return path_; }

private:
    std::string path_;
    int fd_ = -1;
    uint64_t nextOpID_ = 1;

    void ensureHeader();
    void appendRecord(uint8_t type, uint64_t opID, const std::vector<uint8_t>& payload);
};
