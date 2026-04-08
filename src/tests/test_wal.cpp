#include "../Table.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

#pragma pack(push, 1)
struct TestRecordHeader {
    uint32_t payloadSize;
    uint8_t  type;
    uint8_t  reserved[3];
    uint64_t opID;
    uint32_t checksum;
};
#pragma pack(pop)

off_t fileSize(const std::string& path) {
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0) return -1;
    return st.st_size;
}

void appendBytes(const std::string& path, const void* buf, size_t n) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
    assert(fd >= 0);
    assert(::write(fd, buf, n) == static_cast<ssize_t>(n));
    ::close(fd);
}

void cleanup(const std::string& base, bool stringCol) {
    std::remove((base + ".mdb").c_str());
    std::remove((base + ".mdb.idx").c_str());
    std::remove((base + ".mdb.wal").c_str());
    if (stringCol) std::remove((base + ".mdb.1.str").c_str());
    std::remove((base + ".mdb.2.str").c_str());
}

} // namespace

int main() {
    {
        const std::string base = "/tmp/wal_insert_recover";
        cleanup(base, true);
        {
            Table t(base + ".mdb", 4096, std::vector<ColType>{ColType::UINT32, ColType::STRING});
            const uint32_t rid = t.insertTypedRow({ColValue(uint32_t(7)), ColValue(std::string("alpha"))});
            assert(rid == 0);
        }
        {
            Table t(base + ".mdb");
            auto row = t.fetchTypedRow(0);
            assert(row.size() == 2);
            assert(row[0] && row[0]->u32 == 7);
            assert(row[1] && row[1]->str == "alpha");
            assert(fileSize(base + ".mdb.wal") == 8);
        }
        cleanup(base, true);
    }

    {
        const std::string base = "/tmp/wal_delete_recover";
        cleanup(base, false);
        {
            Table t(base + ".mdb", 4096, std::vector<ColType>{ColType::UINT32});
            t.insertTypedRow({ColValue(uint32_t(42))});
            t.flushDurable();
            t.deleteRow(0);
        }
        {
            Table t(base + ".mdb");
            auto row = t.fetchTypedRow(0);
            assert(row.size() == 1);
            assert(!row[0]);
            assert(fileSize(base + ".mdb.wal") == 8);
        }
        cleanup(base, false);
    }

    {
        const std::string base = "/tmp/wal_typed_flush";
        cleanup(base, true);
        {
            Table t(base + ".mdb", 4096, std::vector<ColType>{ColType::INT64, ColType::DOUBLE, ColType::STRING});
            t.insertTypedRow({ColValue(int64_t(-1234)), ColValue(9.25), ColValue(std::string("persisted"))});
            t.flushDurable();
            assert(fileSize(base + ".mdb.wal") == 8);
        }
        {
            Table t(base + ".mdb");
            auto row = t.fetchTypedRow(0);
            assert(row[0] && row[0]->i64 == -1234);
            assert(row[1] && row[1]->f64 == 9.25);
            assert(row[2] && row[2]->str == "persisted");
        }
        cleanup(base, true);
    }

    {
        const std::string base = "/tmp/wal_partial_tail";
        cleanup(base, false);
        {
            Table t(base + ".mdb", 4096, std::vector<ColType>{ColType::UINT32});
            t.insertTypedRow({ColValue(uint32_t(11))});
            t.flushDurable();
        }
        const char junk[] = {'b', 'a', 'd', '!'};
        appendBytes(base + ".mdb.wal", junk, sizeof(junk));
        {
            Table t(base + ".mdb");
            auto row = t.fetchTypedRow(0);
            assert(row[0] && row[0]->u32 == 11);
            assert(fileSize(base + ".mdb.wal") == 8);
        }
        cleanup(base, false);
    }

    {
        const std::string base = "/tmp/wal_bad_checksum";
        cleanup(base, false);
        {
            Table t(base + ".mdb", 4096, std::vector<ColType>{ColType::UINT32});
            t.insertTypedRow({ColValue(uint32_t(55))});
        }
        const TestRecordHeader bad{4, 2, {0, 0, 0}, 999, 0};
        const uint32_t rowID = 0;
        appendBytes(base + ".mdb.wal", &bad, sizeof(bad));
        appendBytes(base + ".mdb.wal", &rowID, sizeof(rowID));
        {
            Table t(base + ".mdb");
            auto row = t.fetchTypedRow(0);
            assert(row[0] && row[0]->u32 == 55);
            assert(fileSize(base + ".mdb.wal") == 8);
        }
        cleanup(base, false);
    }

    std::puts("test_wal: passed");
    return 0;
}
