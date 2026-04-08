#include "Wal.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>

namespace {

static constexpr uint32_t WAL_MAGIC   = 0x4D57414C; // MWAL
static constexpr uint16_t WAL_VERSION = 1;

enum class RecordType : uint8_t {
    Insert = 1,
    Delete = 2,
    Commit = 3,
};

#pragma pack(push, 1)
struct WalHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
};

struct RecordHeader {
    uint32_t payloadSize;
    uint8_t  type;
    uint8_t  reserved[3];
    uint64_t opID;
    uint32_t checksum;
};
#pragma pack(pop)

static_assert(sizeof(WalHeader) == 8, "WalHeader must be 8 bytes");
static_assert(sizeof(RecordHeader) == 20, "RecordHeader must be 20 bytes");

uint32_t fnv1a32(const uint8_t* data, size_t n) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < n; ++i) {
        hash ^= data[i];
        hash *= 16777619u;
    }
    return hash;
}

void appendBytes(std::vector<uint8_t>& out, const void* data, size_t n) {
    const auto* p = static_cast<const uint8_t*>(data);
    out.insert(out.end(), p, p + n);
}

template <typename T>
void appendScalar(std::vector<uint8_t>& out, const T& value) {
    appendBytes(out, &value, sizeof(T));
}

template <typename T>
T readScalar(const std::vector<uint8_t>& in, size_t& pos) {
    if (pos + sizeof(T) > in.size())
        throw std::runtime_error("short WAL payload");
    T value{};
    std::memcpy(&value, in.data() + pos, sizeof(T));
    pos += sizeof(T);
    return value;
}

std::vector<uint8_t> encodeInsertPayload(uint32_t rowID, const std::vector<ColValue>& values) {
    std::vector<uint8_t> payload;
    appendScalar(payload, rowID);
    const uint16_t ncols = static_cast<uint16_t>(values.size());
    appendScalar(payload, ncols);
    appendScalar(payload, uint16_t(0));
    for (const auto& value : values) {
        const uint8_t type = static_cast<uint8_t>(value.type);
        appendScalar(payload, type);
        appendScalar(payload, uint8_t(0));
        appendScalar(payload, uint8_t(0));
        appendScalar(payload, uint8_t(0));

        uint32_t byteLen = 0;
        switch (value.type) {
            case ColType::UINT32: byteLen = 4; break;
            case ColType::INT64:  byteLen = 8; break;
            case ColType::FLOAT:  byteLen = 4; break;
            case ColType::DOUBLE: byteLen = 8; break;
            case ColType::STRING: byteLen = static_cast<uint32_t>(value.str.size()); break;
        }
        appendScalar(payload, byteLen);
        switch (value.type) {
            case ColType::UINT32: appendScalar(payload, value.u32); break;
            case ColType::INT64:  appendScalar(payload, value.i64); break;
            case ColType::FLOAT:  appendScalar(payload, value.f32); break;
            case ColType::DOUBLE: appendScalar(payload, value.f64); break;
            case ColType::STRING:
                appendBytes(payload, value.str.data(), value.str.size());
                break;
        }
    }
    return payload;
}

std::vector<uint8_t> encodeDeletePayload(uint32_t rowID) {
    std::vector<uint8_t> payload;
    appendScalar(payload, rowID);
    return payload;
}

Wal::Operation decodeInsert(uint64_t opID, const std::vector<uint8_t>& payload) {
    size_t pos = 0;
    Wal::Operation op;
    op.kind = Wal::Operation::Kind::Insert;
    op.opID = opID;
    op.rowID = readScalar<uint32_t>(payload, pos);
    const uint16_t ncols = readScalar<uint16_t>(payload, pos);
    (void)readScalar<uint16_t>(payload, pos);
    op.values.reserve(ncols);
    for (uint16_t i = 0; i < ncols; ++i) {
        const auto type = static_cast<ColType>(readScalar<uint8_t>(payload, pos));
        pos += 3; // reserved
        const uint32_t byteLen = readScalar<uint32_t>(payload, pos);
        if (pos + byteLen > payload.size())
            throw std::runtime_error("short WAL payload");
        switch (type) {
            case ColType::UINT32: {
                if (byteLen != 4) throw std::runtime_error("bad WAL uint32 length");
                op.values.emplace_back(readScalar<uint32_t>(payload, pos));
                break;
            }
            case ColType::INT64: {
                if (byteLen != 8) throw std::runtime_error("bad WAL int64 length");
                op.values.emplace_back(readScalar<int64_t>(payload, pos));
                break;
            }
            case ColType::FLOAT: {
                if (byteLen != 4) throw std::runtime_error("bad WAL float length");
                op.values.emplace_back(readScalar<float>(payload, pos));
                break;
            }
            case ColType::DOUBLE: {
                if (byteLen != 8) throw std::runtime_error("bad WAL double length");
                op.values.emplace_back(readScalar<double>(payload, pos));
                break;
            }
            case ColType::STRING: {
                std::string value(reinterpret_cast<const char*>(payload.data() + pos), byteLen);
                pos += byteLen;
                op.values.emplace_back(std::move(value));
                break;
            }
        }
    }
    return op;
}

Wal::Operation decodeDelete(uint64_t opID, const std::vector<uint8_t>& payload) {
    size_t pos = 0;
    Wal::Operation op;
    op.kind = Wal::Operation::Kind::Delete;
    op.opID = opID;
    op.rowID = readScalar<uint32_t>(payload, pos);
    return op;
}

} // namespace

Wal::Wal(const std::string& tablePath) : path_(tablePath + ".wal") {}

Wal::~Wal() {
    if (fd_ >= 0) ::close(fd_);
}

void Wal::openOrCreate(bool create) {
    if (fd_ >= 0) ::close(fd_);
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    if (fd_ < 0)
        throw std::runtime_error(std::string("open WAL failed: ") + std::strerror(errno));
    if (create) {
        if (::ftruncate(fd_, 0) != 0)
            throw std::runtime_error(std::string("truncate WAL failed: ") + std::strerror(errno));
    }
    ensureHeader();

    nextOpID_ = 1;
    const auto ops = committedOperations();
    for (const auto& op : ops)
        if (op.opID >= nextOpID_) nextOpID_ = op.opID + 1;
}

void Wal::ensureHeader() {
    const off_t end = ::lseek(fd_, 0, SEEK_END);
    if (end < 0)
        throw std::runtime_error(std::string("seek WAL failed: ") + std::strerror(errno));
    if (end == 0) {
        WalHeader header{WAL_MAGIC, WAL_VERSION, 0};
        if (::pwrite(fd_, &header, sizeof(header), 0) != ssize_t(sizeof(header)))
            throw std::runtime_error(std::string("write WAL header failed: ") + std::strerror(errno));
        return;
    }

    WalHeader header{};
    if (::pread(fd_, &header, sizeof(header), 0) != ssize_t(sizeof(header)))
        throw std::runtime_error(std::string("read WAL header failed: ") + std::strerror(errno));
    if (header.magic != WAL_MAGIC || header.version != WAL_VERSION)
        throw std::runtime_error("invalid WAL header");
}

void Wal::appendRecord(uint8_t type, uint64_t opID, const std::vector<uint8_t>& payload) {
    RecordHeader header{};
    header.payloadSize = static_cast<uint32_t>(payload.size());
    header.type = type;
    header.opID = opID;

    std::vector<uint8_t> checksumBuf;
    checksumBuf.reserve(1 + sizeof(opID) + payload.size());
    checksumBuf.push_back(type);
    appendScalar(checksumBuf, opID);
    checksumBuf.insert(checksumBuf.end(), payload.begin(), payload.end());
    header.checksum = fnv1a32(checksumBuf.data(), checksumBuf.size());

    const off_t end = ::lseek(fd_, 0, SEEK_END);
    if (end < 0)
        throw std::runtime_error(std::string("seek WAL append failed: ") + std::strerror(errno));
    if (::pwrite(fd_, &header, sizeof(header), end) != ssize_t(sizeof(header)))
        throw std::runtime_error(std::string("write WAL record header failed: ") + std::strerror(errno));
    if (!payload.empty()) {
        if (::pwrite(fd_, payload.data(), payload.size(), end + sizeof(header)) != ssize_t(payload.size()))
            throw std::runtime_error(std::string("write WAL payload failed: ") + std::strerror(errno));
    }
}

uint64_t Wal::appendInsert(uint32_t rowID, const std::vector<ColValue>& values) {
    const uint64_t opID = nextOpID_++;
    appendRecord(static_cast<uint8_t>(RecordType::Insert), opID, encodeInsertPayload(rowID, values));
    return opID;
}

uint64_t Wal::appendDelete(uint32_t rowID) {
    const uint64_t opID = nextOpID_++;
    appendRecord(static_cast<uint8_t>(RecordType::Delete), opID, encodeDeletePayload(rowID));
    return opID;
}

void Wal::appendCommit(uint64_t opID) {
    appendRecord(static_cast<uint8_t>(RecordType::Commit), opID, {});
}

std::vector<Wal::Operation> Wal::committedOperations() const {
    std::vector<Operation> committed;
    if (fd_ < 0) return committed;

    std::unordered_map<uint64_t, Operation> pending;
    off_t pos = sizeof(WalHeader);
    const off_t end = ::lseek(fd_, 0, SEEK_END);
    while (pos + off_t(sizeof(RecordHeader)) <= end) {
        RecordHeader header{};
        if (::pread(fd_, &header, sizeof(header), pos) != ssize_t(sizeof(header)))
            break;

        const off_t payloadPos = pos + sizeof(header);
        const off_t nextPos = payloadPos + off_t(header.payloadSize);
        if (nextPos > end) break;

        std::vector<uint8_t> payload(header.payloadSize);
        if (!payload.empty()) {
            if (::pread(fd_, payload.data(), payload.size(), payloadPos) != ssize_t(payload.size()))
                break;
        }

        std::vector<uint8_t> checksumBuf;
        checksumBuf.reserve(1 + sizeof(header.opID) + payload.size());
        checksumBuf.push_back(header.type);
        appendScalar(checksumBuf, header.opID);
        checksumBuf.insert(checksumBuf.end(), payload.begin(), payload.end());
        if (fnv1a32(checksumBuf.data(), checksumBuf.size()) != header.checksum)
            break;

        try {
            switch (static_cast<RecordType>(header.type)) {
                case RecordType::Insert:
                    pending[header.opID] = decodeInsert(header.opID, payload);
                    break;
                case RecordType::Delete:
                    pending[header.opID] = decodeDelete(header.opID, payload);
                    break;
                case RecordType::Commit: {
                    auto it = pending.find(header.opID);
                    if (it != pending.end()) {
                        committed.push_back(it->second);
                        pending.erase(it);
                    }
                    break;
                }
                default:
                    return committed;
            }
        } catch (...) {
            break;
        }

        pos = nextPos;
    }

    return committed;
}

void Wal::sync() const {
    if (fd_ >= 0) ::fsync(fd_);
}

void Wal::truncate() {
    if (fd_ < 0) return;
    if (::ftruncate(fd_, sizeof(WalHeader)) != 0)
        throw std::runtime_error(std::string("truncate WAL failed: ") + std::strerror(errno));
    nextOpID_ = 1;
}

bool Wal::hasEntries() const {
    if (fd_ < 0) return false;
    struct stat st{};
    if (::fstat(fd_, &st) != 0) return false;
    return st.st_size > off_t(sizeof(WalHeader));
}
