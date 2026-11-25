#include "Engine.hpp"
#include "GroupBy.hpp"
#include "Join.hpp"
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

std::string Engine::tablePath(const std::string& name) const {
    return resolveTableBase(name) + ".mdb";
}

namespace {
void rejectEscapes(const std::string& path, const char* what) {
    if (path.empty()) throw std::invalid_argument(std::string(what) + " must not be empty");
    if (path[0] == '/') throw std::invalid_argument(std::string(what) + " must be relative to the data directory");
    size_t start = 0;
    while (start <= path.size()) {
        size_t end = path.find('/', start);
        if (end == std::string::npos) end = path.size();
        if (path.compare(start, end - start, "..") == 0)
            throw std::invalid_argument(std::string(what) + " must not contain '..'");
        start = end + 1;
    }
}
}  // namespace

void Engine::setDataDir(const std::string& dir) {
    dataDir_ = dir;
    while (dataDir_.size() > 1 && dataDir_.back() == '/') dataDir_.pop_back();
    journalChecked_.store(false);  // the new directory may hold its own journal
}

// ── table write locks ───────────────────────────────────────────────────────

uint64_t Engine::newSessionId() {
    std::lock_guard<std::mutex> g(lockMutex_);
    return nextSessionId_++;
}

void Engine::acquireWriteLock(const std::string& table, uint64_t owner, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(lockMutex_);
    const bool ok = lockCv_.wait_for(lk, timeout, [&] {
        auto it = writeLockOwner_.find(table);
        return it == writeLockOwner_.end() || it->second == owner;
    });
    if (!ok)
        throw std::invalid_argument("could not obtain lock on table '" + table +
                                    "': another transaction is writing it");
    writeLockOwner_[table] = owner;
}

void Engine::releaseWriteLock(const std::string& table, uint64_t owner) {
    {
        std::lock_guard<std::mutex> g(lockMutex_);
        auto it = writeLockOwner_.find(table);
        if (it != writeLockOwner_.end() && it->second == owner) writeLockOwner_.erase(it);
    }
    lockCv_.notify_all();
}

void Engine::releaseWriteLocks(uint64_t owner) {
    {
        std::lock_guard<std::mutex> g(lockMutex_);
        for (auto it = writeLockOwner_.begin(); it != writeLockOwner_.end();)
            it = it->second == owner ? writeLockOwner_.erase(it) : std::next(it);
    }
    lockCv_.notify_all();
}

// ── multi-table commit journal ───────────────────────────────────────────────
// File layout: "MTXJ" u32 version | u32 payloadSize | u32 fnv1a(payload) | payload | u32 COMMIT
// payload: u32 ntables, then per table: name, u32 expectedFirstRow, deletes, inserts.
// A journal without the trailing COMMIT word (crash while writing it) is ignored.

namespace {

constexpr uint32_t kJournalMagic = 0x4A58544D;   // "MTXJ"
constexpr uint32_t kJournalCommit = 0xC0FFEE01;

uint32_t fnv1a(const std::string& b) {
    uint32_t h = 2166136261u;
    for (unsigned char c : b) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

void putU32(std::string& out, uint32_t v) { out.append(reinterpret_cast<const char*>(&v), 4); }

void putValue(std::string& out, const ColValue& v) {
    out.push_back(static_cast<char>(v.type));
    switch (v.type) {
        case ColType::UINT32: out.append(reinterpret_cast<const char*>(&v.u32), 4); break;
        case ColType::INT64: out.append(reinterpret_cast<const char*>(&v.i64), 8); break;
        case ColType::FLOAT: out.append(reinterpret_cast<const char*>(&v.f32), 4); break;
        case ColType::DOUBLE: out.append(reinterpret_cast<const char*>(&v.f64), 8); break;
        case ColType::STRING:
            putU32(out, static_cast<uint32_t>(v.str.size()));
            out += v.str;
            break;
    }
}

struct Reader {
    const std::string& b;
    size_t pos = 0;
    void need(size_t n) {
        if (pos + n > b.size()) throw std::runtime_error("transaction journal is truncated");
    }
    uint32_t u32() {
        need(4);
        uint32_t v;
        std::memcpy(&v, b.data() + pos, 4);
        pos += 4;
        return v;
    }
    std::string bytes(size_t n) {
        need(n);
        std::string s = b.substr(pos, n);
        pos += n;
        return s;
    }
    ColValue value() {
        need(1);
        const auto type = static_cast<ColType>(b[pos++]);
        switch (type) {
            case ColType::UINT32: { uint32_t v; std::memcpy(&v, bytes(4).data(), 4); return ColValue(v); }
            case ColType::INT64: { int64_t v; std::memcpy(&v, bytes(8).data(), 8); return ColValue(v); }
            case ColType::FLOAT: { float v; std::memcpy(&v, bytes(4).data(), 4); return ColValue(v); }
            case ColType::DOUBLE: { double v; std::memcpy(&v, bytes(8).data(), 8); return ColValue(v); }
            case ColType::STRING: { const uint32_t n = u32(); return ColValue(bytes(n)); }
        }
        throw std::runtime_error("transaction journal has an unknown value type");
    }
};

struct JournalEntry {
    std::string table;
    uint32_t expectedFirstRow = 0;
    std::vector<uint32_t> deletes;
    std::vector<std::vector<ColValue>> inserts;
};

void writeFileDurable(const std::string& path, const std::string& data, bool fsyncIt) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) throw std::runtime_error("cannot write transaction journal: " + std::string(std::strerror(errno)));
    const ssize_t n = ::write(fd, data.data(), data.size());
    if (fsyncIt) ::fsync(fd);
    ::close(fd);
    if (n != static_cast<ssize_t>(data.size())) throw std::runtime_error("short write to transaction journal");
}

}  // namespace

std::string Engine::journalPath() const {
    return (dataDir_.empty() ? std::string(".") : dataDir_) + "/.mdb_txn_journal";
}

void Engine::commitTables(const std::vector<TableCommit>& commits) {
    std::lock_guard<std::mutex> jg(journalMutex_);
    // Preflight under each table's statement mutex: rows valid, capacity available,
    // and the first row ID each table will assign (stable: we hold its write lock).
    std::vector<JournalEntry> entries;
    for (const auto& c : commits) {
        std::lock_guard<std::mutex> lk(tableMutex(c.table));
        Table& t = openTable(c.table);
        for (const auto& row : c.inserts) t.validateRow(row);
        t.ensureCapacity(c.inserts, c.deletes.size());
        entries.push_back({c.table, static_cast<uint32_t>(t.rowsRecorded()), c.deletes, c.inserts});
    }

    std::string payload;
    putU32(payload, static_cast<uint32_t>(entries.size()));
    for (const auto& e : entries) {
        putU32(payload, static_cast<uint32_t>(e.table.size()));
        payload += e.table;
        putU32(payload, e.expectedFirstRow);
        putU32(payload, static_cast<uint32_t>(e.deletes.size()));
        for (uint32_t id : e.deletes) putU32(payload, id);
        putU32(payload, static_cast<uint32_t>(e.inserts.size()));
        for (const auto& row : e.inserts) {
            putU32(payload, static_cast<uint32_t>(row.size()));
            for (const auto& v : row) putValue(payload, v);
        }
    }
    std::string file;
    putU32(file, kJournalMagic);
    putU32(file, 1);
    putU32(file, static_cast<uint32_t>(payload.size()));
    putU32(file, fnv1a(payload));
    file += payload;
    putU32(file, kJournalCommit);
    writeFileDurable(journalPath(), file, syncCommit_);  // ← commit point

    if (const char* fp = std::getenv("MDB_TEST_CRASH_AFTER_TABLES")) {  // crash-recovery tests only
        size_t applied = 0;
        const size_t crashAfter = std::strtoul(fp, nullptr, 10);
        for (const auto& c : commits) {
            if (applied++ == crashAfter) std::_Exit(0);
            std::lock_guard<std::mutex> lk(tableMutex(c.table));
            openTable(c.table).applyAtomic(c.deletes, c.inserts);
        }
        std::_Exit(0);
    }

    for (const auto& c : commits) {
        std::lock_guard<std::mutex> lk(tableMutex(c.table));
        openTable(c.table).applyAtomic(c.deletes, c.inserts);
    }
    ::unlink(journalPath().c_str());
}

void Engine::recoverJournal() {
    if (journalChecked_.load(std::memory_order_acquire)) return;
    std::lock_guard<std::recursive_mutex> g(recoveryMutex_);  // other threads wait for replay to finish
    if (journalChecked_.load() || recovering_) return;      // recovering_: replay re-entering openTable
    recovering_ = true;
    try {
        replayJournal();
    } catch (...) {
        recovering_ = false;  // stay unchecked: every later table access retries and fails loudly
        throw;
    }
    recovering_ = false;
    journalChecked_.store(true, std::memory_order_release);
}

void Engine::replayJournal() {
    const std::string path = journalPath();
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;  // no journal: nothing in flight
    std::string file;
    char buf[1 << 16];
    ssize_t n;
    while ((n = ::read(fd, buf, sizeof(buf))) > 0) file.append(buf, static_cast<size_t>(n));
    ::close(fd);

    Reader hdr{file};
    if (file.size() < 20) { ::unlink(path.c_str()); return; }
    if (hdr.u32() != kJournalMagic || hdr.u32() != 1) throw std::runtime_error("unrecognized transaction journal " + path);
    const uint32_t size = hdr.u32();
    const uint32_t sum = hdr.u32();
    if (file.size() < 16 + size_t(size) + 4) { ::unlink(path.c_str()); return; }  // torn: never committed
    const std::string payload = file.substr(16, size);
    uint32_t marker;
    std::memcpy(&marker, file.data() + 16 + size, 4);
    if (fnv1a(payload) != sum || marker != kJournalCommit) { ::unlink(path.c_str()); return; }

    Reader r{payload};
    std::vector<JournalEntry> entries(r.u32());
    for (auto& e : entries) {
        e.table = r.bytes(r.u32());
        e.expectedFirstRow = r.u32();
        e.deletes.resize(r.u32());
        for (auto& id : e.deletes) id = r.u32();
        e.inserts.resize(r.u32());
        for (auto& row : e.inserts) {
            row.resize(r.u32());
            for (auto& v : row) v = r.value();
        }
    }
    // Roll forward: a table either already has this transaction's rows (applied
    // before the crash) or is exactly where the commit left it. No table mutex: this
    // runs before any table is first opened (other threads wait at recoveryMutex_),
    // and the caller may already hold the mutex of the table it is about to open.
    for (const auto& e : entries) {
        Table& t = openTable(e.table);
        const size_t recorded = t.rowsRecorded();
        if (recorded == e.expectedFirstRow) {
            t.applyAtomic(e.deletes, e.inserts);
            t.flushDurable();
        } else if (recorded != e.expectedFirstRow + e.inserts.size()) {
            throw std::runtime_error("transaction journal does not match table '" + e.table +
                                     "' (expected " + std::to_string(e.expectedFirstRow) + " rows, found " +
                                     std::to_string(recorded) + "); run mdb verify");
        }
    }
    ::unlink(path.c_str());
}

std::string Engine::resolveTableBase(const std::string& name) const {
    if (dataDir_.empty()) return name;
    rejectEscapes(name, "table name");
    return dataDir_ + "/" + name;
}

std::string Engine::resolveFile(const std::string& path) const {
    if (dataDir_.empty()) return path;
    rejectEscapes(path, "file path");
    return dataDir_ + "/" + path;
}

bool Engine::tableExists(const std::string& name) const {
    return access(tablePath(name).c_str(), F_OK) == 0;
}

std::mutex& Engine::tableMutex(const std::string& name) {
    std::lock_guard<std::mutex> g(registryMutex_);
    auto& slot = tableLocks_[name];
    if (!slot) slot = std::make_unique<std::mutex>();
    return *slot;
}

void Engine::flushAll() {
    std::vector<std::pair<std::string, std::shared_ptr<Table>>> open;
    {
        std::lock_guard<std::mutex> g(registryMutex_);
        open.assign(tables_.begin(), tables_.end());
    }
    for (auto& [name, table] : open) {
        std::lock_guard<std::mutex> lk(tableMutex(name));
        table->flushDurable();
    }
}

void Engine::setSyncCommit(bool on) {
    std::lock_guard<std::mutex> g(registryMutex_);
    syncCommit_ = on;
    for (auto& [name, table] : tables_) table->setSyncCommit(on);
}

Table& Engine::createTable(const std::string& name, uint16_t numCols, uint16_t pageSize) {
    recoverJournal();
    auto p = std::make_shared<Table>(tablePath(name), pageSize, numCols);
    std::lock_guard<std::mutex> g(registryMutex_);
    p->setSyncCommit(syncCommit_);
    tables_[name] = p;
    return *p;
}

Table& Engine::createTypedTable(const std::string& name,
                                const std::vector<ColType>& colTypes,
                                uint16_t pageSize) {
    recoverJournal();
    auto p = std::make_shared<Table>(tablePath(name), pageSize, colTypes);
    std::lock_guard<std::mutex> g(registryMutex_);
    p->setSyncCommit(syncCommit_);
    tables_[name] = p;
    return *p;
}

Table& Engine::openTable(const std::string& name) {
    recoverJournal();
    std::lock_guard<std::mutex> g(registryMutex_);
    auto it = tables_.find(name);
    if (it != tables_.end()) return *(it->second);
    auto p = std::make_shared<Table>(tablePath(name));
    p->setSyncCommit(syncCommit_);
    tables_[name] = p;
    return *p;
}

void Engine::flush(const std::string& name) {
    openTable(name).flushDurable();
}

uint32_t Engine::insert(const std::string& name, const std::vector<ValueType>& row) {
    return openTable(name).insertRow(row);
}

uint32_t Engine::insertTyped(const std::string& name, const std::vector<ColValue>& row) {
    return openTable(name).insertTypedRow(row);
}

std::vector<uint32_t> Engine::whereEq(const std::string& name, uint16_t col, ValueType v) {
    return openTable(name).scanEquals(col, v);
}

std::vector<uint32_t> Engine::whereEqString(const std::string& name, uint16_t col, const std::string& needle) {
    return openTable(name).scanEqualsString(col, needle);
}

std::vector<uint32_t> Engine::whereBetween(const std::string& name, uint16_t col, ValueType lo, ValueType hi) {
    return openTable(name).whereBetween(col, lo, hi);
}

std::vector<uint32_t> Engine::whereAnd(const std::string& name, const std::vector<Predicate>& predicates) {
    return openTable(name).whereAnd(predicates);
}

std::vector<uint32_t> Engine::whereOr(const std::string& name, const std::vector<Predicate>& predicates) {
    return openTable(name).whereOr(predicates);
}

ValueType Engine::sum(const std::string& name, uint16_t col) {
    return openTable(name).sumColumnHybrid(col);
}

ValueType Engine::minColumn(const std::string& name, uint16_t col) {
    return openTable(name).minColumn(col);
}

ValueType Engine::maxColumn(const std::string& name, uint16_t col) {
    return openTable(name).maxColumn(col);
}

std::unordered_map<ValueType, uint64_t>
Engine::groupCount(const std::string& name, uint16_t keyCol) {
    Table& t = openTable(name);
    return GroupBy::countByKey(t, keyCol, t.useGPU(), t.gpuThreshold());
}

std::unordered_map<ValueType, uint64_t>
Engine::groupSum(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    Table& t = openTable(name);
    return GroupBy::sumByKey(t, keyCol, valCol, t.useGPU(), t.gpuThreshold());
}

std::unordered_map<ValueType, double>
Engine::groupAvg(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    return GroupBy::avgByKey(openTable(name), keyCol, valCol);
}

std::unordered_map<ValueType, ValueType>
Engine::groupMin(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    return GroupBy::minByKey(openTable(name), keyCol, valCol);
}

std::unordered_map<ValueType, ValueType>
Engine::groupMax(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    return GroupBy::maxByKey(openTable(name), keyCol, valCol);
}

std::vector<std::pair<uint32_t,uint32_t>>
Engine::join(const std::string& left, uint16_t leftCol,
             const std::string& right, uint16_t rightCol) {
    return Join::hashJoinEq(openTable(left), leftCol, openTable(right), rightCol);
}
