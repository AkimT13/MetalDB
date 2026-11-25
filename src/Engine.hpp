#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "Table.hpp"
#include "Predicate.hpp"

class Engine {
public:
    Engine() = default;

    Table& createTable(const std::string& name, uint16_t numCols, uint16_t pageSize = 4096);
    Table& createTypedTable(const std::string& name,
                            const std::vector<ColType>& colTypes,
                            uint16_t pageSize = 4096);
    Table& openTable(const std::string& name);
    void flush(const std::string& name);

    // Applies to every table this engine opens or creates from now on (and to
    // tables already open). See Table::setSyncCommit.
    void setSyncCommit(bool on);

    // ── Concurrency ──────────────────────────────────────────────────────────
    // The table registry is internally synchronized. Table objects themselves are
    // not thread-safe (reads populate page caches), so concurrent callers must
    // hold tableMutex(name) around every operation on that table. executeMiniSQL
    // does this for each statement.
    std::mutex& tableMutex(const std::string& name);

    // ── Sandboxing ───────────────────────────────────────────────────────────
    // With a data directory set, table names and COPY file paths are resolved
    // relative to it; absolute paths and ".." components are rejected. Without
    // one (the default), names are used as filesystem paths as-is.
    void setDataDir(const std::string& dir);
    const std::string& dataDir() const { return dataDir_; }
    std::string resolveTableBase(const std::string& name) const;  // "<base>" (no .mdb)
    std::string resolveFile(const std::string& path) const;
    bool tableExists(const std::string& name) const;

    // Checkpoints every open table (WAL folded into base files, fsync).
    void flushAll();

    // ── Transactions (used by Session) ──────────────────────────────────────
    // Table-level write locks: a writer owns a table until it releases it (an
    // autocommit statement for one statement, a transaction until COMMIT /
    // ROLLBACK). Readers never take them: table files only ever hold committed
    // data, so reads are READ COMMITTED without blocking. Acquiring waits up to
    // `timeout` and then throws std::invalid_argument ("could not obtain lock").
    uint64_t newSessionId();
    void acquireWriteLock(const std::string& table, uint64_t owner, std::chrono::milliseconds timeout);
    void releaseWriteLock(const std::string& table, uint64_t owner);
    void releaseWriteLocks(uint64_t owner);

    // Commits one transaction's changes to several tables atomically: the full
    // change set is written to a commit journal (<data-dir>/.mdb_txn_journal) before
    // any table is touched, then applied table by table, then the journal is
    // cleared. If the process dies mid-way, the next Engine use replays the journal.
    // The caller holds the write lock of every table involved.
    struct TableCommit {
        std::string table;
        std::vector<uint32_t> deletes;
        std::vector<std::vector<ColValue>> inserts;
    };
    void commitTables(const std::vector<TableCommit>& commits);

    uint32_t insert(const std::string& name, const std::vector<ValueType>& row);
    uint32_t insertTyped(const std::string& name, const std::vector<ColValue>& row);
    std::vector<uint32_t> whereEq(const std::string& name, uint16_t col, ValueType v);
    std::vector<uint32_t> whereEqString(const std::string& name, uint16_t col, const std::string& needle);
    std::vector<uint32_t> whereBetween(const std::string& name, uint16_t col, ValueType lo, ValueType hi);
    std::vector<uint32_t> whereAnd(const std::string& name, const std::vector<Predicate>& predicates);
    std::vector<uint32_t> whereOr(const std::string& name, const std::vector<Predicate>& predicates);
    ValueType sum(const std::string& name, uint16_t col);
    ValueType minColumn(const std::string& name, uint16_t col);
    ValueType maxColumn(const std::string& name, uint16_t col);

    // GroupBy aggregations
    std::unordered_map<ValueType, uint64_t>  groupCount(const std::string& name, uint16_t keyCol);
    std::unordered_map<ValueType, uint64_t>  groupSum  (const std::string& name, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, double>    groupAvg  (const std::string& name, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, ValueType> groupMin  (const std::string& name, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, ValueType> groupMax  (const std::string& name, uint16_t keyCol, uint16_t valCol);

    // Hash-equi join: returns (leftRowID, rightRowID) pairs
    std::vector<std::pair<uint32_t,uint32_t>>
    join(const std::string& left, uint16_t leftCol,
         const std::string& right, uint16_t rightCol);

private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::unordered_map<std::string, std::unique_ptr<std::mutex>> tableLocks_;
    std::mutex registryMutex_;
    bool syncCommit_ = false;
    std::string dataDir_;
    std::string tablePath(const std::string& name) const;

    std::mutex lockMutex_;
    std::condition_variable lockCv_;
    std::unordered_map<std::string, uint64_t> writeLockOwner_;
    uint64_t nextSessionId_ = 1;

    std::mutex journalMutex_;                  // one multi-table commit at a time
    std::recursive_mutex recoveryMutex_;       // replay re-enters openTable on the same thread
    std::atomic<bool> journalChecked_{false};
    bool recovering_ = false;
    std::string journalPath() const;
    void recoverJournal();  // once per data directory, before the first table access
    void replayJournal();
};
