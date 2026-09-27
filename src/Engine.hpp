#pragma once
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
};
