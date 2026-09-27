#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "ValueTypes.hpp"
#include "Predicate.hpp"
#include "MasterPage.hpp"
#include "ColumnFile.hpp"
#include "RowIndex.hpp"
#include "Wal.hpp"

class Table
{
public:
    struct Materialized
    {
        std::vector<ValueType> values;
        std::vector<uint32_t> rowIDs;
    };

    // Constructors
    Table(const std::string &path, uint16_t pageSize, uint16_t numColumns);  // all UINT32
    Table(const std::string &path, uint16_t pageSize,                        // typed columns
          const std::vector<ColType>& colTypes);
    Table(const std::string &path);  // open existing
    ~Table();
    Table(const Table&) = delete;             // owns file descriptors
    Table& operator=(const Table&) = delete;

    const std::string& path() const { return path_; }
    uint16_t pageSize() const { return mp_.pageSize; }
    std::vector<ColType> columnTypes() const {
        std::vector<ColType> types;
        for (const auto& c : cols_) types.push_back(c.colType());
        return types;
    }
    size_t rowsRecorded() const { return rowIndex_.rowsRecorded(); }
    std::vector<uint32_t> whereBetween(uint16_t colIdx, ValueType lo, ValueType hi);
    std::vector<uint32_t> scanPredicate(const Predicate& predicate);
    std::vector<uint32_t> whereAnd(const std::vector<Predicate>& predicates);
    std::vector<uint32_t> whereOr(const std::vector<Predicate>& predicates);

    // Knobs
    void setUseGPU(bool on) { useGPU_ = on; }
    void setGPUThreshold(size_t n) { gpuThreshold_ = n; }
    bool useGPU() const { return useGPU_; }
    size_t gpuThreshold() const { return gpuThreshold_; }

    // Core ops (legacy ValueType / new typed)
    uint32_t insertRow(const std::vector<ValueType> &values);
    uint32_t insertTypedRow(const std::vector<ColValue> &values);
    std::vector<std::optional<ValueType>> fetchRow(uint32_t rowID);
    std::vector<std::optional<ColValue>>  fetchTypedRow(uint32_t rowID);
    // Single cell; nullopt if the row is not live. Cheaper than fetchTypedRow
    // when only a few columns of a wide row are needed.
    std::optional<ColValue> fetchTypedValue(uint32_t rowID, uint16_t colIdx) const;
    void deleteRow(uint32_t rowID);
    void flushDurable();

    // Atomically deletes `deleteRowIDs` (non-live IDs are ignored) and inserts
    // `inserts`, as one WAL transaction: after a crash either all of it or none
    // of it is recovered. Rows are validated before anything is logged, so an
    // invalid row throws std::invalid_argument and leaves the table untouched.
    // Returns the row IDs assigned to the inserted rows, in order.
    std::vector<uint32_t> applyAtomic(const std::vector<uint32_t>& deleteRowIDs,
                                      const std::vector<std::vector<ColValue>>& inserts);

    // Throws std::invalid_argument unless `values` matches the schema exactly.
    void validateRow(const std::vector<ColValue>& values) const;

    // Throws std::runtime_error if `inserts` (after `freedPerColumn` slots are
    // released by accompanying deletes) cannot fit: page-ID space or 4 GiB
    // string heaps. Called before anything is logged — the WAL is redo-only, so a
    // committed operation that later fails to apply would make the table
    // unopenable.
    void ensureCapacity(const std::vector<std::vector<ColValue>>& inserts, size_t freedPerColumn = 0) const;

    // When on, every commit fsyncs the WAL before returning (durable against OS
    // crash / power loss, at the cost of one fsync per statement). Default off:
    // commits survive process crashes; call flushDurable() for a checkpoint.
    void setSyncCommit(bool on) { syncCommit_ = on; }
    bool syncCommit() const { return syncCommit_; }
    bool isLive(uint32_t rowID) const { return rowIndex_.isLive(rowID); }

    // Scans / Aggregates
    std::vector<ValueType> materializeColumn(uint16_t colIdx);
    Materialized materializeColumnWithRowIDs(uint16_t colIdx);

    // Hybrid scan (CPU for small / no-GPU; GPU for large)
    std::vector<uint32_t> scanEquals(uint16_t colIdx, ValueType val);

    // CPU-only string equality scan (STRING columns only)
    std::vector<uint32_t> scanEqualsString(uint16_t colIdx, const std::string& needle);

    // CPU-only sum (you already had this)
    ValueType sumColumn(uint16_t colIdx);

    // Hybrid sum (CPU for small / no-GPU; GPU for large), exact 64-bit. Falls
    // back to the CPU if the GPU path fails.
    uint64_t sumColumn64(uint16_t colIdx);
    // Legacy: truncated to 32 bits (kept for the original C API / tests).
    ValueType sumColumnHybrid(uint16_t colIdx) { return static_cast<ValueType>(sumColumn64(colIdx)); }

    // Min/max via zone-map metadata (header-only reads)
    ValueType minColumn(uint16_t colIdx);
    ValueType maxColumn(uint16_t colIdx);

    std::vector<std::vector<ValueType>>
    projectRows(const std::vector<uint32_t> &rowIDs, const std::vector<uint16_t> &cols);
    // Helper access for algos: expose column file and a forEach wrapper
    inline ColumnFile &columnFile(uint16_t c) { return cols_[c]; }
    inline const ColumnFile &columnFile(uint16_t c) const { return cols_[c]; }

    template <typename Fn>
    void rowIndexForEachLive(Fn fn) { rowIndex_.forEachLive(fn); }
    size_t numColumns() const { return cols_.size(); }
    size_t rowCount() const { return rowIndex_.liveRows(); }

    static std::vector<uint32_t> intersectRowIDs(const std::vector<uint32_t>& lhs,
                                                 const std::vector<uint32_t>& rhs);
    static std::vector<uint32_t> unionRowIDs(const std::vector<uint32_t>& lhs,
                                             const std::vector<uint32_t>& rhs);

private:
    void openOrCreate(uint16_t pageSize, uint16_t numColumns, bool create);
    std::vector<uint32_t> allLiveRowIDs() const;
    void validatePredicate(const Predicate& predicate) const;
    void validatePredicates(const std::vector<Predicate>& predicates) const;
    void recoverFromWal();
    uint32_t insertTypedRowInternal(const std::vector<ColValue>& values, uint32_t expectedRowID);
    void deleteRowInternal(uint32_t rowID);

    // CPU helper (over materialized vectors)
    std::vector<uint32_t> scanEqualsCPUFromMaterialized(uint16_t colIdx, ValueType val);

    std::string path_;
    int fd_;
    MasterPage mp_;
    std::vector<ColumnFile> cols_;
    RowIndex rowIndex_;
    Wal wal_;

    // GPU usage knobs (single definition!)
    bool useGPU_ = true;
    size_t gpuThreshold_ = 4096;
    bool syncCommit_ = false;
};
