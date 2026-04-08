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
    std::vector<uint32_t> whereBetween(uint16_t colIdx, ValueType lo, ValueType hi);
    std::vector<uint32_t> scanPredicate(const Predicate& predicate);
    std::vector<uint32_t> whereAnd(const std::vector<Predicate>& predicates);
    std::vector<uint32_t> whereOr(const std::vector<Predicate>& predicates);

    // Knobs
    void setUseGPU(bool on) { useGPU_ = on; }
    void setGPUThreshold(size_t n) { gpuThreshold_ = n; }

    // Core ops (legacy ValueType / new typed)
    uint32_t insertRow(const std::vector<ValueType> &values);
    uint32_t insertTypedRow(const std::vector<ColValue> &values);
    std::vector<std::optional<ValueType>> fetchRow(uint32_t rowID);
    std::vector<std::optional<ColValue>>  fetchTypedRow(uint32_t rowID);
    void deleteRow(uint32_t rowID);
    void flushDurable();

    // Scans / Aggregates
    std::vector<ValueType> materializeColumn(uint16_t colIdx);
    Materialized materializeColumnWithRowIDs(uint16_t colIdx);

    // Hybrid scan (CPU for small / no-GPU; GPU for large)
    std::vector<uint32_t> scanEquals(uint16_t colIdx, ValueType val);

    // CPU-only string equality scan (STRING columns only)
    std::vector<uint32_t> scanEqualsString(uint16_t colIdx, const std::string& needle);

    // CPU-only sum (you already had this)
    ValueType sumColumn(uint16_t colIdx);

    // Hybrid sum (CPU for small / no-GPU; GPU for large)
    ValueType sumColumnHybrid(uint16_t colIdx);

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
};
