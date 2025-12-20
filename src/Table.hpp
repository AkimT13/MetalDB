#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "ValueTypes.hpp"
#include "MasterPage.hpp"
#include "ColumnFile.hpp"
#include "RowIndex.hpp"

class Table
{
public:
    struct Materialized
    {
        std::vector<ValueType> values;
        std::vector<uint32_t> rowIDs;
    };

    // Constructors
    Table(const std::string &path, uint16_t pageSize, uint16_t numColumns);
    Table(const std::string &path);
    std::vector<uint32_t> whereBetween(uint16_t colIdx, ValueType lo, ValueType hi);

    // Knobs
    void setUseGPU(bool on) { useGPU_ = on; }
    void setGPUThreshold(size_t n) { gpuThreshold_ = n; }

    // Core ops
    uint32_t insertRow(const std::vector<ValueType> &values);
    std::vector<std::optional<ValueType>> fetchRow(uint32_t rowID);
    void deleteRow(uint32_t rowID);

    // Scans / Aggregates
    std::vector<ValueType> materializeColumn(uint16_t colIdx);
    Materialized materializeColumnWithRowIDs(uint16_t colIdx);

    // Hybrid scan (CPU for small / no-GPU; GPU for large)
    std::vector<uint32_t> scanEquals(uint16_t colIdx, ValueType val);

    // CPU-only sum (you already had this)
    ValueType sumColumn(uint16_t colIdx);

    // Hybrid sum (CPU for small / no-GPU; GPU for large)
    ValueType sumColumnHybrid(uint16_t colIdx);

    std::vector<std::vector<ValueType>>
    projectRows(const std::vector<uint32_t> &rowIDs, const std::vector<uint16_t> &cols);
    // Helper access for algos: expose column file and a forEach wrapper
    inline ColumnFile &columnFile(uint16_t c) { return cols_[c]; }

    template <typename Fn>
    void rowIndexForEachLive(Fn fn) { rowIndex_.forEachLive(fn); }
    size_t numColumns() const { return cols_.size(); }

private:
    void openOrCreate(uint16_t pageSize, uint16_t numColumns, bool create);

    // CPU helper (over materialized vectors)
    std::vector<uint32_t> scanEqualsCPUFromMaterialized(uint16_t colIdx, ValueType val);

    std::string path_;
    int fd_;
    MasterPage mp_;
    std::vector<ColumnFile> cols_;
    RowIndex rowIndex_;

    // GPU usage knobs (single definition!)
    bool useGPU_ = true;
    size_t gpuThreshold_ = 4096;
};