// Table.cpp
#include "Table.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

// GPU hooks (implemented in gpu_scan_equals.mm)
extern "C" bool metalIsAvailable();
std::vector<uint32_t>
gpuScanEquals(const std::vector<uint32_t>& values,
              const std::vector<uint32_t>& rowIDs,
              uint32_t needle);

void Table::openOrCreate(uint16_t pageSize, uint16_t numColumns, bool create) {
    fd_ = open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    assert(fd_ >= 0);

    if (create) {
        mp_ = MasterPage::initnew(fd_, pageSize, numColumns);
    } else {
        mp_ = MasterPage::load(fd_);
        numColumns = mp_.numColumns;
    }

    cols_.clear();
    cols_.reserve(numColumns);
    for (uint16_t c = 0; c < numColumns; ++c) {
        cols_.emplace_back(path_, mp_, c);
    }

    // Initialize/open RowIndex sidecar now that numColumns is known
    rowIndex_ = RowIndex(path_, numColumns);
    rowIndex_.openOrCreate();
}

Table::Table(const std::string& path, uint16_t pageSize, uint16_t numColumns)
  : path_(path), fd_(-1), rowIndex_(path, numColumns) {
    openOrCreate(pageSize, numColumns, /*create=*/true);
}

Table::Table(const std::string& path)
  : path_(path), fd_(-1), rowIndex_(path, 0) {
    openOrCreate(/*pageSize*/0, /*numColumns*/0, /*create=*/false);
}

uint32_t Table::insertRow(const std::vector<ValueType>& values) {
    assert(values.size() == cols_.size());
    std::vector<uint32_t> slots(values.size());
    for (size_t c = 0; c < values.size(); ++c) {
        slots[c] = cols_[c].allocSlot(values[c]);
    }
    uint32_t rowID = rowIndex_.appendRow(slots);
    return rowID;
}

std::vector<std::optional<ValueType>> Table::fetchRow(uint32_t rowID) {
    auto slotsOpt = rowIndex_.fetch(rowID);
    std::vector<std::optional<ValueType>> out(cols_.size());
    if (!slotsOpt) return out; // all nullopt if deleted or out-of-range

    const auto& slots = *slotsOpt;
    for (size_t c = 0; c < cols_.size(); ++c) {
        out[c] = cols_[c].fetchSlot(slots[c]);
    }
    return out;
}

void Table::deleteRow(uint32_t rowID) {
    auto slotsOpt = rowIndex_.fetch(rowID);
    if (!slotsOpt) return;
    auto& slots = *slotsOpt;
    for (size_t c = 0; c < cols_.size(); ++c) {
        cols_[c].deleteSlot(slots[c]);
    }
    rowIndex_.markDeleted(rowID);
}

std::vector<ValueType> Table::materializeColumn(uint16_t colIdx) {
    assert(colIdx < cols_.size());
    std::vector<ValueType> out;
    out.reserve(1024); // heuristic; will grow as needed

    rowIndex_.forEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        auto v = cols_[colIdx].fetchSlot(slots[colIdx]);
        if (v.has_value()) out.push_back(*v);
        // if tombstoned mid-flight, skip
    });
    return out;
}

ValueType Table::sumColumn(uint16_t colIdx) {
    assert(colIdx < cols_.size());
    uint64_t acc = 0; // avoid overflow for many values
    rowIndex_.forEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        auto v = cols_[colIdx].fetchSlot(slots[colIdx]);
        if (v) acc += *v;
    });
    return static_cast<ValueType>(acc);
}

Table::Materialized Table::materializeColumnWithRowIDs(uint16_t colIdx) {
    assert(colIdx < cols_.size());
    Materialized m;
    m.values.reserve(1024);
    m.rowIDs.reserve(1024);

    rowIndex_.forEachLive([&](uint32_t rowID, const std::vector<uint32_t>& slots){
        auto v = cols_[colIdx].fetchSlot(slots[colIdx]);
        if (v) {
            m.values.push_back(*v);
            m.rowIDs.push_back(rowID);
        }
    });
    return m;
}

// CPU helper over a materialized view (used when GPU is off/unavailable/small)
std::vector<uint32_t> Table::scanEqualsCPUFromMaterialized(uint16_t colIdx, ValueType val) {
    auto m = materializeColumnWithRowIDs(colIdx);
    std::vector<uint32_t> out;
    out.reserve(m.values.size());
    for (size_t i = 0; i < m.values.size(); ++i) {
        if (m.values[i] == val) out.push_back(m.rowIDs[i]);
    }
    return out;
}

// Hybrid scanEquals: CPU for small inputs, GPU for large
std::vector<uint32_t> Table::scanEquals(uint16_t colIdx, ValueType val) {
    assert(colIdx < cols_.size());

    // Materialize once. If small or GPU is unavailable, use CPU on the vectors.
    auto m = materializeColumnWithRowIDs(colIdx);
    const size_t n = m.values.size();

    if (!useGPU_ || n < gpuThreshold_ || !metalIsAvailable()) {
        std::vector<uint32_t> out;
        out.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            if (m.values[i] == val) out.push_back(m.rowIDs[i]);
        }
        return out;
    }

    // GPU path
    return gpuScanEquals(m.values, m.rowIDs, static_cast<uint32_t>(val));
}

// gpu_sum host entry
uint64_t gpuSumU32(const std::vector<uint32_t>& values);


// Hybrid sum: CPU for small, GPU for large when available
ValueType Table::sumColumnHybrid(uint16_t colIdx) {
    assert(colIdx < cols_.size());

    auto vals = materializeColumn(colIdx);
    const size_t n = vals.size();
    if (!useGPU_ || n < gpuThreshold_ || !metalIsAvailable()) {
        uint64_t acc = 0;
        for (auto v : vals) acc += v;
        return static_cast<ValueType>(acc);
    }

    uint64_t s = gpuSumU32(vals);
    return static_cast<ValueType>(s);
}

