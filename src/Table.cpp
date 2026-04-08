// Table.cpp
#include "Table.hpp"
#include "ColumnFile.hpp"
#include "gpu_string_scan.h"
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <stdexcept>
#include <unordered_map>
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
    rowIndex_.openOrCreate(create);
    wal_.openOrCreate(create);
    if (!create) recoverFromWal();
}

extern "C" std::vector<uint32_t>
gpuScanBetween(const std::vector<uint32_t>& values,
               const std::vector<uint32_t>& rowIDs,
               uint32_t lo, uint32_t hi);


std::vector<uint32_t> Table::whereBetween(uint16_t colIdx, ValueType lo, ValueType hi) {
    assert(colIdx < cols_.size());

    std::vector<ValueType> values; values.reserve(1024);
    std::vector<uint32_t>  rowIDs; rowIDs.reserve(1024);

    // Cache min/max per pageID to avoid repeated header reads
    std::unordered_map<uint16_t, std::pair<ValueType,ValueType>> cache;

    rowIndex_.forEachLive([&](uint32_t rowID, const std::vector<uint32_t>& slots){
        uint32_t slotID = slots[colIdx];
        // Use the public helper (or: uint16_t pid = uint16_t(slotID >> 16);)
        uint16_t pid = ColumnFile::pageIdFromSlotId(slotID);

        auto it = cache.find(pid);
        if (it == cache.end()) {
            auto mm = cols_[colIdx].zoneMap(pid);  // <— CHEAP header peek
            it = cache.emplace(pid, mm).first;
        }

        const auto [pmin, pmax] = it->second;
        if (pmax < lo || pmin > hi) return; // prune page

        // Candidate: fetch actual value and check
        auto v = cols_[colIdx].fetchSlot(slotID);
        if (v) {
            values.push_back(*v);
            rowIDs.push_back(rowID);
        }
    });

    // CPU small path
    const size_t n = values.size();
    if (!useGPU_ || n < gpuThreshold_ || !metalIsAvailable()) {
        std::vector<uint32_t> out; out.reserve(n);
        for (size_t i = 0; i < n; ++i)
            if (values[i] >= lo && values[i] <= hi) out.push_back(rowIDs[i]);
        return out;
    }

    // GPU path
    return gpuScanBetween(values, rowIDs, static_cast<uint32_t>(lo), static_cast<uint32_t>(hi));
}

void Table::validatePredicate(const Predicate& predicate) const {
    if (predicate.colIdx >= cols_.size())
        throw std::invalid_argument("predicate column index out of bounds");

    const ColType type = cols_[predicate.colIdx].colType();
    switch (predicate.kind) {
        case Predicate::Kind::EQ:
            if (type != ColType::UINT32)
                throw std::invalid_argument("numeric predicates require UINT32 columns");
            return;
        case Predicate::Kind::BETWEEN:
            if (type != ColType::UINT32)
                throw std::invalid_argument("numeric predicates require UINT32 columns");
            if (predicate.lo > predicate.hi)
                throw std::invalid_argument("BETWEEN predicates require lo <= hi");
            return;
        case Predicate::Kind::EQ_STRING:
            if (type != ColType::STRING)
                throw std::invalid_argument("string predicates require STRING columns");
            return;
        default:
            throw std::invalid_argument("unknown predicate kind");
    }
}

void Table::validatePredicates(const std::vector<Predicate>& predicates) const {
    for (const auto& predicate : predicates)
        validatePredicate(predicate);
}

std::vector<uint32_t> Table::allLiveRowIDs() const {
    std::vector<uint32_t> rowIDs;
    rowIDs.reserve(rowIndex_.liveRows());
    rowIndex_.forEachLiveID([&](uint32_t rowID) { rowIDs.push_back(rowID); });
    return rowIDs;
}

std::vector<uint32_t> Table::scanPredicate(const Predicate& predicate) {
    validatePredicate(predicate);

    switch (predicate.kind) {
        case Predicate::Kind::EQ:
            return scanEquals(predicate.colIdx, predicate.lo);
        case Predicate::Kind::BETWEEN:
            return whereBetween(predicate.colIdx, predicate.lo, predicate.hi);
        case Predicate::Kind::EQ_STRING:
            return scanEqualsString(predicate.colIdx, predicate.needle);
        default:
            throw std::invalid_argument("unknown predicate kind");
    }
}

std::vector<uint32_t> Table::intersectRowIDs(const std::vector<uint32_t>& lhs,
                                             const std::vector<uint32_t>& rhs) {
    assert(std::is_sorted(lhs.begin(), lhs.end()));
    assert(std::is_sorted(rhs.begin(), rhs.end()));

    std::vector<uint32_t> out;
    out.reserve(std::min(lhs.size(), rhs.size()));

    size_t i = 0, j = 0;
    while (i < lhs.size() && j < rhs.size()) {
        if (lhs[i] == rhs[j]) {
            out.push_back(lhs[i]);
            ++i;
            ++j;
        } else if (lhs[i] < rhs[j]) {
            ++i;
        } else {
            ++j;
        }
    }
    return out;
}

std::vector<uint32_t> Table::unionRowIDs(const std::vector<uint32_t>& lhs,
                                         const std::vector<uint32_t>& rhs) {
    assert(std::is_sorted(lhs.begin(), lhs.end()));
    assert(std::is_sorted(rhs.begin(), rhs.end()));

    std::vector<uint32_t> out;
    out.reserve(lhs.size() + rhs.size());

    size_t i = 0, j = 0;
    while (i < lhs.size() && j < rhs.size()) {
        if (lhs[i] == rhs[j]) {
            out.push_back(lhs[i]);
            ++i;
            ++j;
        } else if (lhs[i] < rhs[j]) {
            out.push_back(lhs[i++]);
        } else {
            out.push_back(rhs[j++]);
        }
    }
    while (i < lhs.size()) out.push_back(lhs[i++]);
    while (j < rhs.size()) out.push_back(rhs[j++]);
    return out;
}

std::vector<uint32_t> Table::whereAnd(const std::vector<Predicate>& predicates) {
    validatePredicates(predicates);
    if (predicates.empty()) return allLiveRowIDs();

    std::vector<uint32_t> result = scanPredicate(predicates.front());
    for (size_t i = 1; i < predicates.size(); ++i) {
        if (result.empty()) return result;
        // GPU scans may return unsorted IDs; sort both operands for merge.
        std::sort(result.begin(), result.end());
        auto rhs = scanPredicate(predicates[i]);
        std::sort(rhs.begin(), rhs.end());
        result = intersectRowIDs(result, rhs);
    }
    return result;
}

std::vector<uint32_t> Table::whereOr(const std::vector<Predicate>& predicates) {
    validatePredicates(predicates);
    if (predicates.empty()) return {};
    // Single predicate: return raw scan result (same as whereEq / whereBetween).
    if (predicates.size() == 1) return scanPredicate(predicates[0]);

    std::vector<uint32_t> result; // starts empty (trivially sorted)
    for (const auto& predicate : predicates) {
        auto rhs = scanPredicate(predicate);
        std::sort(rhs.begin(), rhs.end());
        result = unionRowIDs(result, rhs); // result is sorted after each union
    }
    return result;
}

Table::Table(const std::string& path, uint16_t pageSize, uint16_t numColumns)
  : path_(path), fd_(-1), rowIndex_(path, numColumns), wal_(path) {
    openOrCreate(pageSize, numColumns, /*create=*/true);
}

Table::Table(const std::string& path, uint16_t pageSize,
             const std::vector<ColType>& colTypes)
  : path_(path), fd_(-1), rowIndex_(path, static_cast<uint16_t>(colTypes.size())), wal_(path) {
    fd_ = open(path_.c_str(), O_RDWR | O_CREAT, 0666);
    assert(fd_ >= 0);
    const uint16_t numCols = static_cast<uint16_t>(colTypes.size());
    mp_ = MasterPage::initnew(fd_, pageSize, colTypes);
    cols_.clear();
    cols_.reserve(numCols);
    for (uint16_t c = 0; c < numCols; ++c)
        cols_.emplace_back(path_, mp_, c);
    rowIndex_ = RowIndex(path_, numCols);
    rowIndex_.openOrCreate(/*create=*/true);
    wal_.openOrCreate(/*create=*/true);
}

Table::Table(const std::string& path)
  : path_(path), fd_(-1), rowIndex_(path, 0), wal_(path) {
    openOrCreate(/*pageSize*/0, /*numColumns*/0, /*create=*/false);
}

std::vector<std::vector<ValueType>>
Table::projectRows(const std::vector<uint32_t>& rowIDs, const std::vector<uint16_t>& cols) {
    std::vector<std::vector<ValueType>> out;
    out.reserve(rowIDs.size());
    for (uint32_t rid : rowIDs) {
        auto slotsOpt = rowIndex_.fetch(rid);
        if (!slotsOpt) continue; // deleted
        const auto& slots = *slotsOpt;
        std::vector<ValueType> row;
        row.reserve(cols.size());
        bool ok = true;
        for (uint16_t c : cols) {
            auto v = cols_[c].fetchSlot(slots[c]);
            if (!v) { ok = false; break; }
            row.push_back(*v);
        }
        if (ok) out.push_back(std::move(row));
    }
    return out;
}

uint32_t Table::insertRow(const std::vector<ValueType>& values) {
    std::vector<ColValue> typed;
    typed.reserve(values.size());
    for (ValueType value : values)
        typed.emplace_back(static_cast<uint32_t>(value));
    return insertTypedRow(typed);
}

uint32_t Table::insertTypedRow(const std::vector<ColValue>& values) {
    assert(values.size() == cols_.size());
    const uint32_t rowID = rowIndex_.rowsRecorded();
    const uint64_t opID = wal_.appendInsert(rowID, values);
    wal_.appendCommit(opID);
    return insertTypedRowInternal(values, rowID);
}

std::vector<std::optional<ValueType>> Table::fetchRow(uint32_t rowID) {
    auto slotsOpt = rowIndex_.fetch(rowID);
    std::vector<std::optional<ValueType>> out(cols_.size());
    if (!slotsOpt) return out;
    const auto& slots = *slotsOpt;
    for (size_t c = 0; c < cols_.size(); ++c)
        out[c] = cols_[c].fetchSlot(slots[c]);
    return out;
}

std::vector<std::optional<ColValue>> Table::fetchTypedRow(uint32_t rowID) {
    auto slotsOpt = rowIndex_.fetch(rowID);
    std::vector<std::optional<ColValue>> out(cols_.size());
    if (!slotsOpt) return out;
    const auto& slots = *slotsOpt;
    for (size_t c = 0; c < cols_.size(); ++c)
        out[c] = cols_[c].fetchTypedSlot(slots[c]);
    return out;
}

void Table::deleteRow(uint32_t rowID) {
    auto slotsOpt = rowIndex_.fetch(rowID);
    if (!slotsOpt) return;
    const uint64_t opID = wal_.appendDelete(rowID);
    wal_.appendCommit(opID);
    deleteRowInternal(rowID);
}

uint32_t Table::insertTypedRowInternal(const std::vector<ColValue>& values, uint32_t expectedRowID) {
    std::vector<uint32_t> slots(values.size());
    for (size_t c = 0; c < values.size(); ++c)
        slots[c] = cols_[c].allocTypedSlot(values[c]);
    const uint32_t rowID = rowIndex_.appendRow(slots);
    assert(rowID == expectedRowID);
    return rowID;
}

void Table::deleteRowInternal(uint32_t rowID) {
    auto slotsOpt = rowIndex_.fetch(rowID);
    if (!slotsOpt) return;
    auto& slots = *slotsOpt;
    for (size_t c = 0; c < cols_.size(); ++c)
        cols_[c].deleteSlot(slots[c]);
    rowIndex_.markDeleted(rowID);
}

void Table::flushDurable() {
    wal_.sync();
    for (auto& col : cols_)
        col.syncData();
    rowIndex_.sync();
    if (fd_ >= 0) mp_.sync(fd_);
    wal_.truncate();
}

void Table::recoverFromWal() {
    if (!wal_.hasEntries()) return;
    const auto ops = wal_.committedOperations();
    for (const auto& op : ops) {
        switch (op.kind) {
            case Wal::Operation::Kind::Insert:
                if (op.rowID < rowIndex_.rowsRecorded()) break;
                if (op.rowID != rowIndex_.rowsRecorded())
                    throw std::runtime_error("WAL rowID gap during recovery");
                insertTypedRowInternal(op.values, op.rowID);
                break;
            case Wal::Operation::Kind::Delete:
                if (!rowIndex_.isLive(op.rowID)) break;
                deleteRowInternal(op.rowID);
                break;
        }
    }
    flushDurable();
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
    m.values.reserve(rowIndex_.liveRows());
    m.rowIDs.reserve(rowIndex_.liveRows());

    ColumnFile& col = cols_[colIdx];
    // Cache the last page pointer: consecutive rows in a sequentially-inserted
    // table land on the same page, so we do O(pages) hash-map lookups instead
    // of O(rows). Row order is preserved (required for key/value alignment in GroupBy).
    uint16_t lastPid = UINT16_MAX;
    const ColumnPage* lastPage = nullptr;

    rowIndex_.forEachLive([&](uint32_t rowID, const std::vector<uint32_t>& slots) {
        uint32_t slotID  = slots[colIdx];
        uint16_t pid     = ColumnFile::pageIdFromSlotId(slotID);
        uint16_t slotIdx = ColumnFile::slotIdxFromSlotId(slotID);
        if (pid != lastPid) { lastPage = &col.pageRef(pid); lastPid = pid; }
        if (slotIdx < lastPage->capacity && lastPage->tombstone[slotIdx]) {
            m.values.push_back(lastPage->readValue(slotIdx));
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

ValueType Table::minColumn(uint16_t colIdx) {
    assert(colIdx < cols_.size());
    // Collect distinct page IDs touched by live rows, then take min of page zone-map mins.
    std::unordered_map<uint16_t, std::pair<ValueType,ValueType>> cache;
    rowIndex_.forEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        uint16_t pid = ColumnFile::pageIdFromSlotId(slots[colIdx]);
        if (cache.find(pid) == cache.end())
            cache.emplace(pid, cols_[colIdx].zoneMap(pid));
    });
    ValueType result = std::numeric_limits<ValueType>::max();
    for (auto& [pid, mm] : cache)
        if (mm.first < result) result = mm.first;
    return result;
}

ValueType Table::maxColumn(uint16_t colIdx) {
    assert(colIdx < cols_.size());
    std::unordered_map<uint16_t, std::pair<ValueType,ValueType>> cache;
    rowIndex_.forEachLive([&](uint32_t /*rowID*/, const std::vector<uint32_t>& slots){
        uint16_t pid = ColumnFile::pageIdFromSlotId(slots[colIdx]);
        if (cache.find(pid) == cache.end())
            cache.emplace(pid, cols_[colIdx].zoneMap(pid));
    });
    ValueType result = std::numeric_limits<ValueType>::min();
    for (auto& [pid, mm] : cache)
        if (mm.second > result) result = mm.second;
    return result;
}

std::vector<uint32_t> Table::scanEqualsString(uint16_t colIdx, const std::string& needle) {
    assert(colIdx < cols_.size());

    const size_t n = rowIndex_.liveRows();

    // GPU path: pack strings into Arrow layout and dispatch kernel.
    if (useGPU_ && n >= gpuThreshold_ && metalIsAvailable()) {
        std::vector<uint32_t> slotIDs, liveRowIDs;
        slotIDs.reserve(n); liveRowIDs.reserve(n);
        rowIndex_.forEachLive([&](uint32_t rowID, const std::vector<uint32_t>& slots) {
            slotIDs.push_back(slots[colIdx]);
            liveRowIDs.push_back(rowID);
        });

        std::vector<char>     chars;
        std::vector<int32_t>  offsets;
        cols_[colIdx].packStringsForGPU(slotIDs, chars, offsets);

        std::vector<uint32_t> gpuResult;
        if (gpuStringScanEquals(chars, offsets, liveRowIDs, needle, gpuResult))
            return gpuResult;
        // GPU pipeline failed — fall through to CPU.
    }

    // CPU fallback
    std::vector<uint32_t> rowIDs;
    rowIndex_.forEachLive([&](uint32_t rowID, const std::vector<uint32_t>& slots) {
        auto cv = cols_[colIdx].fetchTypedSlot(slots[colIdx]);
        if (cv && cv->type == ColType::STRING && cv->str == needle)
            rowIDs.push_back(rowID);
    });
    return rowIDs;
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
