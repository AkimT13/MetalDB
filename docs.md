# MetalDB — Component Documentation

> Last updated: 2026-04-04. For in-progress work and known issues see `PROGRESS.md`.

---

## Architecture Overview

```
Engine              public SQL-like facade, table registry
  └─ Table          per-table insert/fetch/delete/scan/aggregate
       ├─ ColumnFile    on-disk column storage (one per table)
       ├─ RowIndex      row→slotID mapping (.mdb.idx sidecar)
       └─ GPU kernels   gpu_scan_equals, gpu_scan_range, gpu_sum, gpu_groupby
```

Each table produces two files:
- `{name}.mdb` — binary column data. Page 0 is `MasterPage`; subsequent pages are `ColumnPage`s.
- `{name}.mdb.idx` — row index (`RIDX` magic). Each entry: 1-byte status + 3-byte pad + `uint32_t slotIDs[numColumns]`.

---

## C And Python APIs

MetalDB now exposes a pure C surface in `src/mdb.h` and an in-repo Python wrapper in `python/mdb.py`.

Build and test flow:
- `make -C src test_c_api` builds the C wrapper and runs the C tests.
- `make -C src test-python` builds `src/libmdb.dylib` and runs the Python integration tests.

Python notes:
- The Python layer uses `ctypes`; there is no compiled Python extension yet.
- It expects `libmdb.dylib` either in `python/` or `src/`.
- Reopened tables still require explicit schema registration via `Engine.open_table(name, col_types)`.
- Wrapper-side validation raises `ValueError` for Python argument mistakes and `MdbError` for engine/C API failures.

---

## ValueTypes.hpp

Defines the core type vocabulary.

```cpp
using ValueType = uint32_t;   // legacy scalar type; all pre-Phase-2 API uses this

enum class ColType : uint8_t {
    UINT32 = 0,   // 4 bytes
    INT64  = 1,   // 8 bytes
    FLOAT  = 2,   // 4 bytes
    DOUBLE = 3,   // 8 bytes
};

uint16_t colValueBytes(ColType t);  // returns 4 or 8

struct ColValue {
    ColType type;
    union { uint32_t u32; int64_t i64; float f32; double f64; };
    explicit ColValue(uint32_t);
    explicit ColValue(int64_t);
    explicit ColValue(float);
    explicit ColValue(double);
    double   toDouble() const;
    ValueType asU32()  const;
};
```

---

## MasterPage

**Files:** `src/MasterPage.hpp`, `src/MasterPage.cpp`

Page 0 of the `.mdb` file. On-disk layout:
```
uint32_t magic         = 0x4D445042
uint16_t pageSize
uint16_t numColumns
uint16_t headPageIDs[numColumns]
uint8_t  colTypes[numColumns]      ← added Phase 2; absent in old files (defaults UINT32)
```

```cpp
struct MasterPage {
    uint32_t magic;
    uint16_t pageSize, numColumns;
    std::vector<uint16_t> headPageIDs;
    std::vector<ColType>  colTypes;      // one per column

    static MasterPage initnew(int fd, uint16_t pageSize, uint16_t numColumns);
    static MasterPage initnew(int fd, uint16_t pageSize, const std::vector<ColType>&);
    static MasterPage load(int fd);
    void flush(int fd) const;
};
```

---

## ColumnPage / Column.hpp

**File:** `src/Column.hpp`

In-memory representation of one data page. Storage is raw bytes to support variable-width types.

```
Header: pageID, capacity, count, nextFreePage, valueBytes, minValue64, maxValue64
Data:   uint8_t rawValues[capacity * valueBytes]
        bool    tombstone[capacity]
```

Key methods: `writeRaw(slot, ptr, n)`, `readRaw(slot, ptr, n)`, `recomputeMinMax()`.
Legacy `writeValue(slot, ValueType)` / `readValue(slot)` wrappers still present for UINT32 columns.

---

## ColumnFile

**Files:** `src/ColumnFile.hpp`, `src/ColumnFile.cpp`

Manages one column stream on disk. Holds `colType_` and `valueBytes_` (from MasterPage).

```cpp
class ColumnFile {
public:
    ColumnFile(const std::string& path, MasterPage& mp, uint16_t colIdx);

    // Typed API (Phase 2+)
    uint32_t              allocTypedSlot(ColValue val);
    std::optional<ColValue> fetchTypedSlot(uint32_t slotID) const;

    // Legacy uint32 API (wraps typed API)
    uint32_t              allocSlot(ValueType val);
    std::optional<ValueType> fetchSlot(uint32_t slotID) const;
    void                  deleteSlot(uint32_t slotID);

    // Zone-map access for range pruning
    std::pair<ValueType,ValueType> zoneMap(uint16_t pageID);

    static uint16_t pageIdFromSlotId(uint32_t slotID);   // slotID >> 16
    ColType colType() const;
};
```

SlotID encoding: `(pageID << 16) | slotIndex`.

---

## RowIndex

**Files:** `src/RowIndex.hpp`, `src/RowIndex.cpp`

Sidecar `.mdb.idx` file mapping `rowID → slotIDs[numColumns]`.

```cpp
class RowIndex {
public:
    RowIndex(const std::string& pathBase, uint16_t numColumns);
    void openOrCreate(bool create = false);   // create=true truncates file

    uint32_t appendRow(const std::vector<uint32_t>& slotIDs);
    void     markDeleted(uint32_t rowID);
    std::optional<std::vector<uint32_t>> fetch(uint32_t rowID) const;
    void     forEachLive(std::function<void(uint32_t, const std::vector<uint32_t>&)>) const;

    uint32_t rowsRecorded() const;
    uint32_t liveRows() const;
};
```

---

## Table

**Files:** `src/Table.hpp`, `src/Table.cpp`

```cpp
class Table {
public:
    // Constructors
    Table(const std::string& path, uint16_t pageSize, uint16_t numColumns);  // all UINT32
    Table(const std::string& path, uint16_t pageSize, const std::vector<ColType>&);
    Table(const std::string& path);  // open existing

    // Insert
    uint32_t insertRow(const std::vector<ValueType>& values);
    uint32_t insertTypedRow(const std::vector<ColValue>& values);

    // Fetch
    std::vector<std::optional<ValueType>> fetchRow(uint32_t rowID);
    std::vector<std::optional<ColValue>>  fetchTypedRow(uint32_t rowID);

    void deleteRow(uint32_t rowID);

    // Aggregations
    ValueType sumColumn(uint16_t colIdx);
    ValueType sumColumnHybrid(uint16_t colIdx);   // GPU when large
    ValueType minColumn(uint16_t colIdx);          // zone-map O(pages)
    ValueType maxColumn(uint16_t colIdx);          // zone-map O(pages)

    // Scans
    std::vector<uint32_t> scanEquals(uint16_t colIdx, ValueType val);      // hybrid
    std::vector<uint32_t> whereBetween(uint16_t colIdx, ValueType lo, ValueType hi);

    // Materialize helpers (used by GroupBy / GPU dispatch)
    std::vector<ValueType>  materializeColumn(uint16_t colIdx);
    Materialized            materializeColumnWithRowIDs(uint16_t colIdx);  // {values, rowIDs}
    std::vector<std::vector<ValueType>> projectRows(const std::vector<uint32_t>& rowIDs,
                                                     const std::vector<uint16_t>& cols);

    // GPU control
    void setUseGPU(bool v);
    void setGPUThreshold(size_t n);
};
```

---

## Engine

**Files:** `src/Engine.hpp`, `src/Engine.cpp`

Top-level facade. Owns a `std::unordered_map<std::string, Table>` registry.

```cpp
class Engine {
public:
    Table& createTable(const std::string& name, uint16_t numCols, uint16_t pageSize = 4096);
    Table& createTypedTable(const std::string& name, const std::vector<ColType>&,
                            uint16_t pageSize = 4096);
    Table& openTable(const std::string& name);
    Table& getTable(const std::string& name);

    uint32_t insert(const std::string& name, const std::vector<ValueType>& row);
    uint32_t insertTyped(const std::string& name, const std::vector<ColValue>& row);

    std::vector<uint32_t> scanEquals(const std::string& name, uint16_t col, ValueType val);
    std::vector<uint32_t> whereBetween(const std::string& name, uint16_t col,
                                        ValueType lo, ValueType hi);

    ValueType sumColumn(const std::string& name, uint16_t col);
    ValueType minColumn(const std::string& name, uint16_t col);
    ValueType maxColumn(const std::string& name, uint16_t col);

    std::unordered_map<ValueType, uint64_t>  groupCount(const std::string& name, uint16_t keyCol);
    std::unordered_map<ValueType, uint64_t>  groupSum  (const std::string& name, uint16_t keyCol,
                                                         uint16_t valCol);
    std::unordered_map<ValueType, double>    groupAvg  (const std::string& name, uint16_t keyCol,
                                                         uint16_t valCol);
    std::unordered_map<ValueType, ValueType> groupMin  (const std::string& name, uint16_t keyCol,
                                                         uint16_t valCol);
    std::unordered_map<ValueType, ValueType> groupMax  (const std::string& name, uint16_t keyCol,
                                                         uint16_t valCol);

    std::vector<std::pair<uint32_t,uint32_t>> join(const std::string& left,  uint16_t leftCol,
                                                    const std::string& right, uint16_t rightCol);
};
```

---

## GroupBy

**Files:** `src/GroupBy.hpp`, `src/GroupBy.cpp`

Static methods. GPU path active when `useGPU=true`, `n >= gpuThreshold`, and `metalIsAvailable()`.

```cpp
namespace GroupBy {
    std::unordered_map<ValueType, uint64_t>  countByKey(Table&, uint16_t keyCol,
                                                          bool useGPU = true,
                                                          size_t gpuThreshold = 4096);
    std::unordered_map<ValueType, uint64_t>  sumByKey  (Table&, uint16_t keyCol,
                                                          uint16_t valCol,
                                                          bool useGPU = true,
                                                          size_t gpuThreshold = 4096);
    std::unordered_map<ValueType, double>    avgByKey  (Table&, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, ValueType> minByKey  (Table&, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, ValueType> maxByKey  (Table&, uint16_t keyCol, uint16_t valCol);
}
```

**Note:** avg/min/max are CPU-only. GPU path for count/sum has known performance issues
(see `PROGRESS.md` — shader compilation not cached).

---

## Join

**Files:** `src/Join.hpp`, `src/Join.cpp`

```cpp
namespace Join {
    // Hash join on equality of one column from each table.
    // Returns pairs of (leftRowID, rightRowID).
    std::vector<std::pair<uint32_t,uint32_t>>
    hashJoinEq(Table& left, uint16_t leftCol, Table& right, uint16_t rightCol);
}
```

---

## GPU Kernels

| File | Kernel | Dispatch | Notes |
|------|--------|----------|-------|
| `gpu_scan_equals.mm` | `scan_equals` | 1D grid over n values | Returns matching rowIDs |
| `gpu_scan_range.mm`  | `scan_between` | 1D grid over n values | lo ≤ v ≤ hi |
| `gpu_sum.mm`         | `reduce_sum_pass1/2` | Two-pass tree reduction | 64-bit accumulator in pass 2 |
| `gpu_groupby.mm`     | `group_by` | 1D grid over n rows | Single-pass device-atomic hash table; **pipeline not cached — slow on first call** |

All kernels: UINT32 inputs only. Falls back to CPU for other ColTypes.
Pipeline state objects are created per-call (not cached) — see `PROGRESS.md` for fix plan.

---

## Build

```bash
# From src/ directory
make              # build all test binaries
make run          # build + run all tests
make fast TEST=test_groupby   # build + run one test
make clean        # remove binaries, .o, .mdb, .mdb.idx
```

Test binaries: `test_engine`, `test_groupby`, `test_gpu_scan_equals`, `test_gpu_sum`,
`test_scan_hybrid`, `test_persist_pages`, `test_where_range`, `test_join`, `test_types`.
