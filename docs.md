# MetalDB — Component Documentation

> Last updated: 2026-04-08. For in-progress work and known issues see `PROGRESS.md`.

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
- Both C and Python now expose explicit table flush:
  - C: `mdb_flush`
  - Python: `Engine.flush(name)`

---

## PostgreSQL Wire Protocol

`mdb pgserve <port>` speaks the PostgreSQL v3 protocol (simple-query subset), so stock
clients work unchanged:

```bash
./mdb pgserve 5433 --data-dir /srv/metaldb --password s3cret &
psql -h 127.0.0.1 -p 5433 -U app -c "SELECT c1, count(*) FROM 'events' GROUP BY c1"
```

```python
import psycopg2
conn = psycopg2.connect(host="127.0.0.1", port=5433, user="app", password="s3cret")
cur = conn.cursor()
cur.execute("INSERT INTO 'events' VALUES (%s, %s)", (42, "O'Brien"))   # client-side quoting works
conn.commit()
```

- accepts every `mdb serve` option, plus `--password PW` (cleartext password auth;
  without it, connections are trusted — bind to localhost or use `--data-dir`)
- result columns carry real type OIDs: UINT32 / INT64 → `int8`, FLOAT → `float4`,
  DOUBLE → `float8`, STRING → `text`; values are sent in text format
- command tags: `SELECT n`, `INSERT 0 n`, `UPDATE n`, `DELETE n`, `COPY n`, `CREATE TABLE`
- errors carry SQLSTATEs (`42P01` undefined table, `42703` undefined column,
  `42601` syntax, `22003` out of range, `42501` sandbox violation, `28P01` bad password)
- a Query message may hold several `;`-separated statements; an error stops the rest
- `BEGIN` / `COMMIT` / `ROLLBACK` / `SET` / `RESET` / `DISCARD` are accepted as no-ops for
  driver compatibility — every statement already autocommits atomically, and `ROLLBACK`
  sends a NOTICE saying nothing was undone. `SELECT <integer>` answers health checks.
- not supported: the extended query protocol (server-side prepared statements — drivers
  that require it, e.g. JDBC by default, get SQLSTATE `0A000`), `COPY ... STDIN/STDOUT`,
  TLS, SCRAM / MD5 auth, and `pg_catalog` introspection (so `psql`'s `\d` doesn't work)

Verified against `psql` 16 and psycopg2 2.9.

---

## Operations Tooling

```bash
mdb verify  <table>          # integrity check; exit code 2 on corruption
mdb stats   <table>          # rows, pages, fill factor, heap live/orphaned bytes, file sizes
mdb backup  <table> <dir>    # checkpoint, durable copy of every file, checksummed MANIFEST
mdb restore <dir> <table>    # verify MANIFEST sizes + FNV-1a-64 checksums, then restore
mdb compact <table>          # rebuild with live rows only; renumbers row IDs densely
```

`verify` checks the master page, every live row's slot references (page and slot bounds,
in-use flags, duplicate use, cross-column page sharing), page headers (ID, used-slot
count), that zone maps bound every live UINT32 value (a too-narrow zone map makes range
scans silently skip rows), STRING heap bounds, and the free-page lists (cycles, foreign
pages). Leaked slots are reported as warnings. These tools assume no concurrent writer.

Storage limits: a table holds at most 65,535 pages across all columns (16-bit page IDs;
roughly `65535 × slots_per_page / columns` rows — ~8.9M rows for a 6-column UINT32 table
at 4 KiB pages; larger page sizes raise it), and each STRING column's heap is capped at
4 GiB. Hitting either limit raises an error before anything is written.

---

## CLI Query Surface

The `mdb` CLI now has a one-shot SQL entrypoint:

- `./mdb query "<sql>"`
- `./mdb repl`
- `./mdb serve <port>`
- `./mdb flush <table>`

Supported query shape:
- `SELECT c0, c1 FROM '/tmp/demo'`
- optional `WHERE` boolean expression:
  - comparisons `=`, `!=` / `<>`, `<`, `<=`, `>`, `>=` on every column type
    (numeric literals for numeric columns, string literals for STRING columns;
    strings compare byte-wise)
  - `cN [NOT] BETWEEN a AND b`, `cN [NOT] IN (v1, v2, ...)`
  - `AND`, `OR`, `NOT`, and parentheses with standard precedence (NOT > AND > OR)
  - UINT32 `=` / range leaves use the zone-map-pruned hybrid GPU/CPU scans; other
    leaves are CPU scans combined with sorted row-ID set algebra
  - `-- comments` are ignored
- optional scalar aggregates `COUNT(*)`, `COUNT(cN)`, `SUM(cN)`, `MIN(cN)`, `MAX(cN)`,
  `AVG(cN)` — several per query; `MIN` / `MAX` also work on STRING columns
- optional `GROUP BY cA [, cB ...]` over keys of any type (including STRING), with any
  number of aggregates, combinable with `WHERE`; groups are returned sorted by key.
  A single UINT32 key with only `COUNT` / `SUM` / `AVG` over UINT32 columns and no
  `WHERE` takes the GPU group-by path; everything else is CPU hash aggregation.
- `SELECT DISTINCT cA [, cB ...]` (and `SELECT DISTINCT *`)
- integer `SUM` is exact (128-bit accumulator); `DOUBLE` values print with 15
  significant digits, `FLOAT` with 7
- optional `ORDER BY key [ASC|DESC] [, ...]` where `key` is a selected column, a selected
  aggregate (`count(*)`, `sum(c1)`, ...) or a 1-based output position
- optional `LIMIT n [OFFSET m]`

Write / catalog statements:
- `CREATE TABLE '/tmp/demo' (UINT32, STRING)` or `(c0 UINT32, c1 STRING)`;
  types are `UINT32`, `INT64`, `FLOAT`, `DOUBLE`, `STRING`
- `INSERT INTO '/tmp/demo' VALUES (1, 'a'), (2, 'b')` — literals are coerced to each
  column's type and the whole statement is validated before any row is written
- `DELETE FROM '/tmp/demo' [WHERE ...]` — same `WHERE` grammar as `SELECT`
- `UPDATE '/tmp/demo' SET c1 = 5, c2 = 'x' [WHERE ...]` — copy-on-write: matching
  rows are deleted and re-inserted with new values in one WAL transaction, so
  updated rows get new row IDs (and move to the end of unordered scans)
- every write statement is atomic: all rows are validated first, then logged as a
  single WAL transaction
- `COPY '/tmp/demo' FROM '/path/in.csv' [WITH HEADER]` — RFC 4180 CSV import (quoted
  fields, `""` escapes, embedded newlines, LF or CRLF). The whole file is parsed and
  validated first and inserted as one WAL transaction, then checkpointed; any bad
  record rejects the import with its record number and leaves the table unchanged.
- `COPY '/tmp/demo' TO '/path/out.csv' [WITH HEADER]` and
  `COPY (SELECT ...) TO '/path/out.csv'` — CSV export, written to a temp file and
  renamed into place; floats use round-trip precision so export → import is lossless
- `DESCRIBE '/tmp/demo'` — lists columns and types
- `EXPLAIN <statement>` — one `plan` column describing each stage: table size and GPU
  availability, every WHERE leaf's access path (GPU vs CPU and why, zone-map range
  scans, complements), aggregation strategy, sort, and actual row counts / timings.
  SELECTs are executed; for DELETE / UPDATE only the WHERE clause is evaluated and
  nothing is written.
- REPL: `.timer on` prints execution time after each statement
- `INSERT` / `DELETE` return a single `rows_affected` column; `CREATE TABLE` returns `created`
- writes go through the table WAL; use `mdb flush <table>` for an explicit durable checkpoint
- string literals escape a single quote as `''`

Important limits:
- table references are quoted base paths, not catalog names
- columns are synthetic identifiers `c0`, `c1`, ...
- joins, aliases, arithmetic expressions, subqueries, and CTEs are not supported
- `ORDER BY` keys must appear in the `SELECT` list
- no `HAVING` yet

Output is tab-separated with a header row.

REPL notes:
- statements are executed when terminated by `;`
- multiline queries are accepted until the terminating `;`
- `.help` prints the built-in command summary
- `.quit` exits the session
- REPL execution uses the same mini-SQL executor as `mdb query`

Server notes:
- `mdb serve <port> [--bind ADDR] [--max-connections N] [--idle-timeout SEC]
  [--data-dir DIR] [--sync-commit] [--verbose]`
- clients are served concurrently (one thread per connection) over one shared engine;
  statements are serialized per table, so different tables proceed in parallel
- `--max-connections` (default 64): extra clients receive `ERR\ttoo many connections`
- `--data-dir DIR`: table names and COPY paths resolve inside DIR; absolute paths and
  `..` are rejected (recommended whenever clients are not fully trusted)
- `--sync-commit`: fsync the WAL on every commit
- `--idle-timeout SEC`: idle sessions get `ERR\tidle timeout` and are closed
- requests longer than 16 MiB are rejected and the session closed
- SIGINT / SIGTERM: stop accepting, finish in-flight statements, close sessions,
  checkpoint every open table, exit 0
- the server listens on `127.0.0.1:<port>` unless `--bind` says otherwise
- each newline-terminated request is treated as one SQL statement
- each response ends with `END\n`
- successful responses begin with `OK\n`
- failures begin with `ERR\t<message>\n`
- sending `.quit` returns `BYE\nEND\n` and closes that client session

Example request / response:

```text
client> SELECT c0, c1 FROM '/tmp/demo' WHERE c0 = 2
server> OK
server> c0\tc1
server> 2\t20
server> END
```

Flush notes:
- each table also maintains a WAL sidecar at `<table>.mdb.wal`
- inserts and deletes are written to WAL before base-file mutation
- `./mdb flush <table>` forces WAL sync + base-file checkpoint + WAL truncation
- the command accepts either a base path like `/tmp/demo` or `/tmp/demo.mdb`

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
