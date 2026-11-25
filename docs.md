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
- SQL from C: `mdb_query(engine, sql)` → `MdbQueryResult*` (`mdb_result_column_count`,
  `mdb_result_row_count`, `mdb_result_column_name`, `mdb_result_column_type`,
  `mdb_result_value`, `mdb_free_result`); exact sums via `mdb_sum64`
- SQL from Python: `Engine.query(sql)` returns a `QueryResult` with `.columns`,
  `.types`, and `.rows` (tuples of int / float / str); `Engine.sum()` is exact 64-bit
- On Linux the portable build produces `src/build-cpu/libmdb.so`, which `mdb.py` loads
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
- `BEGIN` / `COMMIT` / `ROLLBACK` run real transactions (see Transactions above), and
  ReadyForQuery reports idle / in-transaction / failed status, as drivers expect.
  `SET` / `RESET` / `DISCARD` are accepted as no-ops; `SELECT <integer>` answers health checks.
- extended query protocol: prepared statements (named / unnamed), `$n` parameters in
  text or binary (int2/4/8, float4/8, bool, text), text or binary results, Describe
  (ParameterDescription + RowDescription / NoData), row-limited Execute with
  PortalSuspended, Close, and error recovery at Sync — so drivers that bind parameters
  server-side (psycopg 3, JDBC, asyncpg-style clients) work
- server-side cursors: `DECLARE name CURSOR FOR ...`, `FETCH [n | ALL] FROM name`, `MOVE`,
  `CLOSE`; declared cursors can also be fetched as protocol portals (psycopg named cursors)
- catalog introspection (`PgCatalog.cpp`): psql `\dt`, `\dt+`, `\d`, `\d table`, `\d+ table`,
  `\l`, `\dn`; `information_schema.tables` / `.columns`; `version()`, `current_database()`,
  `current_schema()`, `current_user`, `SHOW <setting>`; `pg_type` lookups. Tables are
  discovered from the `.mdb` files under `--data-dir` (or the working directory), including
  subdirectories (`sales/orders`). Columns report Postgres types (`bigint`, `real`,
  `double precision`, `text`) and `NOT NULL`. Other catalog queries return correctly shaped
  empty results, so tools degrade gracefully instead of failing. Names starting with `pg_`
  are reserved for the catalog.
- not supported: `COPY ... STDIN/STDOUT`, TLS, SCRAM / MD5 auth, NULL parameters

Verified against `psql` 16 (including `\bind`), psycopg 3.3 (server-side binding, binary
results, prepared statements, named cursors), and psycopg2 2.9 — see
`python/test_pg_clients.py`, which CI runs.

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
  - column-to-column comparisons `cA <op> cB` (both numeric, or both STRING; integers
    compare exactly across UINT32 / INT64), evaluated as a CPU scan
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
- integer `SUM` is exact (128-bit accumulator); `DOUBLE` / `FLOAT` values print as the
  shortest decimal that round-trips exactly (like PostgreSQL 12+: `19.99`, `0.30000000000000004`)
- optional `ORDER BY key [ASC|DESC] [, ...]` where `key` is a selected column, a selected
  aggregate (`count(*)`, `sum(c1)`, ...) or a 1-based output position
- optional `LIMIT n [OFFSET m]`
- an optional alias after the table (`FROM '/tmp/demo' [AS] d`), and qualified columns
  (`d.c0`, or `name.c0` without an alias)

Joins:
- `SELECT ... FROM a [AS x] [INNER] JOIN b [AS y] ON x.cN = y.cM [AND x.cP = y.cQ ...]
  [JOIN c ...]`, followed by the usual `WHERE` / `GROUP BY` / `ORDER BY` / `LIMIT`
- any number of tables, left-deep; each `ON` compares a column of the table being joined
  with a column of an earlier one. Self-joins work with aliases.
- key types must match in kind: integers with integers (UINT32 = INT64 is fine), FLOAT /
  DOUBLE with each other, STRING with STRING. NaN never matches.
- columns are referenced as `alias.cN` (the alias defaults to the table name). An
  unqualified `cN` is accepted when only one table has that column, and is otherwise
  rejected as ambiguous. `*` and `alias.*` expand in FROM order.
- output headers are qualified: `u.c1`, `count(*)`, `sum(o.c2)`
- `WHERE` conjuncts that only involve one table are pushed into that table's scan, where
  they keep zone maps and the GPU-capable scans; conjuncts that span tables (an `OR`
  across them, `a.c0 < b.c0`) run on joined rows. The join itself is a CPU hash join that
  builds on the joined table, and only the columns the statement reads are materialized.
  `EXPLAIN` shows each scan, the join's build / probe sizes, and the residual filter.
- aggregates, `GROUP BY`, `DISTINCT`, `ORDER BY` on columns that are not selected,
  `COPY (SELECT ... JOIN ...) TO`, transactions (pending rows join), and prepared
  statements all work as on one table
- a join locks each of its tables for the duration of the statement, in a fixed order
- not supported: `LEFT` / `RIGHT` / `FULL` / `CROSS` / `NATURAL` joins, `USING`,
  comma-separated `FROM` lists, and non-equality `ON` conditions (put those in `WHERE`)

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
  renamed into place; floats are written as shortest round-trip decimals, so export →
  import is bit-exact
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

Transactions:
- `BEGIN` / `START TRANSACTION` … `COMMIT` / `END`, and `ROLLBACK` / `ABORT`, in the REPL,
  both servers, the C API (`mdb_query`), and Python (`Engine.transaction()` or `query("BEGIN")`)
- outside a transaction every statement autocommits atomically, as before
- inside one, writes are buffered per session and nothing reaches the table files or WAL
  until `COMMIT`. Statements in the transaction see its own pending changes (WHERE, joins of
  committed and pending rows, GROUP BY, ORDER BY, COPY TO, EXPLAIN). `ROLLBACK` discards them.
- `COMMIT` on one table is a single atomic WAL batch. On several tables, the whole change set
  is first written to a commit journal (`<data-dir>/.mdb_txn_journal`), so a crash mid-commit
  is rolled forward the next time the engine opens a table
- isolation is READ COMMITTED: readers never block and only see committed data. A transaction
  takes a table-level write lock the first time it writes a table and holds it to the end;
  other writers wait up to the lock timeout (10 s) and then fail with
  `could not obtain lock` (SQLSTATE `55P03`)
- after an error inside a transaction, every statement except `COMMIT` / `ROLLBACK` fails with
  `current transaction is aborted` (SQLSTATE `25P02`); `COMMIT` then reports `ROLLBACK`
- not supported: savepoints, and transactional DDL (`CREATE TABLE` takes effect immediately)

Parameters and names:
- `$1`, `$2`, ... placeholders bind values supplied separately (C++ `executeMiniSQL(engine,
  sql, &params)`, Postgres prepared statements). A bound value is an untyped literal: it is
  read as a number or a string according to the column it is compared with or assigned to
- tables can be written as `'path'`, `name`, `"name"`, or `public.name`
- `ORDER BY` may use columns that are not selected (plain projections)

Important limits:
- table references are paths (relative to `--data-dir` in server mode), not catalog objects
- columns are synthetic identifiers `c0`, `c1`, ...
- outer joins, arithmetic expressions, subqueries, and CTEs are not supported
- in aggregate / GROUP BY queries, `ORDER BY` keys must appear in the `SELECT` list
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
