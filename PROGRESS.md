# MetalDB — Progress Log

## Completed

### Phase 1 — Engine API (complete)
Exposed existing `GroupBy` and `Join` operations through `Engine`'s public facade.
Added aggregations that were missing from `GroupBy`:

- `GroupBy::avgByKey(t, keyCol, valCol)`
- `GroupBy::minByKey(t, keyCol, valCol)`
- `GroupBy::maxByKey(t, keyCol, valCol)`

Added zone-map-based aggregations to `Table`:

- `Table::minColumn(colIdx)` — reads page headers only, O(unique pages)
- `Table::maxColumn(colIdx)` — same

New `Engine` methods: `groupCount`, `groupSum`, `groupAvg`, `groupMin`, `groupMax`,
`minColumn`, `maxColumn`, `join`.

Tests: `test_engine`, `test_groupby`, `test_join` all pass.

---

### Phase 2 — Typed Column Storage (complete)
Introduced `ColType` enum and `ColValue` tagged union in `ValueTypes.hpp`.

Supported types:

| ColType | Storage | C++ type |
|---------|---------|----------|
| UINT32  | 4 bytes | uint32_t |
| INT64   | 8 bytes | int64_t  |
| FLOAT   | 4 bytes | float    |
| DOUBLE  | 8 bytes | double   |

`MasterPage` now persists a `colTypes[]` array on disk (backward compatible — old files
default all columns to UINT32). `ColumnPage` uses raw byte storage
(`vector<uint8_t> rawValues`, capacity × valueBytes). GPU kernels still operate only on
UINT32/FLOAT; other types fall back to CPU automatically.

New API: `Engine::createTypedTable`, `Engine::insertTyped`,
`Table::insertTypedRow`, `Table::fetchTypedRow`.

Tests: `test_types` passes.

---

### Phase 3 — GPU Group By (complete, known performance limitations)
Single-pass GPU group-by kernel (`src/gpu_groupby.mm`) using Metal device-space atomics
and an open-addressing hash table. `GroupBy::countByKey` and `sumByKey` dispatch to GPU
when `n >= gpuThreshold && metalIsAvailable()`, with automatic CPU fallback.

Fixed a latent correctness bug: `RowIndex::openOrCreate(bool create)` now truncates the
`.idx` file when `create=true`, preventing stale rows from previous runs accumulating.

Tests: `test_groupby` passes (correctness verified, CPU and GPU results agree).

---

### Phase 4 — C API + Python Bindings (complete, internal developer API)
Added a pure C wrapper in `src/mdb.h` / `src/mdb_c.cpp` covering table creation,
typed insert/fetch/delete, equality/range scans, compound `WHERE`, aggregations,
group-by, and join. The wrapper now validates caller input before reaching
assert-backed engine internals and clears stale `lastError` state after successful calls.

Added an internal Python API in `python/mdb.py` using `ctypes` over `libmdb.dylib`.
This exposes:

- `Engine`
- `Predicate`
- type constants `UINT32`, `INT64`, `FLOAT`, `DOUBLE`, `STRING`
- `MdbError`

Python-side validation now rejects invalid schema definitions, bad predicate objects,
negative / oversized column indexes, closed-engine usage, and obvious value/type
mismatches before crossing the C boundary. UTF-8 string round-trip and compound
predicate paths are covered in `python/test_mdb.py`.

Build/test support:

- `make -C src test_c_api`
- `make -C src test-python`

Verified:

- `make -C src test-python`
- `make -C src run`

Design choice for now: keep the Python layer as an in-repo internal developer API.
Packaging (`pyproject.toml`, wheels, install-time dylib handling) is explicitly deferred
until the API surface stabilizes.

---

### Phase 5 — Mini-SQL v1 CLI Query Surface (complete, intentionally small)
Added a one-shot query path to `mdb`:

- `mdb query "<sql>"`

The new mini-SQL layer is implemented in `src/MiniSQL.cpp` and routes directly into the
current engine rather than introducing a planner. Supported v1 features:

- `SELECT <cols>` and `SELECT *`
- `FROM '<table-base-path>'`
- flat `WHERE` with all-`AND` or all-`OR`
- numeric `=` and `BETWEEN`
- string equality
- scalar `COUNT(*)`, `SUM`, `MIN`, `MAX`, `AVG`
- `GROUP BY cN` with exactly one aggregate expression

Intentional v1 restrictions:

- synthetic column identifiers only (`c0`, `c1`, ...)
- mixed `AND` / `OR` in a single `WHERE` is rejected
- no joins, aliases, `ORDER BY`, `LIMIT`, parentheses, subqueries, or CTEs
- `GROUP BY ... WHERE ...` is rejected for now
- grouped queries are limited to the existing UINT32 group-by paths

Result printing is tab-separated with a header row. Missing tables now fail cleanly in
the query path instead of creating typo files through `Engine::openTable()`.

Coverage:

- parser / executor / aggregate / group-by tests in `test_mini_sql`
- one end-to-end `./mdb query ...` assertion in the same test

Verified:

- `make -C src fast TEST=test_mini_sql`
- `make -C src test-python`
- `make -C src run`

---

## Known Issues / Next Work

### Next Logical Usability Step — REPL Polish Or Server Mode

The one-shot mini-SQL query surface is now in place. The next usability decision is whether to:

- add an interactive REPL on top of `mdb query` for local exploration, or
- move up the roadmap to server mode now that a query language exists

The engine substrate no longer blocks either direction.

### GPU Group By Performance

The GPU group-by path has been substantially improved. Measured on `test_groupby`
(100k rows, 10 distinct keys):

| State | Cold | Hot |
|-------|------|-----|
| Before fixes | ~10s | ~10s |
| After fixes (a) + (b) + (c) | ~105ms | ~58ms |

Root causes and fixes applied:

**a) GpuGroupByContext singleton — DONE**
`MTL::Device`, `MTL::CommandQueue`, and `MTL::ComputePipelineState` are now cached on
first use. Metal shaders are pre-compiled to `.metallib` at build time. Previously
`newLibraryWithSource` recompiled the kernel string on every call (~8–9s overhead).

**b) Two-level threadgroup reduction kernel — DONE**
Replaced the single-pass global-atomic kernel in `src/gpu_groupby.metal` with a
two-level reduction:
- Phase 1: each threadgroup initializes a private 256-slot hash table in threadgroup
  memory (~12 KB).
- Phase 2: threads insert into the threadgroup-local table (low contention); falls back
  to direct global insert on overflow (high-cardinality edge case).
- Phase 3: one barrier, then each thread merges its slice of the threadgroup table into
  the global table.

For 100k rows / 10 keys this reduces global atomic operations from ~100k to ~3.9k.

**c) ColumnPage copy elimination — DONE**
Added `ColumnFile::pageRef(uint16_t pid) const` returning `const ColumnPage&` into the
in-memory cache. `fetchTypedSlot` now uses this reference instead of returning a full
`ColumnPage` copy. Previously each per-slot fetch allocated and copied a ~4 KB page;
with 100k rows this produced ~400 MB of heap churn (~1.8s per materialize call).
Fixed in `src/ColumnFile.hpp` and `src/ColumnFile.cpp`.

### materializeColumnWithRowIDs — Page Cache Optimization (DONE)

Root cause was O(rows) `unordered_map` lookups into the page cache even after warm-up.

**Fix applied:** Cache the last-accessed `ColumnPage*` in `materializeColumnWithRowIDs`.
Consecutive rows inserted sequentially share the same page, so the cached pointer is
reused for ~capacity rows before a new `pageRef()` hash-map call is needed. This reduces
hash-map lookups from O(rows) → O(pages) (~25 vs 100k for the test case).

`ColumnFile::pageRef` was promoted to the public API to enable this (and future callers).

Measured on `test_groupby` (100k rows, 10 keys):

| State | Cold | Hot |
|-------|------|-----|
| Before (phase c) | ~105ms | ~58ms |
| After page-cache optimization | ~69ms | ~35ms |

### 32-bit Sum Overflow in GPU Path
`bucketSums` uses `device atomic_uint` (32-bit). Per-group sums overflow if they exceed
~4.29 billion. Metal does not support `atomic_fetch_add` on `device atomic_ulong` (64-bit)
on current Apple GPUs. Options: use two 32-bit accumulators (hi + lo), or use a
non-atomic reduction pass for sums.

### Deferred: String Column Support
Variable-length storage requires a separate heap file and indirection pointers.
Not started. Recommend doing this after GPU performance work is complete.

### Deferred: GPU Kernels for INT64 / FLOAT / DOUBLE
Current GPU paths (scan_equals, scan_range, sum, group_by) operate on UINT32 only.
Extending to other ColTypes requires kernel variants or a template-like approach in MSL.
