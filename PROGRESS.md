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

## Known Issues / Next Work

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

### Remaining Bottleneck: materializeColumnWithRowIDs (NEXT PRIORITY)

The hot path at 58ms is now dominated by `materializeColumnWithRowIDs` (~18ms per call,
3 calls × 18ms = 54ms). The GPU kernel itself runs in <2ms once data is loaded.

Root cause: the current implementation iterates the row index and calls `fetchTypedSlot`
per slot, which does a hash-map lookup into the page cache on every row.

**Next fix — Page-scan materialization optimization:**
Instead of row-index iteration + per-slot `fetchTypedSlot`, scan pages directly in
sequential order and copy contiguous value arrays in bulk. Expected improvement:
~18ms → ~2ms per call (~10x speedup for materialization), which would bring the
100k-row hot path from ~58ms to ~10ms or less.

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
