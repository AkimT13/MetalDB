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

### GPU Group By Performance (HIGH PRIORITY)

The current GPU group-by path is **slower than CPU** for all practical inputs. Measured
~10 seconds for 100k rows vs. sub-millisecond on CPU. Root causes:

1. **Runtime shader compilation per call** — `newLibraryWithSource` recompiles the Metal
   kernel string on every `gpuGroupByCountSum` invocation (~8–9s overhead). Fix: compile
   once at startup and cache the `ComputePipelineState` in a singleton context struct.

2. **Device and CommandQueue created per call** — `CreateSystemDefaultDevice()` and
   `newCommandQueue()` are called inside `gpuGroupByCountSum`. Fix: add to the same
   singleton.

3. **High atomic contention for low-cardinality group-by** — with 10 distinct keys and
   100k threads, ~10k threads race on each of 10 buckets. Fix: switch to a two-level
   reduction — each threadgroup builds a private partial table in threadgroup memory, then
   a second pass merges partial tables into the global result.

4. **Data size too small for GPU to win** — 100k × 4 bytes = 400 KB. GPU overhead
   doesn't amortize until data is in the tens of MB (~1–5M rows with cached pipeline).

**Recommended fix sequence:**
```
a) Add GpuGroupByContext singleton (Device + Queue + PSO cached on first use)
b) Implement two-level threadgroup-local reduction kernel
c) Raise default gpuThreshold to 1_000_000 until (a) is done
```

After (a), expected overhead drops from ~10s to ~1ms, making GPU competitive at scale.
After (b), GPU wins at ~500k+ rows for moderate cardinality (100–10k distinct keys).

### 32-bit Sum Overflow in GPU Path
`bucketSums` uses `device atomic_uint` (32-bit). Per-group sums overflow if they exceed
~4.29 billion. Metal does not support `atomic_fetch_add` on `device atomic_ulong` (64-bit)
on current Apple GPUs. Options: use two 32-bit accumulators (hi + lo), or use a
non-atomic reduction pass for sums.

### Deferred: String Column Support
Variable-length storage requires a separate heap file and indirection pointers.
Not started. Recommend doing this after GPU performance is fixed.

### Deferred: GPU Kernels for INT64 / FLOAT / DOUBLE
Current GPU paths (scan_equals, scan_range, sum, group_by) operate on UINT32 only.
Extending to other ColTypes requires kernel variants or a template-like approach in MSL.
