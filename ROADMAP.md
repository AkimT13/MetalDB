# MetalDB — Roadmap

Current state: GPU-accelerated column-store. Supports UINT32 + STRING columns, insert/delete,
equality/range/compound WHERE (AND/OR), groupby, hash join, persistence, and a small one-shot
mini-SQL CLI query surface plus a thin interactive REPL. Single-threaded, with C++, C, and
internal Python (`ctypes`) APIs.

---

## Correctness / Durability

### WAL + Group Commit
Replace per-insert `fsync` with a sequential write-ahead log. One `fsync` per commit batch.
Enables atomic multi-row writes and crash recovery. Foundational for everything else.

### MVCC (Multi-Version Concurrency Control)
Keep old row versions instead of tombstoning. Readers never block writers. Required for snapshot
isolation. Adds a version chain to RowIndex. High complexity.

### UPDATE support
Currently impossible. Implement as copy-on-write (delete + insert) logged atomically in WAL.
Fits the column store model without in-place mutation.

---

## Performance

### Column Compression
Per-column encoding strategies:
- Run-length encoding for low-cardinality columns (status flags, enums)
- Delta encoding for monotonic columns (timestamps, auto-increment IDs)
- Dictionary encoding for string columns

Fewer bytes read from disk = faster scans even without GPU.

### Vectorized CPU Execution (SIMD)
Use `arm_neon.h` intrinsics in CPU fallback paths. Process 4×uint32 or 8×uint16 per instruction.
Closes performance gap between CPU and GPU for datasets below `gpuThreshold_`.

### GPU Hash Join
Current join is CPU hash join. GPU equi-join (build hashtable in shared memory, probe in parallel)
could be 10–50× faster for large tables.

### Zone Map Statistics / Histograms
Extend per-page zone maps beyond min/max. Add HyperLogLog sketches or histograms for better
query planning and intermediate result size estimation.

---

## Usability / Interface

*Current focus area. See ordering rationale below.*

### C API  [DONE]
A thin C header (`mdb.h`) wrapping the Engine C++ class. Enables all other usability features.
No parser required — raw insert/scan/groupby calls over a stable ABI.

### Python / C Bindings  [DONE, internal API]
`pybind11` or `ctypes` wrapper around the C API. Makes MetalDB usable from data science
workflows (pandas interop, Jupyter notebooks) without a network round-trip or query parser.

### Query Language (mini-SQL)  [DONE, v1 CLI SURFACE]
Parser for `SELECT col FROM table WHERE ... GROUP BY ...`. No subqueries or CTEs needed to
cover 80% of analytical queries. Makes the engine usable without writing C++.

### Networking / Server Mode  [NEXT]
TCP server with a line protocol (or Postgres wire protocol subset). Any Postgres client, `psql`,
JDBC, or `psycopg2` can connect without compiling C++. Requires a query language to be useful.

---

## Schema / Types

### NULL support
Null bitmap per page (1 bit per slot, separate from tombstone). Currently every slot either has
a value or is deleted — no "present but unknown."

### Additional column types
`INT64`, `FLOAT`, `DOUBLE`, `BOOL`, `TIMESTAMP` (u64 unix µs), `FIXED_STRING(N)`.
Infrastructure exists (ColType enum, ColValue). Gap: GPU kernels only handle UINT32 today.

### Schema evolution (ALTER TABLE)
`ADD COLUMN` (append new ColumnFile, backfill default) and `DROP COLUMN` (mark inactive in
MasterPage). Required for evolving data models without full table rebuilds.

---

## Analytics

### Window Functions
`RANK()`, `ROW_NUMBER()`, `LAG()`/`LEAD()` over ordered partitions. Requires a sort kernel.
High complexity, high value for time-series analytics.

### Materialized Views
Pre-compute GROUP BY results, update incrementally on insert. Reads become O(1).

### Approximate Query Processing
Sample-based `COUNT DISTINCT` (HyperLogLog) or `PERCENTILE` (t-digest). Trades exactness
for ~100× speed on 10M+ row scans.

---

## Operations

### String Heap Compaction
Heap files are append-only forever. A compaction pass rewrites only live strings, reclaiming
orphaned bytes from deleted rows. Required before STRING is safe for long-running workloads.

### Backup / Point-in-Time Restore
Copy data files + WAL to backup location. Replay WAL from a checkpoint to a target LSN.
Straightforward once WAL exists.

### CLI / REPL  [DONE, v1]
Interactive `mdb repl` now exists as a thin shell over mini-SQL execution:
- prompt + continuation prompt
- `;`-terminated statements
- `.help`, `.quit`

Still intentionally missing: history, editing, catalog/schema introspection, and pretty table
rendering.

---

## Usability Ordering Rationale

```
C API  →  Python bindings  →  mini-SQL  →  Networking
```

**C API first**: Python bindings, networking, and a CLI all need a stable non-C++ interface.
A C header is the minimal common foundation — a few hours of work that unlocks three downstream
features. Without it, each consumer (Python, server, CLI) would need its own ad-hoc bridge.

**Python bindings second**: Immediately makes MetalDB usable from Jupyter / pandas without any
query language. Data exploration can start before SQL is written. Also validates that the C API
is complete enough to be useful. This is now in place as an internal `ctypes` wrapper.

**mini-SQL third**: Once the engine is callable from Python, building a query parser on top of
the C API (or directly in C++) gives a human-readable interface. The parser targets the Engine
operations already implemented — no new execution engine needed.

**Networking last**: A server without a query language just exposes raw RPC calls (not useful).
A server with SQL becomes a full database server. Postgres wire protocol compatibility is
the highest-leverage networking choice — it's free client support.
