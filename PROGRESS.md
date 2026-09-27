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

### Phase 6 — Interactive REPL (complete, thin layer over mini-SQL)
Added an interactive mode to the CLI:

- `mdb repl`

The REPL intentionally reuses the same `executeMiniSQL()` path as `mdb query`, so query
semantics stay aligned and there is no second execution path to maintain. Supported REPL
behavior:

- `mdb> ` primary prompt and `...> ` continuation prompt
- `;`-terminated statements, including multiline queries
- `.help` and `.quit`
- tab-separated result printing with the same header/row format as one-shot queries

Coverage:

- REPL smoke coverage in `test_mini_sql`
- multiline statement execution through `./mdb repl` in the same test

Verified:

- `make -C src fast TEST=test_mini_sql`
- `make -C src run`

---

### Phase 7 — TCP Server Mode (complete, minimal line protocol)
Added a network entrypoint to the CLI:

- `mdb serve <port>`

The server is intentionally small and reuses the same `executeMiniSQL()` path used by
`mdb query` and `mdb repl`. Current behavior:

- listens on `127.0.0.1:<port>`
- handles one request per line
- keeps each client connection open for multiple sequential requests
- returns explicit framed responses ending in `END`
- accepts `.quit` per connection and replies with `BYE`

Wire format in v1:

- success: `OK` line, tab-separated result body, then `END`
- error: `ERR\t<message>` line, then `END`

Coverage:

- end-to-end TCP integration in `test_server`
- success, aggregate, malformed SQL, empty request, `.quit`, and reconnect behavior
- missing-table query verified to return `ERR` without creating a typo file

Verified:

- `make -C src fast TEST=test_server`

Note: local TCP bind/connect verification required running this test outside the default
sandbox on this machine.

---

### Phase 8 — WAL + Group Commit (complete, v1 per-table durability)

Added a per-table WAL sidecar at `<table>.mdb.wal` and moved the durable checkpoint boundary
up to the table layer.

Current v1 behavior:

- WAL records for `insert` and `delete`, each followed by a commit marker
- recovery on table open with trailing partial or bad-checksum records ignored
- replayed operations checkpointed back into base files, then WAL truncated to header
- low-level per-write `fsync` removed from `ColumnFile` and `RowIndex`
- explicit durable checkpoint via `Table::flushDurable()` / `Engine::flush()`
- public flush surfaces added to:
  - CLI: `mdb flush <table>`
  - C API: `mdb_flush`
  - Python: `Engine.flush(name)`

Coverage:

- dedicated recovery/corruption test in `test_wal`
- CLI flush coverage in `test_mini_sql`
- C API flush/reopen coverage in `test_c_api`
- Python flush/reopen coverage in `python/test_mdb.py`
- server regression still green with WAL-enabled table open/recovery path

Verified:

- `make -C src fast TEST=test_wal`
- `make -C src fast TEST=test_c_api`
- `make -C src fast TEST=test_mini_sql`
- `make -C src test-python`

---

### Phase 9 — Mini-SQL v2: Writes, Range Comparisons, ORDER BY / LIMIT (complete)

Makes the SQL surface (`mdb query`, `mdb repl`, `mdb serve`) usable end-to-end without
dropping to C++/C/Python for table creation or writes.

New statements:

- `CREATE TABLE '<path>' (UINT32, INT64, FLOAT, DOUBLE, STRING ...)` (optional `cN` names)
- `INSERT INTO '<path>' VALUES (...), (...)` — multi-row; literals coerced to column types,
  all rows validated before any write (a bad literal leaves the table unchanged)
- `DELETE FROM '<path>' [WHERE ...]`
- `DESCRIBE '<path>'`

Query additions:

- `<`, `<=`, `>`, `>=` in `WHERE` on UINT32 columns, lowered onto the existing BETWEEN
  path (so GPU range scans still apply). Unsatisfiable ranges such as `c0 < 0` are folded
  away at parse time (AND → empty result, OR → term dropped).
- `ORDER BY` on selected columns / aggregates / 1-based positions, multi-key, `ASC`/`DESC`,
  type-aware (numeric vs. string) comparison
- `LIMIT n [OFFSET m]`
- tokenizer: signed and decimal numeric literals, `''` escape inside string literals;
  integer-only contexts now reject `1.5` instead of silently truncating it

Coverage: new DDL/DML, comparison, ORDER BY/LIMIT and cross-process CLI cases in
`test_mini_sql`.

Verified (CPU path, Metal entry points stubbed): `test_mini_sql`, `test_engine`,
`test_groupby`, `test_join`, `test_compound_where`, `test_where_range`,
`test_persist_pages`, `test_scan_hybrid`, `test_wal`, `test_server`.

---

### Build Hygiene — Portable CPU Build + CI (complete)

- `make cpu` / `make cpu-run` build the engine without Metal (`src/gpu_cpu_stub.cpp`
  stands in for the `.mm` sources) into `src/build-cpu/`, and run 19 test binaries.
  Works on Linux and on Macs without the Metal toolchain.
- GitHub Actions CI (`.github/workflows/ci.yml`): CPU suite on Linux (gcc + clang) and macOS.
- Root `Makefile` now delegates to `src/Makefile` (it referenced deleted sources).
- `.gitignore` no longer ignores `src/tests/test_*` sources — this is why
  `tests/test_string_gpu.cpp` was referenced by the Makefile but never committed.
  Added that test: GPU string equality vs. CPU reference over 60k rows with empty
  strings, prefix needles, multi-byte UTF-8, and deleted rows.
- `test_types` portability fix (`int64_t` vs `long long` overload ambiguity on Linux).
- Restored `Table::rowCount()` so the older `test_table` compiles again.

---

### Atomic Statements + Storage I/O Hardening (complete)

- WAL transactions: records appended with a shared transaction ID are replayed only
  if that ID's commit record follows (`Wal::beginTxn` + txn-scoped `appendInsert` /
  `appendDelete`). The on-disk format is unchanged — single-op records are the
  one-element case, so existing WALs recover exactly as before.
- `Table::applyAtomic(deletes, inserts)`: validate → log group → commit → apply. A
  crash either recovers the whole statement or none of it. Multi-row mini-SQL
  `INSERT` and `DELETE` now use it.
- `Table::validateRow`: schema/type checks before anything is logged
  (`insertTypedRow` previously accepted mismatched `ColValue` types and wrote garbage).
- Synchronous commit: `Table::setSyncCommit` / `Engine::setSyncCommit` fsync the WAL on
  every commit for power-loss durability (default off; `flush` remains the checkpoint).
- Write/read failures in `ColumnFile`, the string heap, `RowIndex`, and `MasterPage` now
  throw instead of printing `perror` and continuing (silent data loss). `RowIndex`
  entries are written with one `pwrite` instead of three `write` calls.

Coverage: `test_atomic` — batch validation, committed-but-unapplied transaction
replay, uncommitted (torn) transaction discard, SQL statements + sync-commit mode.

---

### Mini-SQL v3: Boolean WHERE Expressions on All Types (complete)

Mini-SQL is now split into modules:

| File | Role |
|------|------|
| `SqlAst.hpp` | tokens + AST (`SelectItem`, `WhereExpr`, `ParsedStatement`) |
| `SqlParser.cpp` | lexer + recursive-descent parser |
| `WhereEval.cpp` | WHERE validation, literal coercion, evaluation |
| `MiniSQL.cpp` | statement execution |

WHERE is now a full boolean tree: `AND` / `OR` / `NOT`, parentheses, `!=` / `<>`,
`[NOT] BETWEEN`, `[NOT] IN (...)`, on UINT32, INT64, FLOAT, DOUBLE, and STRING columns.
Evaluation yields sorted row-ID sets: UINT32 `=` / range leaves still go through
`Table::scanPredicate` (zone maps + GPU dispatch), string equality through the GPU
string scan, everything else is a CPU scan, and connectives are intersect / union /
complement. Comparisons are type-correct: INT64 compares exactly against integer
literals (no 53-bit rounding), and fractional or out-of-range literals against UINT32
columns become the right ranges (`c < 2.5` → `c <= 2`, `c = 1.5` → no rows).

Also: several scalar aggregates per query, `COUNT(cN)`, `MIN` / `MAX` on strings,
projections / aggregates fetch only referenced cells (`Table::fetchTypedValue`,
`RowIndex::slotsOf`) instead of whole rows, `LIMIT` without `ORDER BY` stops
materializing early, `-- comments`, and parse errors now name the offending token.

Coverage: `test_sql_where` — 45 WHERE clauses checked against a brute-force reference
over 5k rows of all column types with deletions, run CPU-only and GPU-eligible.

---

### UPDATE (complete)

`UPDATE '<path>' SET cN = literal [, ...] [WHERE ...]`, implemented copy-on-write on
top of `Table::applyAtomic`: matching rows are re-inserted with the new values and the
old versions deleted in one WAL transaction, so a crash never leaves a half-applied
UPDATE. Updated rows receive new row IDs. Values are type-coerced and validated before
anything is logged. Coverage in `test_mini_sql` (multi-column SET, full-table UPDATE,
no-match UPDATE, type / range / duplicate-column errors, reopen consistency).

---

### GROUP BY v2 + DISTINCT + Exact Aggregates (complete)

- `GROUP BY` accepts multiple key columns of any type (STRING included), any number of
  aggregates, and combines with `WHERE`. Grouped columns may appear anywhere in the
  select list; ungrouped plain columns are rejected with a clear error.
- Execution: GPU-capable fast path (single UINT32 key, COUNT/SUM/AVG over UINT32, no
  WHERE → `GroupBy::countByKey` / `sumByKey`); otherwise hash aggregation keyed by a
  self-delimiting encoding of the key tuple, output sorted by typed key comparison.
- `SELECT DISTINCT` (rewritten to GROUP BY over the selected columns).
- Aggregates are exact: integer SUM uses a 128-bit accumulator (previously summed in
  `long double` and printed with 6 significant digits, e.g. `1.23457e+07`); DOUBLE
  output uses 15 significant digits.
- `MiniSQLResult::types` reports each output column's logical type (used by ORDER BY,
  and by the upcoming Postgres wire protocol for column type OIDs).

Coverage: `test_sql_groupby` (20k rows; fast path, string keys + WHERE, INT64 sums
beyond 2^53, multi-key, DISTINCT, empty input) in CPU and GPU dispatch modes; new
GROUP BY cases in `test_mini_sql`.

---

### EXPLAIN + REPL Timing (complete)

`EXPLAIN <statement>` returns the executed plan as rows of text: table size and GPU
availability/threshold, each WHERE leaf's access path (e.g. `range scan [2, 4294967295]
with zone-map page pruning (GPU: 250000 rows)`, `CPU typed scan (DOUBLE)`, complement
steps), rows matched and filter time, aggregation strategy (GPU-capable group-by vs CPU
hash aggregation + group count), LIMIT push-down, sort, output rows, total time.
`EXPLAIN DELETE / UPDATE` evaluates only the WHERE clause and reports how many rows
would be rewritten — nothing is written. Tracing uses a `thread_local` collector so
concurrent sessions never mix plans. REPL gains `.timer on|off`.

---

### CSV Import / Export via COPY (complete)

- `COPY '<table>' FROM '<file>' [WITH HEADER]`: RFC 4180 reader (`Csv.cpp`: quoted
  fields, `""`, embedded CR/LF, CRLF records). Parse + type-coerce everything first,
  insert as one WAL transaction, then `flushDurable()` so large imports do not leave a
  large WAL. Errors name the CSV record number; the table is left unchanged.
- `COPY '<table>' TO '<file>'` and `COPY (SELECT ...) TO '<file>' [WITH HEADER]`:
  writes `<file>.tmp` and renames into place (no half-written exports). DOUBLE / FLOAT
  are written with round-trip precision (%.17g / %.9g) so export → import is bit-exact.

Coverage: `test_copy` — all column types, strings with commas / quotes / newlines /
padding / empty, bit-exact float round trip, CRLF + blank lines, five kinds of bad
files rejected atomically, COPY (SELECT ...), durability across reopen.

---

### Operations Tooling + Storage Correctness Fixes (complete)

New `TableTools` module and CLI commands: `mdb verify`, `mdb stats`, `mdb backup`,
`mdb restore`, `mdb compact` (see docs.md → Operations Tooling). Backups carry a
MANIFEST with sizes and FNV-1a-64 checksums; restore verifies everything before writing.
Compaction rebuilds a table from its live rows (reclaiming deleted slots, row-index
entries, and orphaned STRING heap bytes — the roadmap's "heap compaction" item).

Storage bugs found and fixed while building the verifier:

- **Master-page overwrite at 65536 pages**: new page IDs were `uint16_t(end / pageSize)`
  and silently wrapped to page 0. Now a clear "table is full" error.
- **Free-list space leak**: a full page that regained a slot became the free-list head
  without linking the previous head, so partially filled pages were forgotten. Pages are
  now pushed/popped properly via `nextFreePage` (backward compatible on disk).
- **4 GiB string heap wrap**: offsets are u32; now an explicit error.
- **Unrecoverable committed transactions**: a WAL transaction that committed but then
  failed to apply (e.g. out of page-ID space) could never be replayed, so the table
  could not be reopened. `Table::ensureCapacity` now checks free slots, page-ID budget,
  and heap limits before anything is logged.
- Creating a table over an existing file now truncates it (stale pages used to remain);
  opening a missing table throws instead of creating an empty file; page sizes too
  small for the master page / one slot are rejected up front.
- File-descriptor leaks: `Table` now closes its master fd and `RowIndex` is a move-only
  RAII owner of its fd.

Coverage: `test_tools` — verify on healthy tables, injected zone-map and row-index
corruption detected, compact preserves data and shrinks the heap, backup/restore
round-trip, tampered backup rejected, free-list reuse regression, page-limit
exhaustion leaves a reopenable table, geometry validation.

---

### Concurrent Server (complete)

The server previously handled one client at a time and gave each connection its own
`Engine`, so sessions had independent in-memory caches of the same table files. Now:

- one shared `Engine`; its registry is mutex-protected and every mini-SQL statement
  takes a per-table lock (`Engine::tableMutex`), so statements on one table are
  serialized (reads populate page caches, so they are not lock-free) while different
  tables run in parallel. Metal pipeline init was already double-checked-locked.
- thread per connection with `--max-connections`, `--idle-timeout`, 16 MiB request cap,
  `--bind`, `--sync-commit`, `--verbose`
- `--data-dir` sandbox (`Engine::setDataDir` / `resolveTableBase` / `resolveFile`):
  rejects absolute paths and `..` for table names and COPY files
- SIGPIPE ignored (a client disconnecting mid-response used to kill the server)
- graceful SIGINT/SIGTERM shutdown: drain sessions, `Engine::flushAll()` checkpoint, exit 0
- `Engine::groupCount/groupSum` now honor the table's `setUseGPU` / `setGPUThreshold`

Coverage: `test_server_concurrency` — 4 parallel writers + readers on one table and a
writer on another (no lost rows, table verifies clean), connection limit, oversized
request, graceful shutdown with a live session (exit 0, WAL checkpointed), sandbox path
rejection, idle timeout. Also run under ThreadSanitizer (clean after fixing a
tracker-lifetime race it found in the shutdown path).

---

### PostgreSQL Wire Protocol (complete, simple-query subset)

`mdb pgserve <port>` (`PgWire.cpp`) speaks PostgreSQL protocol v3: SSL/GSS negotiation
(declined), startup, optional cleartext `--password` auth, ParameterStatus /
BackendKeyData, multi-statement simple queries, RowDescription with type OIDs from
`MiniSQLResult::types`, DataRow (text), proper CommandComplete tags, SQLSTATE-coded
ErrorResponse, EmptyQueryResponse, NOTICEs, Terminate. Transaction / SET commands are
no-op shims for driver compatibility; the extended protocol is rejected with `0A000` and
the session resynchronizes at Sync. Shares the concurrent accept loop, limits, sandbox,
and graceful shutdown with `mdb serve` (the server was refactored around a protocol-
agnostic `SessionIO`).

Verified manually with stock `psql` 16 and psycopg2 2.9 (typed values, parameter
quoting, error codes). Automated coverage: `test_pgwire` (raw v3 client: SSL
negotiation, typed RowDescription, tags, error-stops-batch, SQLSTATEs, empty query,
health check, transaction shims, extended-protocol rejection + Sync recovery, password
auth success / failure, Terminate).

---

## Known Issues / Next Work

### Next Logical Steps

- Extended query protocol (Parse / Bind / Execute) so JDBC and psycopg3's default mode work
- Minimal `pg_catalog` views so `psql \d` and BI tools can introspect tables
- Real multi-statement transactions (the WAL already groups operations by transaction ID)

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
