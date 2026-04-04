# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MetalDB is a GPU-accelerated column-oriented database engine written in C++17 for macOS. It uses Apple's Metal GPU framework for scan/aggregation kernels with automatic CPU fallback for small datasets.

## Build & Test Commands

All commands run from the project root (where the top-level `Makefile` lives). Source files are under `src/`, but the root Makefile also works directly.

```bash
# Build all test binaries
make

# Build and run all tests
make run

# Build and run a single test
make fast TEST=test_engine

# Clean binaries, object files, and all .mdb / .mdb.idx data files
make clean
```

Test binaries: `test_engine`, `test_groupby`, `test_gpu_scan_equals`, `test_gpu_sum`, `test_scan_hybrid`, `test_persist_pages`, `test_where_range`, `test_join`.

## Architecture

### Layer Overview

```
Engine            (public SQL-like API, table registry)
  └─ Table        (per-table operations: insert/fetch/delete/scan)
       ├─ ColumnFile   (on-disk column storage, one file per table)
       ├─ RowIndex     (row→slot mapping, sidecar .mdb.idx file)
       └─ GPU kernels  (gpu_scan_equals, gpu_scan_range, gpu_sum)
```

### Storage Format

Each table produces two files:
- `{name}.mdb` — binary column data. Page 0 is a `MasterPage` (magic `0x4D445042`, page size, column count, per-column free-list head IDs). Subsequent pages are `ColumnPage`s: header with capacity/count/nextFreePage, then `values[]` and `tombstone[]` arrays.
- `{name}.mdb.idx` — row index (`RIDX` magic). Each entry holds a status byte and an array of `uint32_t` slot IDs (`(pageID << 16) | slotIdx`) — one per column. Enables row-oriented access over a column store.

### Hybrid GPU/CPU Execution

`Table` methods like `scanEquals`, `whereBetween`, and `sumColumnHybrid` automatically dispatch to GPU Metal kernels when the column has ≥ threshold rows (default 4096). Use `setUseGPU(bool)` / `setGPUThreshold(size_t)` to control. GPU code is in `.mm` files; Metal shaders are embedded strings compiled at runtime via `MTLDevice newLibraryWithSource`.

### Zone Maps

Each `ColumnPage` tracks `minValue`/`maxValue` for cheap range pruning — page headers are read before data to skip irrelevant pages.

### Key Types

- `ValueType` = `uint32_t` (all columns are currently fixed-width 32-bit unsigned integers)
- `SlotID` = `(pageID << 16) | slotIndex` packed into a `uint32_t`
- `RowID` = row index into the `.idx` file (0-based)

## Source Layout

```
src/
  *.hpp / *.cpp / *.mm   — core engine source
  tests/                 — one .cpp per test binary
  Makefile               — mirrors root Makefile, adds `run-cli` and `mdb` CLI target
metal-cpp/               — header-only Metal C++ bindings (dependency)
docs.md                  — component-level API documentation
```

Metal-cpp headers are expected at `/usr/local/share/metal-cpp` (system install) or the local `metal-cpp/` directory. The build uses `-I/usr/local/share/metal-cpp`.
