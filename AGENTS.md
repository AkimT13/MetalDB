# Repository Guidelines

## Project Structure & Module Organization
Core engine code lives in [`src/`](/Users/akimtarasov/Desktop/Development/MetalDB/src): storage and execution classes are in `*.hpp`, `*.cpp`, and `*.mm`, and Metal shaders are in `*.metal`. Tests live in [`src/tests/`](/Users/akimtarasov/Desktop/Development/MetalDB/src/tests), typically one `test_*.cpp` per binary. The CLI entrypoint is [`src/mdb.cpp`](/Users/akimtarasov/Desktop/Development/MetalDB/src/mdb.cpp). Reference docs are in [`docs.md`](/Users/akimtarasov/Desktop/Development/MetalDB/docs.md) and progress notes in [`PROGRESS.md`](/Users/akimtarasov/Desktop/Development/MetalDB/PROGRESS.md). Vendor Metal headers are checked in under [`metal-cpp/`](/Users/akimtarasov/Desktop/Development/MetalDB/metal-cpp).

## Build, Test, and Development Commands
Prefer the maintained makefile in `src/`:

```bash
make -C src              # build shaders, CLI, and test binaries
make -C src run          # run the full test suite
make -C src fast TEST=test_engine
make -C src run-cli      # exercise the demo CLI flow
make -C src clean        # remove binaries, objects, and temp .mdb files
```

The root `Makefile` exists but lags behind current sources; use it only if you first verify its target list.

## Coding Style & Naming Conventions
Use C++17 with existing Apple/Metal conventions. Match the surrounding files: 4-space indentation, braces on the same line, and concise comments only where the control flow is non-obvious. Types and classes use `PascalCase` (`ColumnFile`, `MasterPage`), methods use `camelCase` (`insertTypedRow`), and test binaries/files use `test_*`. Keep GPU host code in `.mm` and shader code in `.metal`.

## Testing Guidelines
Add or update focused regression tests in `src/tests/` for any behavior change. Follow the current naming pattern such as `test_groupby.cpp` or `test_string_gpu.cpp`. Run `make -C src run` before opening a PR; for faster iteration use `make -C src fast TEST=test_join`. No formal coverage gate is checked in, so rely on targeted test additions for new storage, GPU, and persistence paths.

## Commit & Pull Request Guidelines
Recent history favors short, imperative commit subjects, often scoped by feature or phase, for example `Add STRING column support via per-column heap file` or `Phase 3: GPU group-by kernel...`. Keep commits narrow and descriptive. PRs should explain the user-visible or storage-format impact, list test commands run, and call out any macOS/Metal prerequisites. Include sample output or screenshots only when CLI behavior changes materially.

## Environment & Configuration Notes
This project targets macOS on Apple Silicon with Metal available. The current build expects Metal C++ headers at `/usr/local/share/metal-cpp`; if you use the vendored copy, update include paths consistently rather than mixing both setups.
