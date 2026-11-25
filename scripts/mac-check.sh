#!/usr/bin/env bash
# One-shot verification for Apple Silicon: builds the Metal targets, runs every
# test (continuing past failures), the Python and PostgreSQL-client tests, and the
# GPU benchmark, and writes everything to mac-check-report.md at the repo root.
#
#   make mac-check                     # from the repo root
#   BENCH_MAX_ROWS=4000000 make mac-check
#
# Paste the report (or its summary) back to Claude to get failures fixed.
set -u
cd "$(dirname "$0")/.."
ROOT=$(pwd)
REPORT="$ROOT/mac-check-report.md"
LOGDIR="$ROOT/mac-check-logs"
rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

pass=0
fail=0
failed=()

section() { printf '\n## %s\n\n' "$1" >> "$REPORT"; }

# run <name> <log> <command...>: records PASS/FAIL and the tail of failing output
run() {
    local name=$1 log=$2
    shift 2
    if "$@" > "$log" 2>&1; then
        printf -- '- PASS `%s`\n' "$name" >> "$REPORT"
        pass=$((pass + 1))
    else
        local code=$?
        printf -- '- **FAIL** `%s` (exit %s)\n\n```\n' "$name" "$code" >> "$REPORT"
        tail -n 25 "$log" >> "$REPORT"
        printf '```\n' >> "$REPORT"
        fail=$((fail + 1))
        failed+=("$name")
    fi
}

{
    echo "# MetalDB Mac check"
    echo
    echo "- date: $(date -u '+%Y-%m-%d %H:%M UTC')"
    echo "- commit: $(git rev-parse --short HEAD) ($(git log -1 --format=%s))"
    echo "- macOS: $(sw_vers -productVersion 2>/dev/null || echo unknown)"
    echo "- chip: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
    echo "- GPU: $(system_profiler SPDisplaysDataType 2>/dev/null | awk -F': ' '/Chipset Model/{print $2; exit}')"
    echo "- Xcode: $(xcodebuild -version 2>/dev/null | head -1 || echo not found)"
    echo "- clang: $(clang++ --version 2>/dev/null | head -1)"
} > "$REPORT"

section "Toolchain"
if xcrun -sdk macosx metal --version > "$LOGDIR/metal-version.log" 2>&1; then
    printf -- '- Metal compiler: %s\n' "$(head -1 "$LOGDIR/metal-version.log")" >> "$REPORT"
else
    {
        echo "- **Metal compiler not available**. Xcode 26+ ships it separately; install it with"
        echo "  \`xcodebuild -downloadComponent MetalToolchain\`, then re-run."
    } >> "$REPORT"
fi
if [ ! -d /usr/local/share/metal-cpp ] && [ ! -d "$ROOT/metal-cpp" ]; then
    echo "- **metal-cpp headers not found** at /usr/local/share/metal-cpp or ./metal-cpp" >> "$REPORT"
fi

section "Build"
run "make -C src all" "$LOGDIR/build.log" make -C src all
run "make -C src libmdb.dylib" "$LOGDIR/dylib.log" make -C src libmdb.dylib
run "make -C src bench_gpu" "$LOGDIR/bench-build.log" make -C src bench_gpu

section "Tests (Metal build)"
TESTS=$(make -s --no-print-directory -C src print-tests 2>/dev/null)
if [ -z "$TESTS" ]; then
    echo "- **could not read TESTS from src/Makefile**" >> "$REPORT"
fi
for t in $TESTS; do
    if [ -x "src/$t" ]; then
        run "$t" "$LOGDIR/$t.log" bash -c "cd src && ./$t"
    else
        printf -- '- **FAIL** `%s` (not built; see build log)\n' "$t" >> "$REPORT"
        fail=$((fail + 1))
        failed+=("$t")
    fi
done

section "Python"
run "python/test_mdb.py" "$LOGDIR/python.log" python3 python/test_mdb.py
if python3 -c "import psycopg" 2>/dev/null || python3 -c "import psycopg2" 2>/dev/null; then
    run "python/test_pg_clients.py" "$LOGDIR/pg-clients.log" python3 python/test_pg_clients.py src
else
    echo "- skipped PostgreSQL client tests (\`pip3 install 'psycopg[binary]' psycopg2-binary\` to enable)" >> "$REPORT"
fi
if command -v psql > /dev/null; then
    echo "- psql found: $(psql --version)" >> "$REPORT"
fi

section "GPU benchmark"
if [ -x src/bench_gpu ]; then
    if (cd src && BENCH_MAX_ROWS="${BENCH_MAX_ROWS:-1000000}" ./bench_gpu) > "$LOGDIR/bench.log" 2>&1; then
        pass=$((pass + 1))
    else
        fail=$((fail + 1))
        failed+=("bench_gpu (GPU/CPU mismatch or crash)")
    fi
    cat "$LOGDIR/bench.log" >> "$REPORT"
else
    echo "- bench_gpu not built" >> "$REPORT"
fi

{
    echo
    echo "## Summary"
    echo
    echo "- passed: $pass"
    echo "- failed: $fail"
    for f in "${failed[@]+"${failed[@]}"}"; do echo "  - $f"; done
    echo "- full logs: mac-check-logs/"
} >> "$REPORT"

echo "MetalDB Mac check: $pass passed, $fail failed"
echo "Report: $REPORT"
[ "$fail" -eq 0 ]
