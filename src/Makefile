CXX      := clang++
CC       := clang
CXXFLAGS := -std=c++17 -arch arm64 -I/usr/local/share/metal-cpp -I../metal-cpp -g -MMD -MP
LDFLAGS  := -framework Metal -framework Foundation

# Metal shader compilation
METAL_SDK   := macosx
METAL_FLAGS := -std=metal3.0

METAL_SRCS    := gpu_scan_equals.metal gpu_scan_range.metal gpu_sum.metal gpu_groupby.metal gpu_string_scan.metal
METALLIB_SRCS := $(METAL_SRCS:.metal=.metallib)

%.air: %.metal
	xcrun -sdk $(METAL_SDK) metal $(METAL_FLAGS) -c $< -o $@

%.metallib: %.air
	xcrun -sdk $(METAL_SDK) metallib $< -o $@

# Core sources (both .cpp and .mm)
SRCS := MasterPage.cpp ColumnFile.cpp RowIndex.cpp Table.cpp \
        gpu_scan_equals.mm gpu_sum.mm gpu_scan_range.mm gpu_groupby.mm gpu_string_scan.mm \
        Engine.cpp GroupBy.cpp Join.cpp MiniSQL.cpp SqlParser.cpp WhereEval.cpp Csv.cpp TableTools.cpp QuerySession.cpp \
        Server.cpp PgWire.cpp Wal.cpp mdb_c.cpp

# Object files (+ auto-generated dependency files)
OBJS := $(SRCS:.cpp=.o)
OBJS := $(OBJS:.mm=.o)
DEPS := $(OBJS:.o=.d)

# Tests
TESTS := test_gpu_scan_equals test_gpu_sum test_scan_hybrid test_persist_pages test_where_range \
         test_engine test_groupby test_join test_types test_string_gpu test_compound_where \
         test_c_api test_mini_sql test_server test_wal test_atomic test_sql_where \
         test_sql_groupby test_copy test_tools test_server_concurrency \
         test_pgwire test_gpu_groupby

all: $(METALLIB_SRCS) $(TESTS) libmdb.a

# Compile rules
%.o: %.mm
	$(CXX) $(CXXFLAGS) -c $< -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ---- CLI ----
# I link mdb against the core object files so the CLI can use the same code paths.
mdb: $(OBJS) mdb.o
	$(CXX) $(CXXFLAGS) $(OBJS) mdb.o -o $@ $(LDFLAGS)

mdb.o: mdb.cpp
	$(CXX) $(CXXFLAGS) -c mdb.cpp -o mdb.o

# Optional helper to try a quick flow
.PHONY: run-cli
run-cli: mdb
	./mdb create /tmp/demo.mdb 4096 2
	./mdb insert /tmp/demo.mdb 2 10
	./mdb insert /tmp/demo.mdb 3 20
	./mdb insert /tmp/demo.mdb 2 30
	./mdb select-eq /tmp/demo.mdb 0 2
	./mdb select-between /tmp/demo.mdb 0 2 3
	./mdb sum /tmp/demo.mdb 1

# ---- Tests ----
test_gpu_scan_equals: $(OBJS) tests/test_gpu_scan_equals.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_gpu_sum: $(OBJS) tests/test_gpu_sum.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_scan_hybrid: $(OBJS) tests/test_scan_hybrid.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_persist_pages: $(OBJS) tests/test_persist_pages.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_where_range: $(OBJS) tests/test_where_range.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_engine: $(OBJS) tests/test_engine.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_groupby: $(OBJS) tests/test_groupby.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_join: $(OBJS) tests/test_join.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_types: $(OBJS) tests/test_types.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_string_gpu: $(OBJS) tests/test_string_gpu.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_compound_where: $(OBJS) tests/test_compound_where.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_mini_sql: $(OBJS) tests/test_mini_sql.cpp mdb
	$(CXX) $(CXXFLAGS) $(OBJS) tests/test_mini_sql.cpp -o $@ $(LDFLAGS)

test_server: $(OBJS) tests/test_server.cpp mdb
	$(CXX) $(CXXFLAGS) $(OBJS) tests/test_server.cpp -o $@ $(LDFLAGS)

test_wal: $(OBJS) tests/test_wal.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_atomic: $(OBJS) tests/test_atomic.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_sql_where: $(OBJS) tests/test_sql_where.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_sql_groupby: $(OBJS) tests/test_sql_groupby.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_copy: $(OBJS) tests/test_copy.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_tools: $(OBJS) tests/test_tools.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

test_server_concurrency: $(OBJS) tests/test_server_concurrency.cpp mdb
	$(CXX) $(CXXFLAGS) $(OBJS) tests/test_server_concurrency.cpp -o $@ $(LDFLAGS)

test_pgwire: $(OBJS) tests/test_pgwire.cpp mdb
	$(CXX) $(CXXFLAGS) $(OBJS) tests/test_pgwire.cpp -o $@ $(LDFLAGS)

test_gpu_groupby: $(OBJS) tests/test_gpu_groupby.cpp
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

# ---- C API library + C test ----
libmdb.a: $(OBJS)
	ar rcs $@ $^

libmdb.dylib: $(OBJS)
	$(CXX) -dynamiclib $(OBJS) -framework Metal -framework Foundation -lc++ -o $@

test_c_api: libmdb.a tests/test_c_api.c
	$(CC) -std=c11 -arch arm64 -I. tests/test_c_api.c -L. -lmdb \
	    -framework Metal -framework Foundation -lc++ -o $@

.PHONY: test-python
test-python: libmdb.dylib
	python3 ../python/test_mdb.py

.PHONY: run clean fast

# Run the full suite
run: all
	./test_gpu_scan_equals
	./test_gpu_sum
	./test_scan_hybrid
	./test_persist_pages
	./test_where_range
	./test_engine
	./test_groupby
	./test_join
	./test_types
	./test_string_gpu
	./test_compound_where
	./test_c_api
	./test_mini_sql
	./test_server
	./test_wal
	./test_atomic
	./test_sql_where
	./test_sql_groupby
	./test_copy
	./test_tools
	./test_server_concurrency
	./test_pgwire
	./test_gpu_groupby

# Quick single-test runner: make fast TEST=test_gpu_sum
fast: $(TESTS)
	@if [ -z "$(TEST)" ]; then echo "Usage: make fast TEST=<binary>"; exit 1; fi
	./$(TEST)

clean: cpu-clean
	rm -f $(OBJS) $(DEPS) $(TESTS) mdb mdb.o libmdb.a libmdb.dylib
	rm -f $(METALLIB_SRCS) $(METAL_SRCS:.metal=.air)
	rm -f /tmp/table_* /tmp/demo.mdb /tmp/demo.mdb.idx /tmp/demo.mdb.wal *.mdb *.mdb.idx *.wal *.str
	rm -f /tmp/c_*.mdb /tmp/c_*.mdb.idx /tmp/c_*.mdb.wal /tmp/c_*.str
	rm -f /tmp/py_*.mdb /tmp/py_*.mdb.idx /tmp/py_*.mdb.wal /tmp/py_*.str
	rm -f /tmp/sql_*.mdb /tmp/sql_*.mdb.idx /tmp/sql_*.mdb.wal /tmp/sql_*.str
	rm -f /tmp/wal_*.mdb /tmp/wal_*.mdb.idx /tmp/wal_*.str /tmp/wal_*.wal

# ---- Portable CPU-only build ----------------------------------------------------
# Builds the engine without Metal (gpu_cpu_stub.cpp replaces the *.mm sources) so it
# compiles and tests on Linux, CI runners, and Macs without the Metal toolchain.
# Every hybrid path takes its CPU branch. Output goes to build-cpu/.
#
#   make cpu        # build CLI + CPU test suite
#   make cpu-run    # build and run the CPU test suite
CPU_DIR      := build-cpu
CPU_CXX      ?= c++
CPU_CC       ?= cc
CPU_CXXFLAGS := -std=c++17 -O1 -g -Wall -fPIC -I. -MMD -MP
CPU_CORE     := MasterPage ColumnFile RowIndex Table Engine GroupBy Join MiniSQL SqlParser WhereEval Csv TableTools QuerySession \
                Server PgWire Wal mdb_c gpu_cpu_stub
CPU_OBJS     := $(addprefix $(CPU_DIR)/,$(addsuffix .o,$(CPU_CORE)))
CPU_TESTS    := test_scan_hybrid test_persist_pages test_where_range test_engine test_groupby \
                test_join test_types test_string_gpu test_compound_where test_mini_sql \
                test_server test_wal test_atomic test_sql_where test_sql_groupby test_copy test_tools test_server_concurrency test_pgwire test_gpu_groupby test_multipage test_persistence test_reuse test_scan_sum \
                test_table test_table_persist test_c_api
CPU_BINS     := $(addprefix $(CPU_DIR)/,$(CPU_TESTS)) $(CPU_DIR)/mdb $(CPU_DIR)/libmdb.so

$(CPU_DIR):
	mkdir -p $(CPU_DIR)

$(CPU_DIR)/%.o: %.cpp | $(CPU_DIR)
	$(CPU_CXX) $(CPU_CXXFLAGS) -c $< -o $@

$(CPU_DIR)/mdb: $(CPU_OBJS) $(CPU_DIR)/mdb.o
	$(CPU_CXX) $^ -o $@ -lpthread

$(CPU_DIR)/libmdb.so: $(CPU_OBJS)
	$(CPU_CXX) -shared $(CPU_OBJS) -o $@ -lpthread

$(CPU_DIR)/test_c_api: $(CPU_OBJS) tests/test_c_api.c | $(CPU_DIR)
	$(CPU_CC) -std=c11 -g -I. -c tests/test_c_api.c -o $(CPU_DIR)/test_c_api.o
	$(CPU_CXX) $(CPU_OBJS) $(CPU_DIR)/test_c_api.o -o $@ -lpthread

$(CPU_DIR)/test_%: tests/test_%.cpp $(CPU_OBJS) | $(CPU_DIR)
	$(CPU_CXX) $(CPU_CXXFLAGS) $< $(CPU_OBJS) -o $@ -lpthread

.PHONY: cpu cpu-run cpu-clean
cpu: $(CPU_BINS)

# Tests run from inside build-cpu/ because some of them invoke ./mdb.
cpu-run: cpu
	@set -e; cd $(CPU_DIR); for t in $(CPU_TESTS); do \
	    echo "== $$t"; ./$$t; \
	done; \
	if command -v python3 >/dev/null; then echo "== python bindings"; python3 ../../python/test_mdb.py; \
	    echo "== postgres clients"; python3 ../../python/test_pg_clients.py .; fi; \
	echo "CPU suite: all $(words $(CPU_TESTS)) tests passed"

cpu-clean:
	rm -rf $(CPU_DIR)

-include $(wildcard $(CPU_DIR)/*.d)

# Include dependency files (safe if missing)
-include $(DEPS)
