CXX      := clang++
CC       := clang
CXXFLAGS := -std=c++17 -arch arm64 -I/usr/local/share/metal-cpp -g -MMD -MP
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
        Engine.cpp GroupBy.cpp Join.cpp MiniSQL.cpp QuerySession.cpp Server.cpp Wal.cpp mdb_c.cpp

# Object files (+ auto-generated dependency files)
OBJS := $(SRCS:.cpp=.o)
OBJS := $(OBJS:.mm=.o)
DEPS := $(OBJS:.o=.d)

# Tests
TESTS := test_gpu_scan_equals test_gpu_sum test_scan_hybrid test_persist_pages test_where_range \
         test_engine test_groupby test_join test_types test_string_gpu test_compound_where \
         test_c_api test_mini_sql test_server test_wal

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

# Quick single-test runner: make fast TEST=test_gpu_sum
fast: $(TESTS)
	@if [ -z "$(TEST)" ]; then echo "Usage: make fast TEST=<binary>"; exit 1; fi
	./$(TEST)

clean:
	rm -f $(OBJS) $(DEPS) $(TESTS) mdb mdb.o libmdb.a libmdb.dylib
	rm -f $(METALLIB_SRCS) $(METAL_SRCS:.metal=.air)
	rm -f /tmp/table_* /tmp/demo.mdb /tmp/demo.mdb.idx *.mdb *.mdb.idx *.str
	rm -f /tmp/c_*.mdb /tmp/c_*.mdb.idx /tmp/c_*.str
	rm -f /tmp/py_*.mdb /tmp/py_*.mdb.idx /tmp/py_*.str
	rm -f /tmp/sql_*.mdb /tmp/sql_*.mdb.idx /tmp/sql_*.str
	rm -f /tmp/wal_*.mdb /tmp/wal_*.mdb.idx /tmp/wal_*.str /tmp/wal_*.wal

# Include dependency files (safe if missing)
-include $(DEPS)
