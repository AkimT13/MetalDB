CXX      := clang++
CXXFLAGS := -std=c++17 -arch arm64 -I/usr/local/share/metal-cpp -g -MMD -MP
LDFLAGS  := -framework Metal -framework Foundation

# Core sources (both .cpp and .mm)
SRCS := MasterPage.cpp Column.cpp ColumnFile.cpp RowIndex.cpp Table.cpp \
        gpu_scan_equals.mm gpu_sum.mm gpu_scan_range.mm gpu_groupby.mm \
        Engine.cpp GroupBy.cpp Join.cpp

# Object files (+ auto-generated dependency files)
OBJS := $(SRCS:.cpp=.o)
OBJS := $(OBJS:.mm=.o)
DEPS := $(OBJS:.o=.d)

# Tests
TESTS := test_gpu_scan_equals test_gpu_sum test_scan_hybrid test_persist_pages test_where_range \
         test_engine test_groupby test_join test_types

all: $(TESTS)

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

# Quick single-test runner: make fast TEST=test_gpu_sum
fast: $(TESTS)
	@if [ -z "$(TEST)" ]; then echo "Usage: make fast TEST=<binary>"; exit 1; fi
	./$(TEST)

clean:
	rm -f $(OBJS) $(DEPS) $(TESTS) mdb mdb.o
	rm -f /tmp/table_* /tmp/demo.mdb /tmp/demo.mdb.idx *.mdb *.mdb.idx

# Include dependency files (safe if missing)
-include $(DEPS)