#include "../Engine.hpp"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <stdexcept>

extern "C" bool metalIsAvailable();

static Predicate eqPred(uint16_t colIdx, ValueType value) {
    Predicate p;
    p.colIdx = colIdx;
    p.kind = Predicate::Kind::EQ;
    p.lo = value;
    p.hi = value;
    return p;
}

static Predicate betweenPred(uint16_t colIdx, ValueType lo, ValueType hi) {
    Predicate p;
    p.colIdx = colIdx;
    p.kind = Predicate::Kind::BETWEEN;
    p.lo = lo;
    p.hi = hi;
    return p;
}

static Predicate stringPred(uint16_t colIdx, const char* needle) {
    Predicate p;
    p.colIdx = colIdx;
    p.kind = Predicate::Kind::EQ_STRING;
    p.needle = needle;
    return p;
}

static void assertSorted(const std::vector<uint32_t>& rowIDs) {
    assert(std::is_sorted(rowIDs.begin(), rowIDs.end()));
}

int main() {
    {
        Engine e;
        auto& t = e.createTable("compound_tbl", 3, 4096);
        const bool haveMetal = metalIsAvailable();
        t.setGPUThreshold(0);

        for (uint32_t i = 0; i < 10'000; ++i)
            e.insert("compound_tbl", {i % 100, i, i % 7});

        auto andHits = e.whereAnd("compound_tbl", {eqPred(0, 17), betweenPred(1, 5000, 5999)});
        assertSorted(andHits);
        assert(andHits.size() == 10);
        for (uint32_t rid : andHits) {
            auto row = t.fetchRow(rid);
            assert(row[0].has_value() && *row[0] == 17u);
            assert(row[1].has_value() && *row[1] >= 5000u && *row[1] <= 5999u);
        }

        auto emptyAnd = e.whereAnd("compound_tbl", {eqPred(0, 999), betweenPred(1, 0, 100)});
        assert(emptyAnd.empty());

        auto allRows = e.whereAnd("compound_tbl", {});
        assertSorted(allRows);
        assert(allRows.size() == 10'000);
        assert(allRows.front() == 0);
        assert(allRows.back() == 9'999);

        auto disjointOr = e.whereOr("compound_tbl", {eqPred(0, 1), eqPred(0, 2)});
        assertSorted(disjointOr);
        assert(disjointOr.size() == 200);

        // whereOr with 2 predicates returns sorted; whereEq returns raw GPU order — sort before comparing.
        auto oneSidedOr = e.whereOr("compound_tbl", {eqPred(0, 1), eqPred(0, 999)});
        auto refEq = e.whereEq("compound_tbl", 0, 1);
        std::sort(refEq.begin(), refEq.end());
        assert(oneSidedOr == refEq);

        auto singleAnd = e.whereAnd("compound_tbl", {eqPred(0, 42)});
        auto refEq42 = e.whereEq("compound_tbl", 0, 42);
        std::sort(singleAnd.begin(), singleAnd.end());
        std::sort(refEq42.begin(), refEq42.end());
        assert(singleAnd == refEq42);

        for (uint32_t rid = 0; rid < 10'000; rid += 5)
            t.deleteRow(rid);
        auto postDelete = e.whereAnd("compound_tbl", {eqPred(0, 10)});
        assertSorted(postDelete);
        for (uint32_t rid : postDelete) {
            assert(rid % 5 != 0);
            auto row = t.fetchRow(rid);
            assert(row[0].has_value() && *row[0] == 10u);
        }

        auto t0 = std::chrono::steady_clock::now();
        auto selectiveAnd = e.whereAnd("compound_tbl", {eqPred(0, 3), betweenPred(1, 1000, 9000)});
        auto t1 = std::chrono::steady_clock::now();
        auto worstOr = e.whereOr("compound_tbl", {betweenPred(1, 0, 4999), betweenPred(1, 5000, 9999)});
        auto t2 = std::chrono::steady_clock::now();
        const double andMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
        const double orMs = std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::printf("  compound WHERE benchmark (10k rows, %s path): AND=%.2f ms OR=%.2f ms\n",
                    haveMetal ? "GPU-capable dispatch" : "CPU-only fallback", andMs, orMs);
        assert(!selectiveAnd.empty());
        assert(!worstOr.empty());

        std::remove("compound_tbl.mdb");
        std::remove("compound_tbl.mdb.idx");
    }

    {
        Engine e;
        auto& t = e.createTable("compound_bench", 3, 4096);
        const bool haveMetal = metalIsAvailable();
        t.setGPUThreshold(0);

        constexpr uint32_t N = 100'000;
        for (uint32_t i = 0; i < N; ++i)
            e.insert("compound_bench", {i % 100, i, i % 2});

        auto b0 = std::chrono::steady_clock::now();
        auto singleEq = e.whereEq("compound_bench", 0, 7);
        auto b1 = std::chrono::steady_clock::now();
        auto singleAnd = e.whereAnd("compound_bench", {eqPred(0, 7)});
        auto b2 = std::chrono::steady_clock::now();
        auto selectiveAnd = e.whereAnd("compound_bench", {eqPred(0, 7), betweenPred(1, 20'000, 69'999)});
        auto b3 = std::chrono::steady_clock::now();
        auto worstOr = e.whereOr("compound_bench", {betweenPred(1, 0, 49'999), betweenPred(1, 50'000, 99'999)});
        auto b4 = std::chrono::steady_clock::now();

        // GPU scan order is non-deterministic between calls; sort before set comparison.
        std::sort(singleEq.begin(), singleEq.end());
        std::sort(singleAnd.begin(), singleAnd.end());
        assert(singleEq == singleAnd);
        assert(!selectiveAnd.empty());
        assert(worstOr.size() == N);

        const double eqMs = std::chrono::duration<double, std::milli>(b1 - b0).count();
        const double singleAndMs = std::chrono::duration<double, std::milli>(b2 - b1).count();
        const double andMs = std::chrono::duration<double, std::milli>(b3 - b2).count();
        const double orMs = std::chrono::duration<double, std::milli>(b4 - b3).count();
        std::printf("  compound WHERE benchmark (100k rows, %s path): eq=%.2f ms single-AND=%.2f ms selective-AND=%.2f ms OR=%.2f ms\n",
                    haveMetal ? "GPU-capable dispatch" : "CPU-only fallback",
                    eqMs, singleAndMs, andMs, orMs);

        std::remove("compound_bench.mdb");
        std::remove("compound_bench.mdb.idx");
    }

    {
        Engine e;
        auto& t = e.createTypedTable("compound_typed", {ColType::STRING, ColType::UINT32, ColType::UINT32});
        t.setGPUThreshold(0);

        const char* cities[] = {"SF", "NYC", "LA", "NYC", "SEA", "NYC"};
        const uint32_t ages[] = {29, 31, 22, 45, 38, 27};
        const uint32_t status[] = {0, 1, 1, 0, 1, 0};
        for (size_t i = 0; i < 6; ++i)
            e.insertTyped("compound_typed", {
                ColValue(std::string(cities[i])),
                ColValue(ages[i]),
                ColValue(status[i])
            });

        auto crossTypeAnd = e.whereAnd("compound_typed", {stringPred(0, "NYC"), betweenPred(1, 30, 50)});
        assertSorted(crossTypeAnd);
        assert(crossTypeAnd.size() == 2);
        assert(crossTypeAnd[0] == 1);
        assert(crossTypeAnd[1] == 3);

        auto orHits = e.whereOr("compound_typed", {stringPred(0, "SEA"), eqPred(2, 0)});
        assertSorted(orHits);
        assert(orHits.size() == 4);

        bool lateMismatchThrown = false;
        try {
            (void)e.whereAnd("compound_typed", {stringPred(0, "nobody"), eqPred(0, 1)});
        } catch (const std::invalid_argument&) {
            lateMismatchThrown = true;
        }
        assert(lateMismatchThrown);

        bool mismatchThrown = false;
        try {
            (void)e.whereAnd("compound_typed", {eqPred(0, 1)});
        } catch (const std::invalid_argument&) {
            mismatchThrown = true;
        }
        assert(mismatchThrown);

        bool oobThrown = false;
        try {
            (void)e.whereOr("compound_typed", {stringPred(9, "NYC")});
        } catch (const std::invalid_argument&) {
            oobThrown = true;
        }
        assert(oobThrown);

        bool lateOobThrown = false;
        try {
            (void)e.whereOr("compound_typed", {stringPred(0, "NYC"), stringPred(9, "NYC")});
        } catch (const std::invalid_argument&) {
            lateOobThrown = true;
        }
        assert(lateOobThrown);

        bool badBetweenThrown = false;
        try {
            (void)e.whereAnd("compound_typed", {betweenPred(1, 50, 30)});
        } catch (const std::invalid_argument&) {
            badBetweenThrown = true;
        }
        assert(badBetweenThrown);

        auto emptyOr = e.whereOr("compound_typed", {});
        assert(emptyOr.empty());

        std::remove("compound_typed.mdb");
        std::remove("compound_typed.mdb.idx");
        std::remove("compound_typed.mdb.0.str");
    }

    {
        Engine e;
        auto& t = e.createTypedTable("compound_bench_typed", {ColType::UINT32, ColType::STRING});
        const bool haveMetal = metalIsAvailable();
        t.setGPUThreshold(0);

        constexpr uint32_t N = 100'000;
        for (uint32_t i = 0; i < N; ++i) {
            const char* city = (i % 10 == 0) ? "NYC" : ((i % 3 == 0) ? "SF" : "LA");
            e.insertTyped("compound_bench_typed", {ColValue(i % 100), ColValue(std::string(city))});
        }

        auto c0 = std::chrono::steady_clock::now();
        auto numericAndString = e.whereAnd("compound_bench_typed", {eqPred(0, 42), stringPred(1, "NYC")});
        auto c1 = std::chrono::steady_clock::now();
        auto numericOnly = e.whereEq("compound_bench_typed", 0, 42);
        auto stringOnly = e.whereEqString("compound_bench_typed", 1, "NYC");
        // GPU scans return unsorted IDs; sort before merge-based intersect.
        std::sort(numericOnly.begin(), numericOnly.end());
        std::sort(stringOnly.begin(), stringOnly.end());
        auto manual = Table::intersectRowIDs(numericOnly, stringOnly);
        auto c2 = std::chrono::steady_clock::now();

        assert(numericAndString == manual);

        const double compoundMs = std::chrono::duration<double, std::milli>(c1 - c0).count();
        const double manualMs = std::chrono::duration<double, std::milli>(c2 - c1).count();
        std::printf("  compound WHERE benchmark (100k numeric+string, %s path): whereAnd=%.2f ms manual-intersect=%.2f ms\n",
                    haveMetal ? "GPU-capable dispatch" : "CPU-only fallback", compoundMs, manualMs);

        std::remove("compound_bench_typed.mdb");
        std::remove("compound_bench_typed.mdb.idx");
        std::remove("compound_bench_typed.mdb.1.str");
    }

    {
        Engine e;
        e.createTypedTable("compound_reopen", {ColType::STRING, ColType::UINT32});
        e.insertTyped("compound_reopen", {ColValue(std::string("NYC")), ColValue(35u)});
        e.insertTyped("compound_reopen", {ColValue(std::string("SF")), ColValue(20u)});
    }

    {
        Engine e;
        auto hits = e.whereAnd("compound_reopen", {stringPred(0, "NYC"), betweenPred(1, 30, 40)});
        assert(hits.size() == 1);
        assert(hits[0] == 0);

        std::remove("compound_reopen.mdb");
        std::remove("compound_reopen.mdb.idx");
        std::remove("compound_reopen.mdb.0.str");
    }

    std::puts("test_compound_where: passed");
    return 0;
}
