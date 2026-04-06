#include "../Engine.hpp"
#include <cassert>
#include <cstdio>
#include <cmath>

int main() {
    Engine e;

    // Table with three typed columns: UINT32, FLOAT, INT64
    e.createTypedTable("typed_tbl", {ColType::UINT32, ColType::FLOAT, ColType::INT64});

    // Insert 100 rows
    for (uint32_t i = 0; i < 100; ++i) {
        e.insertTyped("typed_tbl", {
            ColValue(uint32_t(i)),
            ColValue(float(i) * 0.5f),
            ColValue(int64_t(i) * 1000LL)
        });
    }

    // Fetch row 42 and verify all columns
    {
        auto& t  = e.openTable("typed_tbl");
        auto row = t.fetchTypedRow(42);
        assert(row.size() == 3);
        assert(row[0].has_value() && row[0]->u32 == 42u);
        assert(row[1].has_value() && std::fabs(row[1]->f32 - 21.0f) < 1e-5f);
        assert(row[2].has_value() && row[2]->i64 == 42000LL);
    }

    // Persist and reload
    {
        e.openTable("typed_tbl");  // already open; ensures re-use path

        // Open a fresh Engine instance to force reload from disk
        Engine e2;
        auto& t2 = e2.openTable("typed_tbl");

        auto row = t2.fetchTypedRow(0);
        assert(row.size() == 3);
        assert(row[0].has_value() && row[0]->u32 == 0u);
        assert(row[1].has_value() && std::fabs(row[1]->f32 - 0.0f) < 1e-5f);
        assert(row[2].has_value() && row[2]->i64 == 0LL);

        auto row99 = t2.fetchTypedRow(99);
        assert(row99[0].has_value() && row99[0]->u32 == 99u);
        assert(row99[1].has_value() && std::fabs(row99[1]->f32 - 49.5f) < 1e-4f);
        assert(row99[2].has_value() && row99[2]->i64 == 99000LL);
    }

    std::remove("typed_tbl.mdb");
    std::remove("typed_tbl.mdb.idx");

    // ── STRING column: insert, fetch, scan, persist ───────────────────────────
    {
        Engine e;
        e.createTypedTable("str_tbl", {ColType::STRING, ColType::UINT32});

        const char* names[] = {"alice", "bob", "charlie", "alice", "dave"};
        uint32_t    scores[] = {90, 85, 92, 78, 88};
        for (int i = 0; i < 5; ++i)
            e.insertTyped("str_tbl", {ColValue(std::string(names[i])), ColValue(scores[i])});

        // Fetch and verify string values
        auto& t = e.openTable("str_tbl");
        for (int i = 0; i < 5; ++i) {
            auto row = t.fetchTypedRow(i);
            assert(row.size() == 2);
            assert(row[0].has_value() && row[0]->type == ColType::STRING);
            assert(row[0]->str == names[i]);
            assert(row[1].has_value() && row[1]->u32 == scores[i]);
        }

        // Scan: "alice" should match rowIDs 0 and 3
        auto hits = e.whereEqString("str_tbl", 0, "alice");
        assert(hits.size() == 2);
        bool has0 = false, has3 = false;
        for (auto rid : hits) { if (rid == 0) has0 = true; if (rid == 3) has3 = true; }
        assert(has0 && has3);

        // Scan: "bob" matches rowID 1 only
        auto bobHits = e.whereEqString("str_tbl", 0, "bob");
        assert(bobHits.size() == 1 && bobHits[0] == 1);

        // Scan: "nobody" matches nothing
        assert(e.whereEqString("str_tbl", 0, "nobody").empty());
    }

    // Persist: re-open and verify strings survive
    {
        Engine e2;
        auto& t2 = e2.openTable("str_tbl");
        auto row = t2.fetchTypedRow(2);
        assert(row[0].has_value() && row[0]->str == "charlie");
        assert(row[1].has_value() && row[1]->u32 == 92u);

        auto hits = e2.whereEqString("str_tbl", 0, "alice");
        assert(hits.size() == 2);
    }

    std::remove("str_tbl.mdb");
    std::remove("str_tbl.mdb.idx");
    std::remove("str_tbl.mdb.0.str");

    std::puts("test_types: passed");
    return 0;
}
