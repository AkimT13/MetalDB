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
    std::puts("test_types: passed");
    return 0;
}
