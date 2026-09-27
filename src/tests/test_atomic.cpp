// Statement-level atomicity: Table::applyAtomic groups deletes + inserts into one
// WAL transaction. Verifies validation-before-logging, and crash recovery for both
// a committed-but-unapplied transaction and an uncommitted (torn) one.
#include "../Engine.hpp"
#include "../MiniSQL.hpp"
#include "../Wal.hpp"

#include <cassert>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void cleanup(const std::string& base) {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.1.str"})
        std::remove((base + ext).c_str());
}

std::vector<ColValue> row(uint32_t k, const char* s) {
    return {ColValue(k), ColValue(std::string(s))};
}

size_t liveCount(Table& t) {
    size_t n = 0;
    t.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>&) { ++n; });
    return n;
}

} // namespace

int main() {
    const std::string base = "/tmp/atomic_tbl";
    const std::string path = base + ".mdb";

    // ── applyAtomic basics + validation ─────────────────────────────────────
    cleanup(base);
    {
        Table t(path, 4096, std::vector<ColType>{ColType::UINT32, ColType::STRING});
        auto ids = t.applyAtomic({}, {row(1, "a"), row(2, "b"), row(3, "c")});
        assert((ids == std::vector<uint32_t>{0, 1, 2}));

        // delete 1 + insert 2 in one transaction; duplicate / dead IDs are ignored
        ids = t.applyAtomic({1, 1, 99}, {row(4, "d"), row(5, "e")});
        assert((ids == std::vector<uint32_t>{3, 4}));
        assert(!t.isLive(1));
        assert(liveCount(t) == 4);

        // A bad row anywhere in the batch rejects the whole batch before logging.
        bool threw = false;
        try {
            t.applyAtomic({0}, {row(6, "f"), {ColValue(uint32_t(7))}});
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw);
        assert(t.isLive(0));
        assert(liveCount(t) == 4);

        threw = false;
        try {
            t.insertTypedRow({ColValue(std::string("wrong")), ColValue(std::string("types"))});
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw);
        t.flushDurable();
    }

    // ── committed transaction that never reached the base files ─────────────
    // Simulates a crash after the WAL commit record but before apply.
    {
        Wal wal(path);
        wal.openOrCreate(false);
        const uint64_t txn = wal.beginTxn();
        wal.appendDelete(txn, 0);
        wal.appendInsert(txn, 5, row(10, "recovered"));
        wal.appendInsert(txn, 6, row(11, "recovered-too"));
        wal.appendCommit(txn);
    }
    {
        Table t(path);
        assert(!t.isLive(0));
        assert(t.isLive(5) && t.isLive(6));
        auto r = t.fetchTypedRow(6);
        assert(r[1] && r[1]->str == "recovered-too");
        assert(liveCount(t) == 5);
    }

    // ── uncommitted transaction: must be discarded as a unit ────────────────
    {
        Wal wal(path);
        wal.openOrCreate(false);
        const uint64_t txn = wal.beginTxn();
        wal.appendDelete(txn, 2);
        wal.appendInsert(txn, 7, row(12, "torn"));
        // crash: no commit record
    }
    {
        Table t(path);
        assert(t.isLive(2));
        assert(!t.isLive(7));
        assert(liveCount(t) == 5);
        // The table keeps working after discarding the torn transaction.
        auto ids = t.applyAtomic({}, {row(13, "after")});
        assert(ids[0] == 7);
    }

    // ── mini-SQL statements go through applyAtomic; sync-commit mode works ──
    cleanup(base);
    {
        Engine e;
        e.setSyncCommit(true);
        executeMiniSQL(e, "CREATE TABLE '" + base + "' (UINT32, STRING)");
        assert(e.openTable(base).syncCommit());
        auto r = executeMiniSQL(e, "INSERT INTO '" + base + "' VALUES (1, 'x'), (2, 'y'), (3, 'z')");
        assert(r.rows[0][0] == "3");
        r = executeMiniSQL(e, "DELETE FROM '" + base + "' WHERE c0 >= 2");
        assert(r.rows[0][0] == "2");
    }
    {
        Engine e;
        auto r = executeMiniSQL(e, "SELECT * FROM '" + base + "'");
        assert(r.rows.size() == 1 && r.rows[0][1] == "x");
    }

    cleanup(base);
    std::puts("test_atomic: passed");
    return 0;
}
