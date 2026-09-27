// Operational tooling + the storage bugs it was built to catch:
//  - verify passes on healthy tables and detects injected corruption
//  - compact preserves data, reclaims deleted rows and orphaned heap bytes
//  - backup / restore round-trips and rejects tampered backups
//  - free-page list reuses space freed on previously full pages (was leaked)
//  - the 65535-page limit raises an error instead of overwriting the master page
#include "../Engine.hpp"
#include "../MiniSQL.hpp"
#include "../TableTools.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

void removeTable(const std::string& base) {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.0.str", ".mdb.1.str", ".mdb.2.str"})
        std::remove((base + ext).c_str());
}

uint64_t sizeOf(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0 ? static_cast<uint64_t>(st.st_size) : 0;
}

bool hasLine(const std::vector<std::string>& lines, const std::string& needle) {
    for (const auto& l : lines)
        if (l.find(needle) != std::string::npos) return true;
    return false;
}

std::vector<std::vector<std::string>> dump(const std::string& base) {
    Engine e;
    return executeMiniSQL(e, "SELECT * FROM '" + base + "' ORDER BY c0").rows;
}

void buildTable(const std::string& base, uint32_t rows) {
    removeTable(base);
    Table t(base + ".mdb", 4096, std::vector<ColType>{ColType::UINT32, ColType::STRING, ColType::DOUBLE});
    std::vector<std::vector<ColValue>> batch;
    for (uint32_t i = 0; i < rows; ++i)
        batch.push_back({ColValue(i), ColValue("row-" + std::to_string(i) + std::string(i % 50, 'z')),
                         ColValue(i * 0.5)});
    t.applyAtomic({}, batch);
    t.flushDurable();
}

}  // namespace

int main() {
    const std::string base = "/tmp/tools_tbl";

    // ── verify + compact ────────────────────────────────────────────────────
    buildTable(base, 4000);
    {
        auto rep = tools::verifyTable(base);
        assert(rep.ok() && rep.warnings.empty());
        Engine e;
        executeMiniSQL(e, "DELETE FROM '" + base + "' WHERE c0 < 3000 AND c0 != 1234");
    }
    const auto before = dump(base);
    assert(before.size() == 1001);
    const uint64_t heapBefore = sizeOf(base + ".mdb.1.str");
    {
        auto rep = tools::verifyTable(base);
        assert(rep.ok());
    }
    assert(tools::compactTable(base) == 1001);
    assert(dump(base) == before);
    assert(sizeOf(base + ".mdb.1.str") < heapBefore / 2);
    {
        Table t(base + ".mdb");
        assert(t.rowsRecorded() == 1001 && t.rowCount() == 1001);  // row IDs renumbered densely
        assert(tools::verifyTable(base).ok());
    }
    assert(sizeOf(base + ".compact-tmp.mdb") == 0);  // temp files cleaned up

    // ── backup / restore ────────────────────────────────────────────────────
    const std::string bkDir = "/tmp/tools_backup";
    const std::string restored = "/tmp/tools_restored";
    removeTable(restored);
    for (const auto& f : {"MANIFEST", "tools_tbl.mdb", "tools_tbl.mdb.idx", "tools_tbl.mdb.wal", "tools_tbl.mdb.1.str"})
        std::remove((bkDir + "/" + f).c_str());
    assert(tools::backupTable(base, bkDir) == 4);
    assert(tools::restoreTable(bkDir, restored) == 4);
    assert(dump(restored) == before);
    bool threw = false;
    try {
        tools::restoreTable(bkDir, restored);  // target exists
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
    removeTable(restored);
    {
        // Flip one byte in the backed-up heap: restore must refuse.
        const int fd = ::open((bkDir + "/tools_tbl.mdb.1.str").c_str(), O_RDWR);
        char ch = 0;
        assert(::pread(fd, &ch, 1, 10) == 1);
        ch ^= 0x5A;
        assert(::pwrite(fd, &ch, 1, 10) == 1);
        ::close(fd);
        threw = false;
        try {
            tools::restoreTable(bkDir, restored);
        } catch (const std::runtime_error& ex) {
            threw = std::string(ex.what()).find("checksum mismatch") != std::string::npos;
        }
        assert(threw);
        assert(sizeOf(restored + ".mdb") == 0);  // nothing restored
    }

    // ── corruption detection ────────────────────────────────────────────────
    buildTable(base, 2000);
    uint16_t pid = 0;
    {
        Table t(base + ".mdb");
        const auto* slots = &pid;  // silence unused warnings on some compilers
        (void)slots;
        t.rowIndexForEachLive([&](uint32_t rowID, const std::vector<uint32_t>& s) {
            if (rowID == 1500) pid = ColumnFile::pageIdFromSlotId(s[0]);
        });
    }
    {
        // Shrink the zone map max of the page holding row 1500's c0 (header bytes 12..15).
        const int fd = ::open((base + ".mdb").c_str(), O_RDWR);
        const uint32_t badMax = 0;
        assert(::pwrite(fd, &badMax, 4, off_t(pid) * 4096 + 12) == 4);
        ::close(fd);
        auto rep = tools::verifyTable(base);
        assert(!rep.ok());
        assert(hasLine(rep.errors, "zone map"));
    }
    buildTable(base, 2000);
    {
        // Point a live row-index entry at a slot that is past the page capacity.
        const int fd = ::open((base + ".mdb.idx").c_str(), O_RDWR);
        const size_t entrySize = 4 + 4 * 3;
        const uint32_t badSlot = (1u << 16) | 0xFFFEu;
        assert(::pwrite(fd, &badSlot, 4, off_t(8 + 7 * entrySize + 4)) == 4);  // row 7, column 0
        ::close(fd);
        auto rep = tools::verifyTable(base);
        assert(!rep.ok());
        assert(hasLine(rep.errors, "row 7 c0"));
    }

    // ── free-page list reuses space from previously full pages ──────────────
    removeTable(base);
    {
        Table t(base + ".mdb", 128, 1);  // (128-16)/5 = 22 slots per page
        for (uint32_t i = 0; i < 22 * 6; ++i) t.insertRow({i});  // 6 full pages
        const uint64_t fullSize = sizeOf(base + ".mdb");
        for (uint32_t r : {0u, 1u, 30u, 31u, 60u}) t.deleteRow(r);  // frees slots on 3 full pages
        for (uint32_t i = 0; i < 5; ++i) t.insertRow({1000 + i});
        assert(sizeOf(base + ".mdb") == fullSize);  // all 5 reused, no new page
        t.insertRow({2000});
        assert(sizeOf(base + ".mdb") == fullSize + 128);  // now genuinely full
    }
    assert(tools::verifyTable(base).ok());

    // ── page-ID space exhaustion is an error, not master-page corruption ────
    removeTable(base);
    {
        Table t(base + ".mdb", 32, 1);  // (32-16)/5 = 3 slots per page
        bool full = false;
        std::vector<std::vector<ColValue>> batch;
        uint32_t inserted = 0;
        try {
            while (true) {
                batch.clear();
                for (int i = 0; i < 4096; ++i) batch.push_back({ColValue(inserted + i)});
                t.applyAtomic({}, batch);
                inserted += 4096;
            }
        } catch (const std::runtime_error& ex) {
            full = std::string(ex.what()).find("65535-page limit") != std::string::npos;
        }
        assert(full);
        assert(inserted > 150000);
    }
    {
        Table t(base + ".mdb");  // master page intact: reopens with the right schema
        assert(t.numColumns() == 1 && t.pageSize() == 32);
    }

    // ── geometry validation ─────────────────────────────────────────────────
    threw = false;
    try {
        Table bad(base + "_geom.mdb", 16, 1);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
    std::remove((base + "_geom.mdb").c_str());

    removeTable(base);
    std::puts("test_tools: passed");
    return 0;
}
