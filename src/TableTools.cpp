#include "TableTools.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

#include "Column.hpp"
#include "SqlAst.hpp"
#include "Table.hpp"

namespace tools {

namespace {

bool fileExists(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0;
}

uint64_t fileSize(const std::string& path) {
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0) return 0;
    return static_cast<uint64_t>(st.st_size);
}

std::string baseName(const std::string& path) {
    const auto slash = path.rfind('/');
    return slash == std::string::npos ? path : path.substr(slash + 1);
}

std::string human(uint64_t bytes) {
    char buf[64];
    if (bytes >= (1ull << 30)) std::snprintf(buf, sizeof(buf), "%.2f GiB", bytes / double(1ull << 30));
    else if (bytes >= (1ull << 20)) std::snprintf(buf, sizeof(buf), "%.2f MiB", bytes / double(1ull << 20));
    else if (bytes >= (1ull << 10)) std::snprintf(buf, sizeof(buf), "%.2f KiB", bytes / double(1ull << 10));
    else std::snprintf(buf, sizeof(buf), "%llu B", static_cast<unsigned long long>(bytes));
    return buf;
}

uint64_t fnv1a64File(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot read '" + path + "'");
    uint64_t h = 1469598103934665603ull;
    char buf[1 << 16];
    while (in) {
        in.read(buf, sizeof(buf));
        const std::streamsize n = in.gcount();
        for (std::streamsize i = 0; i < n; ++i) {
            h ^= static_cast<unsigned char>(buf[i]);
            h *= 1099511628211ull;
        }
    }
    return h;
}

void fsyncPath(const std::string& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
    (void)::fsync(fd);
    ::close(fd);
}

// Copies src → dst via dst.tmp + fsync + rename.
void copyFileDurable(const std::string& src, const std::string& dst) {
    const std::string tmp = dst + ".tmp";
    {
        std::ifstream in(src, std::ios::binary);
        if (!in) throw std::runtime_error("cannot read '" + src + "'");
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) throw std::runtime_error("cannot write '" + tmp + "'");
        out << in.rdbuf();
        out.flush();
        if (!out) throw std::runtime_error("write to '" + tmp + "' failed");
    }
    fsyncPath(tmp);
    if (std::rename(tmp.c_str(), dst.c_str()) != 0)
        throw std::runtime_error("cannot rename '" + tmp + "' to '" + dst + "': " + std::strerror(errno));
}

void ensureDir(const std::string& dir) {
    if (::mkdir(dir.c_str(), 0777) != 0 && errno != EEXIST)
        throw std::runtime_error("cannot create directory '" + dir + "': " + std::strerror(errno));
}

std::string tableFilePath(const std::string& base) { return base + ".mdb"; }

void requireTable(const std::string& base) {
    if (!fileExists(tableFilePath(base)))
        throw std::invalid_argument("table '" + base + "' does not exist");
}

std::string suffixOf(const std::string& file, const std::string& base) {
    return file.substr(base.size());  // e.g. ".mdb", ".mdb.idx", ".mdb.3.str"
}

}  // namespace

std::vector<std::string> tableFiles(const std::string& base) {
    std::vector<std::string> files;
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal"})
        if (fileExists(base + ext)) files.push_back(base + ext);
    if (fileExists(tableFilePath(base))) {
        Table t(tableFilePath(base));
        for (uint16_t c = 0; c < t.numColumns(); ++c) {
            const std::string& heap = t.columnFile(c).heapPath();
            if (!heap.empty() && fileExists(heap)) files.push_back(heap);
        }
    }
    return files;
}

// ── verify ───────────────────────────────────────────────────────────────────

VerifyReport verifyTable(const std::string& base) {
    requireTable(base);
    VerifyReport rep;
    Table t(tableFilePath(base));
    const uint16_t ncols = static_cast<uint16_t>(t.numColumns());

    const uint64_t mdbSize = fileSize(tableFilePath(base));
    if (t.pageSize() == 0) {
        rep.errors.push_back("master page: page size is 0");
        return rep;
    }
    if (mdbSize % t.pageSize() != 0)
        rep.warnings.push_back("table file size " + std::to_string(mdbSize) + " is not a multiple of page size " +
                               std::to_string(t.pageSize()) + " (torn page extension?)");
    const uint64_t pageCount = mdbSize / t.pageSize();
    rep.info.push_back("master page: " + std::to_string(ncols) + " columns, page size " +
                       std::to_string(t.pageSize()) + ", " + std::to_string(pageCount) + " pages");

    // Pass 1: slot references from live rows.
    std::unordered_map<uint16_t, uint16_t> pageOwner;                 // pid → column
    std::vector<std::unordered_map<uint16_t, uint32_t>> referenced(ncols);  // per column: pid → #live refs
    std::vector<std::unordered_set<uint32_t>> seenSlots(ncols);
    size_t liveRows = 0;
    size_t reportedRefErrors = 0;
    auto refError = [&](const std::string& msg) {
        if (++reportedRefErrors <= 20) rep.errors.push_back(msg);
    };

    t.rowIndexForEachLive([&](uint32_t rowID, const std::vector<uint32_t>& slots) {
        ++liveRows;
        for (uint16_t c = 0; c < ncols; ++c) {
            const uint32_t slotID = slots[c];
            const uint16_t pid = ColumnFile::pageIdFromSlotId(slotID);
            const uint16_t idx = ColumnFile::slotIdxFromSlotId(slotID);
            const std::string where = "row " + std::to_string(rowID) + " c" + std::to_string(c);
            if (pid == 0 || pid >= pageCount) {
                refError(where + ": slot points at page " + std::to_string(pid) + " outside the table file");
                continue;
            }
            auto [ownerIt, fresh] = pageOwner.emplace(pid, c);
            if (!fresh && ownerIt->second != c) {
                refError(where + ": page " + std::to_string(pid) + " is shared with column c" +
                         std::to_string(ownerIt->second));
                continue;
            }
            const ColumnPage& page = t.columnFile(c).pageRef(pid);
            if (idx >= page.capacity) {
                refError(where + ": slot " + std::to_string(idx) + " beyond page capacity " +
                         std::to_string(page.capacity));
                continue;
            }
            if (!page.tombstone[idx]) refError(where + ": references a free (deleted) slot");
            if (!seenSlots[c].insert(slotID).second) refError(where + ": slot is also used by another row");
            ++referenced[c][pid];
        }
    });
    if (reportedRefErrors > 20)
        rep.errors.push_back("... " + std::to_string(reportedRefErrors - 20) + " more slot reference errors");
    rep.info.push_back("row index: " + std::to_string(liveRows) + " live of " + std::to_string(t.rowsRecorded()) +
                       " recorded rows");

    // Pass 2: per-page checks.
    for (uint16_t c = 0; c < ncols; ++c) {
        const ColumnFile& cf = t.columnFile(c);
        const ColType type = cf.colType();
        const uint64_t heapSize = cf.heapBytes();
        uint64_t orphanSlots = 0;

        for (const auto& [pid, refs] : referenced[c]) {
            const ColumnPage& page = cf.pageRef(pid);
            const std::string where = "c" + std::to_string(c) + " page " + std::to_string(pid);
            if (page.pageID != pid)
                rep.errors.push_back(where + ": header says page " + std::to_string(page.pageID));
            uint32_t used = 0;
            for (uint16_t i = 0; i < page.capacity; ++i) used += page.tombstone[i] ? 1 : 0;
            if (used != page.count)
                rep.errors.push_back(where + ": header count " + std::to_string(page.count) + " but " +
                                     std::to_string(used) + " slots in use");
            if (used > refs) orphanSlots += used - refs;

            const auto zone = cf.zoneMap(pid);
            for (uint16_t i = 0; i < page.capacity; ++i) {
                if (!page.tombstone[i]) continue;
                if (type == ColType::UINT32) {
                    const uint32_t v = page.readValue(i);
                    if (v < zone.first || v > zone.second) {
                        rep.errors.push_back(where + ": zone map [" + std::to_string(zone.first) + ", " +
                                             std::to_string(zone.second) + "] excludes live value " +
                                             std::to_string(v) + " (range scans would miss it)");
                        break;
                    }
                } else if (type == ColType::STRING) {
                    uint32_t pair[2] = {0, 0};
                    page.readRaw(i, pair, 8);
                    if (uint64_t(pair[0]) + pair[1] > heapSize) {
                        rep.errors.push_back(where + " slot " + std::to_string(i) + ": string [" +
                                             std::to_string(pair[0]) + ", +" + std::to_string(pair[1]) +
                                             ") beyond heap size " + std::to_string(heapSize));
                        break;
                    }
                }
            }
        }
        if (orphanSlots)
            rep.warnings.push_back("c" + std::to_string(c) + ": " + std::to_string(orphanSlots) +
                                   " in-use slots not referenced by any live row (leaked; `mdb compact` reclaims)");

        // Free-page list: must be acyclic and contain only pages with free space.
        std::unordered_set<uint16_t> visited;
        uint16_t pid = cf.freeListHead();
        while (pid != UINT16_MAX) {
            if (pid == 0 || pid >= pageCount) {
                rep.errors.push_back("c" + std::to_string(c) + " free list: points outside the file (page " +
                                     std::to_string(pid) + ")");
                break;
            }
            if (!visited.insert(pid).second) {
                rep.errors.push_back("c" + std::to_string(c) + " free list: cycle at page " + std::to_string(pid));
                break;
            }
            auto owner = pageOwner.find(pid);
            if (owner != pageOwner.end() && owner->second != c) {
                rep.errors.push_back("c" + std::to_string(c) + " free list: page " + std::to_string(pid) +
                                     " belongs to column c" + std::to_string(owner->second));
                break;
            }
            const ColumnPage& page = cf.pageRef(pid);
            if (page.count >= page.capacity)
                rep.warnings.push_back("c" + std::to_string(c) + " free list: page " + std::to_string(pid) +
                                       " is full");
            pid = page.nextFreePage;
        }
    }
    rep.info.push_back("checked " + std::to_string(pageOwner.size()) + " referenced pages across " +
                       std::to_string(ncols) + " columns");
    return rep;
}

// ── stats ────────────────────────────────────────────────────────────────────

std::vector<std::pair<std::string, std::string>> tableStats(const std::string& base) {
    requireTable(base);
    std::vector<std::pair<std::string, std::string>> out;
    Table t(tableFilePath(base));
    const uint16_t ncols = static_cast<uint16_t>(t.numColumns());
    const uint64_t pageCount = fileSize(tableFilePath(base)) / t.pageSize();

    out.push_back({"table", base});
    out.push_back({"columns", std::to_string(ncols)});
    out.push_back({"page_size", std::to_string(t.pageSize())});
    out.push_back({"live_rows", std::to_string(t.rowCount())});
    out.push_back({"deleted_rows", std::to_string(t.rowsRecorded() - t.rowCount())});
    out.push_back({"pages", std::to_string(pageCount) + " of 65535 max (" +
                                std::to_string(pageCount * 100 / 65535) + "% of page-ID space)"});

    std::vector<std::unordered_set<uint16_t>> pages(ncols);
    std::vector<uint64_t> liveStringBytes(ncols, 0);
    t.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>& slots) {
        for (uint16_t c = 0; c < ncols; ++c) {
            pages[c].insert(ColumnFile::pageIdFromSlotId(slots[c]));
            if (t.columnFile(c).colType() == ColType::STRING) {
                const ColumnPage& p = t.columnFile(c).pageRef(ColumnFile::pageIdFromSlotId(slots[c]));
                uint32_t pair[2] = {0, 0};
                p.readRaw(ColumnFile::slotIdxFromSlotId(slots[c]), pair, 8);
                liveStringBytes[c] += pair[1];
            }
        }
    });

    for (uint16_t c = 0; c < ncols; ++c) {
        const ColumnFile& cf = t.columnFile(c);
        uint64_t capacity = 0;
        for (uint16_t pid : pages[c]) capacity += cf.pageRef(pid).capacity;
        char fill[32];
        std::snprintf(fill, sizeof(fill), "%.1f%%", capacity ? 100.0 * t.rowCount() / capacity : 0.0);
        std::string line = std::string(sql::colTypeName(cf.colType())) + ", " + std::to_string(pages[c].size()) +
                           " pages, " + fill + " slot fill";
        if (cf.colType() == ColType::STRING) {
            const uint64_t heap = cf.heapBytes();
            const uint64_t orphan = heap > liveStringBytes[c] ? heap - liveStringBytes[c] : 0;
            line += ", heap " + human(heap) + " (" + human(orphan) + " orphaned)";
        }
        out.push_back({"c" + std::to_string(c), line});
    }

    uint64_t total = 0;
    for (const auto& f : tableFiles(base)) {
        const uint64_t sz = fileSize(f);
        total += sz;
        out.push_back({"file " + baseName(f), human(sz)});
    }
    out.push_back({"total_size", human(total)});
    return out;
}

// ── backup / restore ─────────────────────────────────────────────────────────

size_t backupTable(const std::string& base, const std::string& destDir) {
    requireTable(base);
    {
        Table t(tableFilePath(base));
        t.flushDurable();  // fold the WAL into the base files first
    }
    ensureDir(destDir);
    const auto files = tableFiles(base);
    std::ostringstream manifest;
    manifest << "MetalDB-backup 1\n";
    manifest << "table " << baseName(base) << "\n";
    for (const auto& src : files) {
        const std::string name = baseName(base) + suffixOf(src, base);
        const std::string dst = destDir + "/" + name;
        copyFileDurable(src, dst);
        char line[128];
        std::snprintf(line, sizeof(line), "%016llx %llu ", static_cast<unsigned long long>(fnv1a64File(dst)),
                      static_cast<unsigned long long>(fileSize(dst)));
        manifest << "file " << line << name << "\n";
    }
    const std::string manifestPath = destDir + "/MANIFEST";
    {
        std::ofstream out(manifestPath + ".tmp", std::ios::trunc);
        out << manifest.str();
    }
    fsyncPath(manifestPath + ".tmp");
    if (std::rename((manifestPath + ".tmp").c_str(), manifestPath.c_str()) != 0)
        throw std::runtime_error("cannot write backup MANIFEST");
    fsyncPath(destDir);
    return files.size();
}

size_t restoreTable(const std::string& backupDir, const std::string& destBase) {
    std::ifstream in(backupDir + "/MANIFEST");
    if (!in) throw std::invalid_argument("no MANIFEST in '" + backupDir + "'");
    std::string header;
    std::getline(in, header);
    if (header != "MetalDB-backup 1") throw std::invalid_argument("unrecognized backup format: " + header);
    std::string word, table;
    in >> word >> table;
    if (word != "table") throw std::invalid_argument("malformed MANIFEST");

    struct Entry {
        std::string name;
        uint64_t checksum;
        uint64_t size;
    };
    std::vector<Entry> entries;
    std::string hex;
    Entry e;
    while (in >> word >> hex >> e.size >> e.name) {
        if (word != "file") throw std::invalid_argument("malformed MANIFEST");
        e.checksum = std::stoull(hex, nullptr, 16);
        entries.push_back(e);
    }
    if (entries.empty()) throw std::invalid_argument("MANIFEST lists no files");

    // Verify everything before touching the destination.
    for (const auto& entry : entries) {
        const std::string path = backupDir + "/" + entry.name;
        if (!fileExists(path)) throw std::runtime_error("backup file missing: " + entry.name);
        if (fileSize(path) != entry.size) throw std::runtime_error("backup file size mismatch: " + entry.name);
        if (fnv1a64File(path) != entry.checksum) throw std::runtime_error("backup checksum mismatch: " + entry.name);
    }
    if (fileExists(tableFilePath(destBase)))
        throw std::invalid_argument("restore target '" + destBase + "' already exists");

    for (const auto& entry : entries) {
        const std::string suffix = entry.name.substr(table.size());
        copyFileDurable(backupDir + "/" + entry.name, destBase + suffix);
    }
    return entries.size();
}

// ── compact ──────────────────────────────────────────────────────────────────

uint64_t compactTable(const std::string& base) {
    requireTable(base);
    const std::string tmpBase = base + ".compact-tmp";
    for (const auto& f : tableFiles(tmpBase)) std::remove(f.c_str());

    uint64_t kept = 0;
    std::vector<std::string> newSuffixes;
    {
        Table src(tableFilePath(base));
        auto dst = std::make_unique<Table>(tableFilePath(tmpBase), src.pageSize(), src.columnTypes());
        const uint16_t ncols = static_cast<uint16_t>(src.numColumns());

        std::vector<std::vector<ColValue>> batch;
        auto flushBatch = [&] {
            dst->applyAtomic({}, batch);
            kept += batch.size();
            batch.clear();
        };
        src.rowIndexForEachLive([&](uint32_t, const std::vector<uint32_t>& slots) {
            std::vector<ColValue> row;
            row.reserve(ncols);
            for (uint16_t c = 0; c < ncols; ++c) {
                auto v = src.columnFile(c).fetchTypedSlot(slots[c]);
                if (!v) throw std::runtime_error("compact: live row references a free slot (run `mdb verify`)");
                row.push_back(std::move(*v));
            }
            batch.push_back(std::move(row));
            if (batch.size() >= 8192) flushBatch();
        });
        if (!batch.empty()) flushBatch();
        dst->flushDurable();
        dst.reset();
    }

    // Swap the rebuilt files into place, then drop any leftovers from the old table.
    const auto oldFiles = tableFiles(base);
    const auto newFiles = tableFiles(tmpBase);
    std::unordered_set<std::string> replaced;
    for (const auto& f : newFiles) {
        const std::string target = base + suffixOf(f, tmpBase);
        fsyncPath(f);
        if (std::rename(f.c_str(), target.c_str()) != 0)
            throw std::runtime_error("compact: cannot move '" + f + "' into place: " + std::strerror(errno));
        replaced.insert(target);
    }
    for (const auto& f : oldFiles)
        if (!replaced.count(f)) std::remove(f.c_str());
    return kept;
}

}  // namespace tools
