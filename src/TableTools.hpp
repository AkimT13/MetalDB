#pragma once
// Offline operational tooling for a single table: integrity verification,
// storage statistics, backup / restore with checksummed manifests, and
// compaction. Exposed through `mdb verify|stats|backup|restore|compact`.
//
// These operate on a table's files directly and assume no other process is
// writing to the table at the same time.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace tools {

struct VerifyReport {
    std::vector<std::string> errors;    // corruption: wrong query results or data loss
    std::vector<std::string> warnings;  // wasted space / recoverable inconsistencies
    std::vector<std::string> info;      // what was checked
    bool ok() const { return errors.empty(); }
};

// Opens the table (which replays any committed WAL tail first) and checks:
// master page, every live row's slot references (page bounds, slot bounds,
// slot-in-use flags, duplicate use), page ownership across columns, page
// headers (ID, used-slot count), zone maps bounding every live UINT32 value,
// STRING heap bounds, the per-column free-page lists (cycles / full pages),
// and orphaned slots.
VerifyReport verifyTable(const std::string& basePath);

// Human-readable key/value storage statistics (row counts, per-column pages,
// fill factor, STRING heap live vs. orphaned bytes, file sizes).
std::vector<std::pair<std::string, std::string>> tableStats(const std::string& basePath);

// Checkpoints the table, then copies all of its files into `destDir` (created if
// needed) with fsync, plus a MANIFEST listing each file's size and FNV-1a-64
// checksum. Returns the number of files copied.
size_t backupTable(const std::string& basePath, const std::string& destDir);

// Verifies every file listed in `backupDir`/MANIFEST against its size and
// checksum, then copies them to `destBasePath`.* (which must not already exist).
// Returns the number of files restored.
size_t restoreTable(const std::string& backupDir, const std::string& destBasePath);

// Rewrites the table with only its live rows: reclaims deleted slots, row-index
// entries, and orphaned STRING heap bytes. Row IDs are renumbered densely in
// their existing order. Returns the number of rows kept.
uint64_t compactTable(const std::string& basePath);

// Paths of every file that belongs to the table (existing ones only).
std::vector<std::string> tableFiles(const std::string& basePath);

}  // namespace tools
