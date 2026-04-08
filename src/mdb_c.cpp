// mdb_c.cpp — C++ bridge implementing the pure-C mdb.h API.
//
// Every public entry point:
//   1. Null-checks all pointer arguments.
//   2. Wraps the Engine call in try/catch(...) so no C++ exception escapes
//      into a C stack frame (which would be undefined behaviour).
//   3. Reports errors via e->lastError + a negative return code / null handle.

#include "mdb.h"

#include <cstdlib>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "Engine.hpp"
#include "Predicate.hpp"
#include "ValueTypes.hpp"

// ── Compile-time alignment checks ─────────────────────────────────────────────
static_assert((int)MDB_UINT32 == (int)ColType::UINT32, "MdbColType/ColType mismatch");
static_assert((int)MDB_INT64  == (int)ColType::INT64,  "MdbColType/ColType mismatch");
static_assert((int)MDB_FLOAT  == (int)ColType::FLOAT,  "MdbColType/ColType mismatch");
static_assert((int)MDB_DOUBLE == (int)ColType::DOUBLE, "MdbColType/ColType mismatch");
static_assert((int)MDB_STRING == (int)ColType::STRING, "MdbColType/ColType mismatch");

static_assert((int)MDB_PRED_EQ        == (int)Predicate::Kind::EQ,
              "MdbPredKind/Predicate::Kind mismatch");
static_assert((int)MDB_PRED_BETWEEN   == (int)Predicate::Kind::BETWEEN,
              "MdbPredKind/Predicate::Kind mismatch");
static_assert((int)MDB_PRED_EQ_STRING == (int)Predicate::Kind::EQ_STRING,
              "MdbPredKind/Predicate::Kind mismatch");

// ── Engine wrapper ─────────────────────────────────────────────────────────────
struct MdbEngine {
    Engine      engine;
    std::string lastError;
    std::string strScratch;  // backing store for MdbValue.str in mdb_fetch_row
};

static void clearLastError(MdbEngine* e) {
    if (e) e->lastError.clear();
}

static std::string tableFilePath(const char* table) {
    return std::string(table) + ".mdb";
}

static Table& requireExistingTable(MdbEngine* e, const char* table) {
    const std::string path = tableFilePath(table);
    if (access(path.c_str(), F_OK) != 0)
        throw std::invalid_argument("table does not exist");
    return e->engine.openTable(std::string(table));
}

static void requireValidColumnIndex(const Table& table, uint16_t col) {
    if (col >= table.numColumns())
        throw std::invalid_argument("column index out of bounds");
}

static void requireValidColumnPair(const Table& table, uint16_t lhs, uint16_t rhs) {
    requireValidColumnIndex(table, lhs);
    requireValidColumnIndex(table, rhs);
}

static void requireMatchingRowWidth(const Table& table, uint32_t numCols) {
    if (numCols != table.numColumns())
        throw std::invalid_argument("row width does not match table schema");
}

// ── Internal helpers ───────────────────────────────────────────────────────────

static std::vector<ColValue> toColValues(const MdbValue* values, uint32_t n) {
    std::vector<ColValue> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; i++) {
        const MdbValue& mv = values[i];
        switch (mv.type) {
            case MDB_UINT32: out.emplace_back(mv.u32); break;
            case MDB_INT64:  out.emplace_back(mv.i64); break;
            case MDB_FLOAT:  out.emplace_back(mv.f32); break;
            case MDB_DOUBLE: out.emplace_back(mv.f64); break;
            case MDB_STRING: out.emplace_back(std::string(mv.str ? mv.str : "")); break;
            default:         out.emplace_back(uint32_t(0)); break;
        }
    }
    return out;
}

static std::vector<Predicate> toPredicates(const MdbPredicate* preds, uint32_t n) {
    std::vector<Predicate> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; i++) {
        Predicate p;
        p.colIdx = preds[i].col_idx;
        p.kind   = static_cast<Predicate::Kind>(preds[i].kind);
        p.lo     = preds[i].lo;
        p.hi     = preds[i].hi;
        if (preds[i].needle) p.needle = preds[i].needle;
        out.push_back(std::move(p));
    }
    return out;
}

// Heap-allocates an MdbRowSet from a vector.  Returns nullptr only on OOM.
static MdbRowSet* makeRowSet(std::vector<uint32_t>&& v) {
    auto* rs = new (std::nothrow) MdbRowSet{};
    if (!rs) return nullptr;
    rs->count   = static_cast<uint32_t>(v.size());
    rs->row_ids = nullptr;
    if (rs->count) {
        rs->row_ids = static_cast<uint32_t*>(malloc(rs->count * sizeof(uint32_t)));
        if (!rs->row_ids) { delete rs; return nullptr; }
        memcpy(rs->row_ids, v.data(), rs->count * sizeof(uint32_t));
    }
    return rs;
}

// ── Lifecycle ──────────────────────────────────────────────────────────────────

MdbEngine* mdb_open(void) {
    try {
        return new MdbEngine{};
    } catch (...) {
        return nullptr;
    }
}

void mdb_close(MdbEngine* e) {
    try { delete e; } catch (...) {}
}

const char* mdb_last_error(MdbEngine* e) {
    if (!e || e->lastError.empty()) return nullptr;
    return e->lastError.c_str();
}

// ── DDL ───────────────────────────────────────────────────────────────────────

int mdb_create_table(MdbEngine* e, const char* name,
                     const MdbColType* col_types, uint32_t num_cols) {
    if (!e || !name || !col_types || num_cols == 0) return MDB_ERR_ARG;
    try {
        std::vector<ColType> types;
        types.reserve(num_cols);
        for (uint32_t i = 0; i < num_cols; i++)
            types.push_back(static_cast<ColType>(col_types[i]));
        e->engine.createTypedTable(std::string(name), types);
        clearLastError(e);
        return MDB_OK;
    } catch (const std::invalid_argument& ex) { e->lastError = ex.what(); return MDB_ERR_ARG; }
      catch (const std::exception&         ex) { e->lastError = ex.what(); return MDB_ERR; }
      catch (...)                              { e->lastError = "unknown"; return MDB_ERR; }
}

// ── DML ───────────────────────────────────────────────────────────────────────

int mdb_insert(MdbEngine* e, const char* table,
               const MdbValue* values, uint32_t num_cols, uint32_t* out_row_id) {
    if (!e || !table || !values) return MDB_ERR_ARG;
    try {
        Table& t = requireExistingTable(e, table);
        requireMatchingRowWidth(t, num_cols);
        auto row = toColValues(values, num_cols);
        uint32_t rid = t.insertTypedRow(row);
        if (out_row_id) *out_row_id = rid;
        clearLastError(e);
        return MDB_OK;
    } catch (const std::invalid_argument& ex) { e->lastError = ex.what(); return MDB_ERR_ARG; }
      catch (const std::exception&         ex) { e->lastError = ex.what(); return MDB_ERR; }
      catch (...)                              { e->lastError = "unknown"; return MDB_ERR; }
}

int mdb_delete(MdbEngine* e, const char* table, uint32_t row_id) {
    if (!e || !table) return MDB_ERR_ARG;
    try {
        requireExistingTable(e, table).deleteRow(row_id);
        clearLastError(e);
        return MDB_OK;
    } catch (const std::invalid_argument& ex) { e->lastError = ex.what(); return MDB_ERR_ARG; }
      catch (const std::exception&         ex) { e->lastError = ex.what(); return MDB_ERR; }
      catch (...)                              { e->lastError = "unknown"; return MDB_ERR; }
}

int mdb_fetch_row(MdbEngine* e, const char* table, uint32_t row_id,
                  MdbValue* out_values, uint32_t num_cols) {
    if (!e || !table || !out_values || num_cols == 0) return MDB_ERR_ARG;
    try {
        Table& t = requireExistingTable(e, table);
        if (num_cols > t.numColumns())
            throw std::invalid_argument("requested column count exceeds table schema");
        auto row = t.fetchTypedRow(row_id);
        if (row.size() < num_cols) return MDB_ERR;

        // First pass: pack all STRING data into the scratch buffer and record
        // each string's start offset so pointers remain stable after resize.
        e->strScratch.clear();
        std::vector<size_t> strOffset(num_cols, static_cast<size_t>(-1));
        for (uint32_t i = 0; i < num_cols; i++) {
            if (row[i] && row[i]->type == ColType::STRING) {
                strOffset[i] = e->strScratch.size();
                e->strScratch += row[i]->str;
                e->strScratch += '\0';
            }
        }

        // Second pass: fill caller's MdbValue array.
        const char* base = e->strScratch.c_str();
        for (uint32_t i = 0; i < num_cols; i++) {
            if (!row[i]) return MDB_ERR;  // slot was deleted
            const ColValue& cv = *row[i];
            MdbValue& mv = out_values[i];
            mv.type = static_cast<MdbColType>(static_cast<int>(cv.type));
            mv.i64  = 0;        // zero the widest union member first
            mv.str  = nullptr;
            switch (cv.type) {
                case ColType::UINT32:  mv.u32 = cv.u32; break;
                case ColType::INT64:   mv.i64 = cv.i64; break;
                case ColType::FLOAT:   mv.f32 = cv.f32; break;
                case ColType::DOUBLE:  mv.f64 = cv.f64; break;
                case ColType::STRING:  mv.str = base + strOffset[i]; break;
            }
        }
        clearLastError(e);
        return MDB_OK;
    } catch (const std::invalid_argument& ex) { e->lastError = ex.what(); return MDB_ERR_ARG; }
      catch (const std::exception&         ex) { e->lastError = ex.what(); return MDB_ERR; }
      catch (...)                              { e->lastError = "unknown"; return MDB_ERR; }
}

// ── Scans ──────────────────────────────────────────────────────────────────────

MdbRowSet* mdb_scan_eq(MdbEngine* e, const char* table, uint16_t col, uint32_t val) {
    if (!e || !table) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnIndex(t, col);
        auto* rs = makeRowSet(t.scanEquals(col, val));
        if (rs) clearLastError(e);
        return rs;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbRowSet* mdb_scan_between(MdbEngine* e, const char* table,
                            uint16_t col, uint32_t lo, uint32_t hi) {
    if (!e || !table) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnIndex(t, col);
        auto* rs = makeRowSet(t.whereBetween(col, lo, hi));
        if (rs) clearLastError(e);
        return rs;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbRowSet* mdb_scan_eq_string(MdbEngine* e, const char* table,
                              uint16_t col, const char* needle) {
    if (!e || !table || !needle) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnIndex(t, col);
        auto* rs = makeRowSet(t.scanEqualsString(col, std::string(needle)));
        if (rs) clearLastError(e);
        return rs;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbRowSet* mdb_where_and(MdbEngine* e, const char* table,
                         const MdbPredicate* preds, uint32_t n) {
    if (!e || !table || (!preds && n > 0)) return nullptr;
    try {
        auto predicates = toPredicates(preds, n);
        auto* rs = makeRowSet(requireExistingTable(e, table).whereAnd(predicates));
        if (rs) clearLastError(e);
        return rs;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbRowSet* mdb_where_or(MdbEngine* e, const char* table,
                        const MdbPredicate* preds, uint32_t n) {
    if (!e || !table || (!preds && n > 0)) return nullptr;
    try {
        auto predicates = toPredicates(preds, n);
        auto* rs = makeRowSet(requireExistingTable(e, table).whereOr(predicates));
        if (rs) clearLastError(e);
        return rs;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

// ── Aggregations ───────────────────────────────────────────────────────────────

int mdb_sum(MdbEngine* e, const char* table, uint16_t col, uint32_t* out) {
    if (!e || !table || !out) return MDB_ERR_ARG;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnIndex(t, col);
        *out = t.sumColumnHybrid(col);
        clearLastError(e);
        return MDB_OK;
    } catch (const std::invalid_argument& ex) { e->lastError = ex.what(); return MDB_ERR_ARG; }
      catch (const std::exception&         ex) { e->lastError = ex.what(); return MDB_ERR; }
      catch (...)                              { e->lastError = "unknown"; return MDB_ERR; }
}

int mdb_min(MdbEngine* e, const char* table, uint16_t col, uint32_t* out) {
    if (!e || !table || !out) return MDB_ERR_ARG;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnIndex(t, col);
        *out = t.minColumn(col);
        clearLastError(e);
        return MDB_OK;
    } catch (const std::invalid_argument& ex) { e->lastError = ex.what(); return MDB_ERR_ARG; }
      catch (const std::exception&         ex) { e->lastError = ex.what(); return MDB_ERR; }
      catch (...)                              { e->lastError = "unknown"; return MDB_ERR; }
}

int mdb_max(MdbEngine* e, const char* table, uint16_t col, uint32_t* out) {
    if (!e || !table || !out) return MDB_ERR_ARG;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnIndex(t, col);
        *out = t.maxColumn(col);
        clearLastError(e);
        return MDB_OK;
    } catch (const std::invalid_argument& ex) { e->lastError = ex.what(); return MDB_ERR_ARG; }
      catch (const std::exception&         ex) { e->lastError = ex.what(); return MDB_ERR; }
      catch (...)                              { e->lastError = "unknown"; return MDB_ERR; }
}

// ── GroupBy ────────────────────────────────────────────────────────────────────

static MdbGroupResult* makeGroupResult(std::unordered_map<uint32_t, uint64_t>&& m) {
    auto* gr = new (std::nothrow) MdbGroupResult{};
    if (!gr) return nullptr;
    gr->count   = static_cast<uint32_t>(m.size());
    gr->entries = nullptr;
    if (gr->count) {
        gr->entries = static_cast<MdbGroupEntry*>(
            malloc(gr->count * sizeof(MdbGroupEntry)));
        if (!gr->entries) { delete gr; return nullptr; }
        uint32_t i = 0;
        for (auto& kv : m) gr->entries[i++] = {kv.first, kv.second};
    }
    return gr;
}

// Variant for maps whose value type is uint32_t (groupMin/groupMax).
static MdbGroupResult* makeGroupResultU32(std::unordered_map<uint32_t, uint32_t>&& m) {
    auto* gr = new (std::nothrow) MdbGroupResult{};
    if (!gr) return nullptr;
    gr->count   = static_cast<uint32_t>(m.size());
    gr->entries = nullptr;
    if (gr->count) {
        gr->entries = static_cast<MdbGroupEntry*>(
            malloc(gr->count * sizeof(MdbGroupEntry)));
        if (!gr->entries) { delete gr; return nullptr; }
        uint32_t i = 0;
        for (auto& kv : m)
            gr->entries[i++] = {kv.first, static_cast<uint64_t>(kv.second)};
    }
    return gr;
}

MdbGroupResult* mdb_group_count(MdbEngine* e, const char* table, uint16_t key_col) {
    if (!e || !table) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnIndex(t, key_col);
        auto* gr = makeGroupResult(e->engine.groupCount(std::string(table), key_col));
        if (gr) clearLastError(e);
        return gr;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbGroupResult* mdb_group_sum(MdbEngine* e, const char* table,
                               uint16_t key_col, uint16_t val_col) {
    if (!e || !table) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnPair(t, key_col, val_col);
        auto* gr = makeGroupResult(e->engine.groupSum(std::string(table), key_col, val_col));
        if (gr) clearLastError(e);
        return gr;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbGroupResult* mdb_group_min(MdbEngine* e, const char* table,
                               uint16_t key_col, uint16_t val_col) {
    if (!e || !table) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnPair(t, key_col, val_col);
        auto* gr = makeGroupResultU32(e->engine.groupMin(std::string(table), key_col, val_col));
        if (gr) clearLastError(e);
        return gr;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbGroupResult* mdb_group_max(MdbEngine* e, const char* table,
                               uint16_t key_col, uint16_t val_col) {
    if (!e || !table) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnPair(t, key_col, val_col);
        auto* gr = makeGroupResultU32(e->engine.groupMax(std::string(table), key_col, val_col));
        if (gr) clearLastError(e);
        return gr;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

MdbGroupResultF* mdb_group_avg(MdbEngine* e, const char* table,
                                uint16_t key_col, uint16_t val_col) {
    if (!e || !table) return nullptr;
    try {
        Table& t = requireExistingTable(e, table);
        requireValidColumnPair(t, key_col, val_col);
        auto m = e->engine.groupAvg(std::string(table), key_col, val_col);
        auto* gr = new (std::nothrow) MdbGroupResultF{};
        if (!gr) return nullptr;
        gr->count   = static_cast<uint32_t>(m.size());
        gr->entries = nullptr;
        if (gr->count) {
            gr->entries = static_cast<MdbGroupEntryF*>(
                malloc(gr->count * sizeof(MdbGroupEntryF)));
            if (!gr->entries) { delete gr; return nullptr; }
            uint32_t i = 0;
            for (auto& kv : m) gr->entries[i++] = {kv.first, kv.second};
        }
        clearLastError(e);
        return gr;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

// ── Join ───────────────────────────────────────────────────────────────────────

MdbJoinResult* mdb_join(MdbEngine* e,
                        const char* left,  uint16_t left_col,
                        const char* right, uint16_t right_col) {
    if (!e || !left || !right) return nullptr;
    try {
        Table& leftTable = requireExistingTable(e, left);
        Table& rightTable = requireExistingTable(e, right);
        requireValidColumnIndex(leftTable, left_col);
        requireValidColumnIndex(rightTable, right_col);
        auto pairs = e->engine.join(std::string(left), left_col,
                                    std::string(right), right_col);
        auto* jr = new (std::nothrow) MdbJoinResult{};
        if (!jr) return nullptr;
        jr->count = static_cast<uint32_t>(pairs.size());
        jr->pairs = nullptr;
        if (jr->count) {
            jr->pairs = static_cast<MdbJoinPair*>(
                malloc(jr->count * sizeof(MdbJoinPair)));
            if (!jr->pairs) { delete jr; return nullptr; }
            for (uint32_t i = 0; i < jr->count; i++)
                jr->pairs[i] = {pairs[i].first, pairs[i].second};
        }
        clearLastError(e);
        return jr;
    } catch (const std::exception& ex) { e->lastError = ex.what(); return nullptr; }
      catch (...)                       { e->lastError = "unknown"; return nullptr; }
}

// ── Free ───────────────────────────────────────────────────────────────────────

void mdb_free_rows(MdbRowSet* rs) {
    if (!rs) return;
    free(rs->row_ids);
    delete rs;
}

void mdb_free_group(MdbGroupResult* gr) {
    if (!gr) return;
    free(gr->entries);
    delete gr;
}

void mdb_free_groupf(MdbGroupResultF* gr) {
    if (!gr) return;
    free(gr->entries);
    delete gr;
}

void mdb_free_join(MdbJoinResult* jr) {
    if (!jr) return;
    free(jr->pairs);
    delete jr;
}
