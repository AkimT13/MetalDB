/*
 * mdb.h — Pure C API for MetalDB.
 *
 * Includable from plain .c files; no C++ types are visible.
 * All functions return MDB_OK (0) on success or a negative error code on
 * failure.  Call mdb_last_error() after any failure to get a human-readable
 * message.
 *
 * Thread safety: MdbEngine is NOT thread-safe.  Use one engine per thread or
 * provide external locking.
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

/* ── Opaque engine handle ─────────────────────────────────────────────────── */
typedef struct MdbEngine MdbEngine;

/* ── Return codes ─────────────────────────────────────────────────────────── */
#define MDB_OK        0
#define MDB_ERR      -1
#define MDB_ERR_ARG  -2
#define MDB_ERR_IO   -3
#define MDB_ERR_OOM  -4
#define MDB_ERR_TYPE -5

/* ── Column types ─────────────────────────────────────────────────────────── */
/*
 * Numerically identical to the internal ColType enum.  A static_assert in
 * mdb_c.cpp guards this at compile time.
 */
typedef enum {
    MDB_UINT32 = 0,
    MDB_INT64  = 1,
    MDB_FLOAT  = 2,
    MDB_DOUBLE = 3,
    MDB_STRING = 4
} MdbColType;

/* ── Tagged value ─────────────────────────────────────────────────────────── */
/*
 * For MDB_STRING columns: the str pointer points into a per-engine scratch
 * buffer.  It is valid only until the next mdb_fetch_row call on the same
 * engine.  Callers that need to retain the string must strdup() it.
 */
typedef struct {
    MdbColType type;
    union { uint32_t u32; int64_t i64; float f32; double f64; };
    const char* str;
} MdbValue;

/* ── Predicate ────────────────────────────────────────────────────────────── */
typedef enum {
    MDB_PRED_EQ        = 0,   /* lo holds the equality value */
    MDB_PRED_BETWEEN   = 1,   /* lo..hi inclusive range */
    MDB_PRED_EQ_STRING = 2    /* needle field used */
} MdbPredKind;

typedef struct {
    uint16_t    col_idx;
    MdbPredKind kind;
    uint32_t    lo;           /* EQ: equality value; BETWEEN: range start */
    uint32_t    hi;           /* BETWEEN: range end (ignored for EQ) */
    const char* needle;       /* EQ_STRING only; NULL for numeric predicates */
} MdbPredicate;

/* ── Result handles ───────────────────────────────────────────────────────── */
typedef struct {
    uint32_t* row_ids;
    uint32_t  count;
} MdbRowSet;

typedef struct { uint32_t key; uint64_t value; } MdbGroupEntry;
typedef struct { MdbGroupEntry* entries; uint32_t count; } MdbGroupResult;

typedef struct { uint32_t key; double value; } MdbGroupEntryF;
typedef struct { MdbGroupEntryF* entries; uint32_t count; } MdbGroupResultF;

typedef struct { uint32_t left_row_id; uint32_t right_row_id; } MdbJoinPair;
typedef struct { MdbJoinPair* pairs; uint32_t count; } MdbJoinResult;

/* ── Lifecycle ────────────────────────────────────────────────────────────── */
MdbEngine*  mdb_open(void);
void        mdb_close(MdbEngine* e);
const char* mdb_last_error(MdbEngine* e);  /* NULL if no error */

/* ── DDL ──────────────────────────────────────────────────────────────────── */
int mdb_create_table(MdbEngine* e, const char* name,
                     const MdbColType* col_types, uint32_t num_cols);

/* ── DML ──────────────────────────────────────────────────────────────────── */
/* out_row_id may be NULL if the row ID is not needed. */
int mdb_insert(MdbEngine* e, const char* table,
               const MdbValue* values, uint32_t num_cols, uint32_t* out_row_id);

int mdb_delete(MdbEngine* e, const char* table, uint32_t row_id);
int mdb_flush(MdbEngine* e, const char* table);

/*
 * Fills out_values[0..num_cols-1].  For STRING columns out_values[i].str
 * points into the engine scratch buffer; see MdbValue docs above.
 */
int mdb_fetch_row(MdbEngine* e, const char* table, uint32_t row_id,
                  MdbValue* out_values, uint32_t num_cols);

/* ── Scans ────────────────────────────────────────────────────────────────── */
MdbRowSet* mdb_scan_eq       (MdbEngine* e, const char* table,
                               uint16_t col, uint32_t val);
MdbRowSet* mdb_scan_between  (MdbEngine* e, const char* table,
                               uint16_t col, uint32_t lo, uint32_t hi);
MdbRowSet* mdb_scan_eq_string(MdbEngine* e, const char* table,
                               uint16_t col, const char* needle);
MdbRowSet* mdb_where_and     (MdbEngine* e, const char* table,
                               const MdbPredicate* preds, uint32_t n);
MdbRowSet* mdb_where_or      (MdbEngine* e, const char* table,
                               const MdbPredicate* preds, uint32_t n);

/* ── Aggregations ─────────────────────────────────────────────────────────── */
int mdb_sum(MdbEngine* e, const char* table, uint16_t col, uint32_t* out);
int mdb_min(MdbEngine* e, const char* table, uint16_t col, uint32_t* out);
int mdb_max(MdbEngine* e, const char* table, uint16_t col, uint32_t* out);

/* ── GroupBy ──────────────────────────────────────────────────────────────── */
MdbGroupResult*  mdb_group_count(MdbEngine* e, const char* table,
                                 uint16_t key_col);
MdbGroupResult*  mdb_group_sum  (MdbEngine* e, const char* table,
                                 uint16_t key_col, uint16_t val_col);
MdbGroupResult*  mdb_group_min  (MdbEngine* e, const char* table,
                                 uint16_t key_col, uint16_t val_col);
MdbGroupResult*  mdb_group_max  (MdbEngine* e, const char* table,
                                 uint16_t key_col, uint16_t val_col);
/* mdb_group_avg uses MdbGroupResultF (double values) */
MdbGroupResultF* mdb_group_avg  (MdbEngine* e, const char* table,
                                 uint16_t key_col, uint16_t val_col);

/* ── Join ─────────────────────────────────────────────────────────────────── */
MdbJoinResult* mdb_join(MdbEngine* e,
                        const char* left,  uint16_t left_col,
                        const char* right, uint16_t right_col);

/* ── Free ─────────────────────────────────────────────────────────────────── */
/* All free functions are safe to call with NULL. */
void mdb_free_rows  (MdbRowSet*);
void mdb_free_group (MdbGroupResult*);
void mdb_free_groupf(MdbGroupResultF*);
void mdb_free_join  (MdbJoinResult*);

#ifdef __cplusplus
}
#endif
