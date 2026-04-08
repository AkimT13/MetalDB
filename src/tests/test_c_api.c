/*
 * test_c_api.c — pure C11 tests for the MetalDB C API.
 *
 * Compiled with clang -std=c11 (NOT clang++).  If this file requires C++
 * headers to compile then mdb.h is broken.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mdb.h"

static int g_failed = 0;

#define CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d  %s\n", __func__, __LINE__, #cond); \
        g_failed++; \
    } \
} while (0)

/* ── helpers ──────────────────────────────────────────────────────────────── */

static MdbValue uint32_val(uint32_t v) {
    MdbValue mv;
    mv.type = MDB_UINT32;
    mv.u32  = v;
    mv.str  = NULL;
    return mv;
}

static MdbValue string_val(const char* s) {
    MdbValue mv;
    mv.type = MDB_STRING;
    mv.u32  = 0;
    mv.str  = s;
    return mv;
}

/* ── tests ────────────────────────────────────────────────────────────────── */

static void test_lifecycle(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);
    CHECK(mdb_last_error(e) == NULL);
    mdb_close(e);
    /* NULL safety */
    CHECK(mdb_last_error(NULL) == NULL);
    printf("PASS test_lifecycle\n");
}

static void test_create_insert_fetch(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32, MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_cif", types, 2) == MDB_OK);

    MdbValue row[2];
    row[0] = uint32_val(42);
    row[1] = uint32_val(99);

    uint32_t rid = 0;
    CHECK(mdb_insert(e, "/tmp/c_cif", row, 2, &rid) == MDB_OK);

    MdbValue out[2];
    CHECK(mdb_fetch_row(e, "/tmp/c_cif", rid, out, 2) == MDB_OK);
    CHECK(out[0].type == MDB_UINT32 && out[0].u32 == 42);
    CHECK(out[1].type == MDB_UINT32 && out[1].u32 == 99);

    mdb_close(e);
    printf("PASS test_create_insert_fetch\n");
}

static void test_string_roundtrip(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_STRING };
    CHECK(mdb_create_table(e, "/tmp/c_str", types, 1) == MDB_OK);

    MdbValue row[1];
    row[0] = string_val("hello world");

    uint32_t rid = 0;
    CHECK(mdb_insert(e, "/tmp/c_str", row, 1, &rid) == MDB_OK);

    MdbValue out[1];
    CHECK(mdb_fetch_row(e, "/tmp/c_str", rid, out, 1) == MDB_OK);
    CHECK(out[0].type == MDB_STRING);
    CHECK(out[0].str != NULL);
    CHECK(strcmp(out[0].str, "hello world") == 0);

    mdb_close(e);
    printf("PASS test_string_roundtrip\n");
}

static void test_scan_eq(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_seq", types, 1) == MDB_OK);

    /* 100 rows with values 0..9 (10 rows per value) */
    for (uint32_t i = 0; i < 100; i++) {
        MdbValue v = uint32_val(i % 10);
        CHECK(mdb_insert(e, "/tmp/c_seq", &v, 1, NULL) == MDB_OK);
    }

    MdbRowSet* rs = mdb_scan_eq(e, "/tmp/c_seq", 0, 5);
    CHECK(rs != NULL);
    CHECK(rs->count == 10);
    mdb_free_rows(rs);

    mdb_close(e);
    printf("PASS test_scan_eq\n");
}

static void test_scan_between(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_sbt", types, 1) == MDB_OK);

    for (uint32_t i = 0; i < 50; i++) {
        MdbValue v = uint32_val(i);
        CHECK(mdb_insert(e, "/tmp/c_sbt", &v, 1, NULL) == MDB_OK);
    }

    /* [10, 19] → 10 rows */
    MdbRowSet* rs = mdb_scan_between(e, "/tmp/c_sbt", 0, 10, 19);
    CHECK(rs != NULL);
    CHECK(rs->count == 10);
    mdb_free_rows(rs);

    mdb_close(e);
    printf("PASS test_scan_between\n");
}

static void test_scan_string(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_STRING };
    CHECK(mdb_create_table(e, "/tmp/c_sstr", types, 1) == MDB_OK);

    const char* words[] = {"apple", "banana", "apple", "cherry", "apple"};
    for (int i = 0; i < 5; i++) {
        MdbValue v = string_val(words[i]);
        CHECK(mdb_insert(e, "/tmp/c_sstr", &v, 1, NULL) == MDB_OK);
    }

    MdbRowSet* rs = mdb_scan_eq_string(e, "/tmp/c_sstr", 0, "apple");
    CHECK(rs != NULL);
    CHECK(rs->count == 3);
    mdb_free_rows(rs);

    mdb_close(e);
    printf("PASS test_scan_string\n");
}

static void test_where_and_or(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32, MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_andor", types, 2) == MDB_OK);

    /* (col0, col1) = (i, i*2) for i in 0..9 */
    for (uint32_t i = 0; i < 10; i++) {
        MdbValue row[2];
        row[0] = uint32_val(i);
        row[1] = uint32_val(i * 2);
        CHECK(mdb_insert(e, "/tmp/c_andor", row, 2, NULL) == MDB_OK);
    }

    /*
     * AND: col0 in [3,9] AND col1 in [8,18]
     *   col0 in [3,9]:  i=3..9  → 7 rows
     *   col1 in [8,18]: i=4..9  → 6 rows  (col1=i*2 ∈ [8,18])
     *   intersection:   i=4..9  → 6 rows
     */
    MdbPredicate preds_and[2];
    preds_and[0].col_idx = 0; preds_and[0].kind = MDB_PRED_BETWEEN;
    preds_and[0].lo = 3; preds_and[0].hi = 9; preds_and[0].needle = NULL;
    preds_and[1].col_idx = 1; preds_and[1].kind = MDB_PRED_BETWEEN;
    preds_and[1].lo = 8; preds_and[1].hi = 18; preds_and[1].needle = NULL;

    MdbRowSet* rs_and = mdb_where_and(e, "/tmp/c_andor", preds_and, 2);
    CHECK(rs_and != NULL);
    CHECK(rs_and->count == 6);
    mdb_free_rows(rs_and);

    /*
     * OR: col0==1 OR col0==5 → 2 rows
     */
    MdbPredicate preds_or[2];
    preds_or[0].col_idx = 0; preds_or[0].kind = MDB_PRED_EQ;
    preds_or[0].lo = 1; preds_or[0].hi = 0; preds_or[0].needle = NULL;
    preds_or[1].col_idx = 0; preds_or[1].kind = MDB_PRED_EQ;
    preds_or[1].lo = 5; preds_or[1].hi = 0; preds_or[1].needle = NULL;

    MdbRowSet* rs_or = mdb_where_or(e, "/tmp/c_andor", preds_or, 2);
    CHECK(rs_or != NULL);
    CHECK(rs_or->count == 2);
    mdb_free_rows(rs_or);

    mdb_close(e);
    printf("PASS test_where_and_or\n");
}

static void test_delete(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_del", types, 1) == MDB_OK);

    MdbValue v = uint32_val(77);
    uint32_t rid = 0;
    CHECK(mdb_insert(e, "/tmp/c_del", &v, 1, &rid) == MDB_OK);

    MdbRowSet* rs = mdb_scan_eq(e, "/tmp/c_del", 0, 77);
    CHECK(rs != NULL && rs->count == 1);
    mdb_free_rows(rs);

    CHECK(mdb_delete(e, "/tmp/c_del", rid) == MDB_OK);

    rs = mdb_scan_eq(e, "/tmp/c_del", 0, 77);
    CHECK(rs != NULL && rs->count == 0);
    mdb_free_rows(rs);

    mdb_close(e);
    printf("PASS test_delete\n");
}

static void test_groupby(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32, MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_grp", types, 2) == MDB_OK);

    /* 3 keys, 4 rows each, all with val=10 */
    for (uint32_t k = 0; k < 3; k++) {
        for (uint32_t j = 0; j < 4; j++) {
            MdbValue row[2];
            row[0] = uint32_val(k);
            row[1] = uint32_val(10);
            CHECK(mdb_insert(e, "/tmp/c_grp", row, 2, NULL) == MDB_OK);
        }
    }

    /* count */
    MdbGroupResult* gc = mdb_group_count(e, "/tmp/c_grp", 0);
    CHECK(gc != NULL);
    CHECK(gc->count == 3);
    for (uint32_t i = 0; i < gc->count; i++)
        CHECK(gc->entries[i].value == 4);
    mdb_free_group(gc);

    /* sum: each group sums 4 * 10 = 40 */
    MdbGroupResult* gs = mdb_group_sum(e, "/tmp/c_grp", 0, 1);
    CHECK(gs != NULL);
    CHECK(gs->count == 3);
    for (uint32_t i = 0; i < gs->count; i++)
        CHECK(gs->entries[i].value == 40);
    mdb_free_group(gs);

    /* avg: 10.0 */
    MdbGroupResultF* ga = mdb_group_avg(e, "/tmp/c_grp", 0, 1);
    CHECK(ga != NULL);
    CHECK(ga->count == 3);
    for (uint32_t i = 0; i < ga->count; i++)
        CHECK(ga->entries[i].value == 10.0);
    mdb_free_groupf(ga);

    mdb_close(e);
    printf("PASS test_groupby\n");
}

static void test_join(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types1[] = { MDB_UINT32 };
    MdbColType types2[] = { MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_jl", types1, 1) == MDB_OK);
    CHECK(mdb_create_table(e, "/tmp/c_jr", types2, 1) == MDB_OK);

    /* left: [1, 2, 3],  right: [2, 3, 4]
     * equi-join on col0: matching pairs (2,2) and (3,3) → 2 pairs */
    for (uint32_t v = 1; v <= 3; v++) {
        MdbValue mv = uint32_val(v);
        CHECK(mdb_insert(e, "/tmp/c_jl", &mv, 1, NULL) == MDB_OK);
    }
    for (uint32_t v = 2; v <= 4; v++) {
        MdbValue mv = uint32_val(v);
        CHECK(mdb_insert(e, "/tmp/c_jr", &mv, 1, NULL) == MDB_OK);
    }

    MdbJoinResult* jr = mdb_join(e, "/tmp/c_jl", 0, "/tmp/c_jr", 0);
    CHECK(jr != NULL);
    CHECK(jr->count == 2);
    mdb_free_join(jr);

    mdb_close(e);
    printf("PASS test_join\n");
}

static void test_null_safety(void) {
    /* NULL engine — must not crash */
    CHECK(mdb_last_error(NULL) == NULL);
    CHECK(mdb_scan_eq(NULL, "t", 0, 0) == NULL);
    CHECK(mdb_scan_between(NULL, "t", 0, 0, 1) == NULL);
    CHECK(mdb_scan_eq_string(NULL, "t", 0, "x") == NULL);
    CHECK(mdb_where_and(NULL, "t", NULL, 0) == NULL);
    CHECK(mdb_where_or(NULL, "t", NULL, 0) == NULL);
    CHECK(mdb_group_count(NULL, "t", 0) == NULL);
    CHECK(mdb_join(NULL, "l", 0, "r", 0) == NULL);

    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    /* NULL table name */
    CHECK(mdb_scan_eq(e, NULL, 0, 0) == NULL);
    CHECK(mdb_create_table(e, NULL, NULL, 0) == MDB_ERR_ARG);

    /* NULL out pointer */
    CHECK(mdb_sum(e, "anything", 0, NULL) == MDB_ERR_ARG);

    mdb_close(e);
    printf("PASS test_null_safety\n");
}

static void test_error_propagation(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_err", types, 1) == MDB_OK);

    /*
     * col 99 does not exist on a 1-column table.  Use mdb_where_and which
     * goes through validatePredicate (throws std::invalid_argument) rather
     * than the assert() path in scanEquals.
     */
    MdbPredicate bad;
    bad.col_idx = 99;
    bad.kind    = MDB_PRED_EQ;
    bad.lo      = 0;
    bad.hi      = 0;
    bad.needle  = NULL;
    MdbRowSet* rs = mdb_where_and(e, "/tmp/c_err", &bad, 1);
    CHECK(rs == NULL);
    const char* err = mdb_last_error(e);
    CHECK(err != NULL && strlen(err) > 0);

    mdb_close(e);
    printf("PASS test_error_propagation\n");
}

static void test_insert_validation_and_error_reset(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    MdbColType types[] = { MDB_UINT32, MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_width", types, 2) == MDB_OK);
    CHECK(mdb_last_error(e) == NULL);

    MdbValue short_row[1];
    short_row[0] = uint32_val(7);
    CHECK(mdb_insert(e, "/tmp/c_width", short_row, 1, NULL) == MDB_ERR_ARG);
    CHECK(mdb_last_error(e) != NULL);

    MdbValue row[2];
    row[0] = uint32_val(7);
    row[1] = uint32_val(9);
    CHECK(mdb_insert(e, "/tmp/c_width", row, 2, NULL) == MDB_OK);
    CHECK(mdb_last_error(e) == NULL);

    mdb_close(e);
    printf("PASS test_insert_validation_and_error_reset\n");
}

static void test_query_validation_and_missing_table(void) {
    MdbEngine* e = mdb_open();
    CHECK(e != NULL);

    unlink("/tmp/c_missing_query.mdb");
    unlink("/tmp/c_missing_query.mdb.idx");

    CHECK(mdb_scan_eq(e, "/tmp/c_missing_query", 0, 1) == NULL);
    CHECK(mdb_last_error(e) != NULL);
    CHECK(access("/tmp/c_missing_query.mdb", F_OK) != 0);

    MdbColType types[] = { MDB_UINT32 };
    CHECK(mdb_create_table(e, "/tmp/c_oob", types, 1) == MDB_OK);

    MdbValue v = uint32_val(11);
    CHECK(mdb_insert(e, "/tmp/c_oob", &v, 1, NULL) == MDB_OK);
    CHECK(mdb_last_error(e) == NULL);

    CHECK(mdb_scan_eq(e, "/tmp/c_oob", 7, 11) == NULL);
    CHECK(mdb_last_error(e) != NULL);

    MdbRowSet* rs = mdb_scan_eq(e, "/tmp/c_oob", 0, 11);
    CHECK(rs != NULL);
    CHECK(rs->count == 1);
    CHECK(mdb_last_error(e) == NULL);
    mdb_free_rows(rs);

    {
        uint32_t out = 0;
        CHECK(mdb_sum(e, "/tmp/c_oob", 7, &out) == MDB_ERR_ARG);
        CHECK(mdb_last_error(e) != NULL);
        CHECK(mdb_sum(e, "/tmp/c_oob", 0, &out) == MDB_OK);
        CHECK(out == 11);
        CHECK(mdb_last_error(e) == NULL);
    }

    mdb_close(e);
    printf("PASS test_query_validation_and_missing_table\n");
}

static void test_free_null(void) {
    mdb_free_rows(NULL);
    mdb_free_group(NULL);
    mdb_free_groupf(NULL);
    mdb_free_join(NULL);
    printf("PASS test_free_null\n");
}

static void test_persist_reopen(void) {
    /* Create, insert, close */
    {
        MdbEngine* e = mdb_open();
        CHECK(e != NULL);
        MdbColType types[] = { MDB_UINT32 };
        CHECK(mdb_create_table(e, "/tmp/c_persist", types, 1) == MDB_OK);
        MdbValue v = uint32_val(123);
        CHECK(mdb_insert(e, "/tmp/c_persist", &v, 1, NULL) == MDB_OK);
        mdb_close(e);
    }

    /* Reopen with a fresh engine and scan */
    {
        MdbEngine* e = mdb_open();
        CHECK(e != NULL);
        MdbRowSet* rs = mdb_scan_eq(e, "/tmp/c_persist", 0, 123);
        CHECK(rs != NULL);
        CHECK(rs->count == 1);
        mdb_free_rows(rs);
        mdb_close(e);
    }

    printf("PASS test_persist_reopen\n");
}

/* ── main ─────────────────────────────────────────────────────────────────── */

int main(void) {
    test_lifecycle();
    test_create_insert_fetch();
    test_string_roundtrip();
    test_scan_eq();
    test_scan_between();
    test_scan_string();
    test_where_and_or();
    test_delete();
    test_groupby();
    test_join();
    test_null_safety();
    test_error_propagation();
    test_insert_validation_and_error_reset();
    test_query_validation_and_missing_table();
    test_free_null();
    test_persist_reopen();

    if (g_failed) {
        fprintf(stderr, "\n%d test(s) FAILED\n", g_failed);
        return 1;
    }
    printf("\nAll C API tests passed.\n");
    return 0;
}
