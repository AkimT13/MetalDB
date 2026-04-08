"""
test_mdb.py — Python integration tests for the MetalDB Python bindings.

Run from the repo root:
    make -C src test-python
Or directly:
    python3 python/test_mdb.py
"""
import sys
import os

# Allow running from anywhere: add python/ to path so `import mdb` works.
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from mdb import Engine, MdbError, Predicate
from mdb import UINT32, INT64, FLOAT, DOUBLE, STRING

_failed = 0

def check(cond: bool, msg: str = "") -> None:
    global _failed
    if not cond:
        import traceback
        frame = traceback.extract_stack()[-2]
        label = msg or f"{frame.filename}:{frame.lineno}  {frame.line}"
        print(f"  FAIL  {label}", file=sys.stderr)
        _failed += 1

def check_eq(a, b, msg: str = "") -> None:
    check(a == b, msg or f"{a!r} != {b!r}")

def check_raises(exc_type, fn):
    try:
        fn()
        check(False, f"expected {exc_type.__name__} but no exception raised")
    except exc_type:
        pass

# ── tests ──────────────────────────────────────────────────────────────────────

def test_lifecycle():
    e = Engine()
    e.close()
    e.close()   # second close must be a no-op

    with Engine() as e2:
        pass    # __exit__ calls close

    print("PASS test_lifecycle")

def test_create_insert_fetch():
    with Engine() as e:
        e.create_table("/tmp/py_cif", [UINT32, UINT32])
        rid = e.insert("/tmp/py_cif", [42, 99])
        row = e.fetch_row("/tmp/py_cif", rid)
        check_eq(row[0], 42)
        check_eq(row[1], 99)
    print("PASS test_create_insert_fetch")

def test_typed_columns():
    with Engine() as e:
        e.create_table("/tmp/py_typed", [UINT32, INT64, FLOAT, DOUBLE])
        rid = e.insert("/tmp/py_typed", [7, -1000, 3.14, 2.718281828])
        row = e.fetch_row("/tmp/py_typed", rid)
        check_eq(row[0], 7)
        check_eq(row[1], -1000)
        check(abs(row[2] - 3.14) < 1e-5, f"float mismatch: {row[2]}")
        check(abs(row[3] - 2.718281828) < 1e-9, f"double mismatch: {row[3]}")
    print("PASS test_typed_columns")

def test_string_roundtrip():
    with Engine() as e:
        e.create_table("/tmp/py_str", [STRING])
        rid = e.insert("/tmp/py_str", ["hello world"])
        row = e.fetch_row("/tmp/py_str", rid)
        check_eq(row[0], "hello world")
    print("PASS test_string_roundtrip")

def test_string_utf8_roundtrip():
    with Engine() as e:
        e.create_table("/tmp/py_utf8", [STRING])
        rid = e.insert("/tmp/py_utf8", ["naive cafe"])
        rid2 = e.insert("/tmp/py_utf8", ["cafe ☕"])
        row1 = e.fetch_row("/tmp/py_utf8", rid)
        row2 = e.fetch_row("/tmp/py_utf8", rid2)
        check_eq(row1[0], "naive cafe")
        check_eq(row2[0], "cafe ☕")
        check_eq(e.scan_eq_string("/tmp/py_utf8", 0, "cafe ☕"), [rid2])
    print("PASS test_string_utf8_roundtrip")

def test_scan_eq():
    with Engine() as e:
        e.create_table("/tmp/py_seq", [UINT32])
        for i in range(100):
            e.insert("/tmp/py_seq", [i % 10])
        rows = e.scan_eq("/tmp/py_seq", 0, 5)
        check_eq(len(rows), 10)
    print("PASS test_scan_eq")

def test_scan_between():
    with Engine() as e:
        e.create_table("/tmp/py_sbt", [UINT32])
        for i in range(50):
            e.insert("/tmp/py_sbt", [i])
        rows = e.scan_between("/tmp/py_sbt", 0, 10, 19)
        check_eq(len(rows), 10)
    print("PASS test_scan_between")

def test_scan_string():
    with Engine() as e:
        e.create_table("/tmp/py_sstr", [STRING])
        for w in ["apple", "banana", "apple", "cherry", "apple"]:
            e.insert("/tmp/py_sstr", [w])
        rows = e.scan_eq_string("/tmp/py_sstr", 0, "apple")
        check_eq(len(rows), 3)
    print("PASS test_scan_string")

def test_where_and_or():
    with Engine() as e:
        e.create_table("/tmp/py_andor", [UINT32, UINT32])
        for i in range(10):
            e.insert("/tmp/py_andor", [i, i * 2])

        # AND: col0 in [3,9] AND col1 in [8,18] → i=4..9 = 6 rows
        rows = e.where_and("/tmp/py_andor", [
            Predicate.between(0, 3, 9),
            Predicate.between(1, 8, 18),
        ])
        check_eq(len(rows), 6)

        # OR: col0==1 OR col0==5 → 2 rows
        rows = e.where_or("/tmp/py_andor", [
            Predicate.eq(0, 1),
            Predicate.eq(0, 5),
        ])
        check_eq(len(rows), 2)
    print("PASS test_where_and_or")

def test_where_string_predicate():
    with Engine() as e:
        e.create_table("/tmp/py_strpred", [UINT32, STRING])
        for i, name in enumerate(["alice", "bob", "alice", "carol", "bob"]):
            e.insert("/tmp/py_strpred", [i, name])
        rows = e.where_and("/tmp/py_strpred", [
            Predicate.eq_string(1, "alice"),
        ])
        check_eq(len(rows), 2)
    print("PASS test_where_string_predicate")

def test_delete():
    with Engine() as e:
        e.create_table("/tmp/py_del", [UINT32])
        rid = e.insert("/tmp/py_del", [77])
        check_eq(len(e.scan_eq("/tmp/py_del", 0, 77)), 1)
        e.delete("/tmp/py_del", rid)
        check_eq(len(e.scan_eq("/tmp/py_del", 0, 77)), 0)
    print("PASS test_delete")

def test_aggregations():
    with Engine() as e:
        e.create_table("/tmp/py_agg", [UINT32])
        for v in [1, 3, 5, 7, 9]:
            e.insert("/tmp/py_agg", [v])
        check_eq(e.sum("/tmp/py_agg", 0), 25)
        check_eq(e.min("/tmp/py_agg", 0), 1)
        check_eq(e.max("/tmp/py_agg", 0), 9)
    print("PASS test_aggregations")

def test_groupby():
    with Engine() as e:
        e.create_table("/tmp/py_grp", [UINT32, UINT32])
        for k in range(3):
            for _ in range(4):
                e.insert("/tmp/py_grp", [k, 10])

        counts = e.group_count("/tmp/py_grp", 0)
        check_eq(len(counts), 3)
        check(all(v == 4 for v in counts.values()), f"counts wrong: {counts}")

        sums = e.group_sum("/tmp/py_grp", 0, 1)
        check(all(v == 40 for v in sums.values()), f"sums wrong: {sums}")

        avgs = e.group_avg("/tmp/py_grp", 0, 1)
        check(all(abs(v - 10.0) < 1e-9 for v in avgs.values()), f"avgs wrong: {avgs}")

        mins = e.group_min("/tmp/py_grp", 0, 1)
        check(all(v == 10 for v in mins.values()), f"mins wrong: {mins}")

        maxs = e.group_max("/tmp/py_grp", 0, 1)
        check(all(v == 10 for v in maxs.values()), f"maxs wrong: {maxs}")
    print("PASS test_groupby")

def test_join():
    with Engine() as e:
        e.create_table("/tmp/py_jl", [UINT32])
        e.create_table("/tmp/py_jr", [UINT32])
        for v in [1, 2, 3]:
            e.insert("/tmp/py_jl", [v])
        for v in [2, 3, 4]:
            e.insert("/tmp/py_jr", [v])
        pairs = e.join("/tmp/py_jl", 0, "/tmp/py_jr", 0)
        check_eq(len(pairs), 2)
        # Each pair is (left_row_id, right_row_id) — just check the count
    print("PASS test_join")

def test_persist_reopen():
    with Engine() as e:
        e.create_table("/tmp/py_persist", [UINT32])
        e.insert("/tmp/py_persist", [999])

    with Engine() as e:
        e.open_table("/tmp/py_persist", [UINT32])
        rows = e.scan_eq("/tmp/py_persist", 0, 999)
        check_eq(len(rows), 1)
    print("PASS test_persist_reopen")

def test_error_handling():
    with Engine() as e:
        e.create_table("/tmp/py_err", [UINT32])

        # Unknown table
        check_raises(MdbError, lambda: e.scan_eq("/tmp/py_nonexist", 0, 1))

        # Schema mismatch
        check_raises(ValueError, lambda: e.insert("/tmp/py_err", [1, 2]))
        check_raises(ValueError, lambda: e.insert("/tmp/py_err", ["bad"]))
        check_raises(ValueError, lambda: e.scan_eq("/tmp/py_err", -1, 1))
        check_raises(ValueError, lambda: e.sum("/tmp/py_err", 70000))
        check_raises(ValueError, lambda: Predicate.eq(-1, 1))
        check_raises(ValueError, lambda: Predicate.eq_string(0, 123))
        check_raises(ValueError,
            lambda: e.where_and("/tmp/py_err", [Predicate.eq(0, 1), "bad"]))

        # Out-of-bounds column via where_and (throws, not asserts)
        check_raises(MdbError,
            lambda: e.where_and("/tmp/py_err", [Predicate.between(99, 0, 1)]))
    print("PASS test_error_handling")

def test_closed_engine_errors():
    e = Engine()
    e.close()
    check_raises(MdbError, lambda: e.create_table("/tmp/py_closed", [UINT32]))
    check_raises(MdbError, lambda: e.scan_eq("/tmp/py_closed", 0, 1))
    print("PASS test_closed_engine_errors")

def test_schema_not_registered():
    with Engine() as e:
        # Trying to insert without registering schema
        check_raises(MdbError, lambda: e.insert("/tmp/py_nosched", [1]))
        # Trying to fetch without schema
        check_raises(MdbError, lambda: e.fetch_row("/tmp/py_nosched", 0))
    print("PASS test_schema_not_registered")

# ── main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_lifecycle()
    test_create_insert_fetch()
    test_typed_columns()
    test_string_roundtrip()
    test_string_utf8_roundtrip()
    test_scan_eq()
    test_scan_between()
    test_scan_string()
    test_where_and_or()
    test_where_string_predicate()
    test_delete()
    test_aggregations()
    test_groupby()
    test_join()
    test_persist_reopen()
    test_error_handling()
    test_closed_engine_errors()
    test_schema_not_registered()

    if _failed:
        print(f"\n{_failed} test(s) FAILED", file=sys.stderr)
        sys.exit(1)
    print("\nAll Python tests passed.")
