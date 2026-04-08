"""
mdb — Python bindings for MetalDB.

Wraps libmdb.dylib (built in src/) via ctypes.  No compilation required.

Quick start::

    from mdb import Engine, UINT32, STRING, Predicate

    with Engine() as e:
        e.create_table("/tmp/demo", [UINT32, STRING])
        rid = e.insert("/tmp/demo", [42, "hello"])
        print(e.scan_eq("/tmp/demo", 0, 42))   # [0]
        print(e.fetch_row("/tmp/demo", rid))   # [42, 'hello']

Thread safety: Engine is NOT thread-safe.  Use one Engine per thread or
provide external locking.
"""
from __future__ import annotations

import ctypes
import pathlib
from typing import Dict, Iterable, List, Optional, Tuple

# ── Library loading ────────────────────────────────────────────────────────────

def _find_lib() -> str:
    here = pathlib.Path(__file__).resolve().parent
    candidates = [
        here / "libmdb.dylib",
        here.parent / "src" / "libmdb.dylib",
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    raise FileNotFoundError(
        "libmdb.dylib not found.  Run `make libmdb.dylib` inside the src/ directory."
    )

_lib = ctypes.CDLL(_find_lib())

# ── Column-type constants ──────────────────────────────────────────────────────

UINT32 = 0
INT64  = 1
FLOAT  = 2
DOUBLE = 3
STRING = 4

_VALID_COL_TYPES = {UINT32, INT64, FLOAT, DOUBLE, STRING}

# ── ctypes structure mirrors ───────────────────────────────────────────────────
# Layout must match mdb.h exactly; verified by static_assert in mdb_c.cpp.

class _ValUnion(ctypes.Union):
    _fields_ = [
        ("u32", ctypes.c_uint32),
        ("i64", ctypes.c_int64),
        ("f32", ctypes.c_float),
        ("f64", ctypes.c_double),
    ]

class _MdbValue(ctypes.Structure):
    # C layout: type(int,4) + 4-byte pad + union(8) + str*(8)  = 24 bytes
    _fields_ = [
        ("type", ctypes.c_int),
        ("_val", _ValUnion),
        ("str",  ctypes.c_char_p),
    ]

class _MdbPredicate(ctypes.Structure):
    # C layout: col_idx(u16,2) + 2-byte pad + kind(int,4) + lo(u32,4)
    #           + hi(u32,4) + needle*(8) = 24 bytes
    _fields_ = [
        ("col_idx", ctypes.c_uint16),
        ("kind",    ctypes.c_int),
        ("lo",      ctypes.c_uint32),
        ("hi",      ctypes.c_uint32),
        ("needle",  ctypes.c_char_p),
    ]

class _MdbRowSet(ctypes.Structure):
    _fields_ = [
        ("row_ids", ctypes.POINTER(ctypes.c_uint32)),
        ("count",   ctypes.c_uint32),
    ]

class _MdbGroupEntry(ctypes.Structure):
    _fields_ = [("key", ctypes.c_uint32), ("value", ctypes.c_uint64)]

class _MdbGroupResult(ctypes.Structure):
    _fields_ = [
        ("entries", ctypes.POINTER(_MdbGroupEntry)),
        ("count",   ctypes.c_uint32),
    ]

class _MdbGroupEntryF(ctypes.Structure):
    _fields_ = [("key", ctypes.c_uint32), ("value", ctypes.c_double)]

class _MdbGroupResultF(ctypes.Structure):
    _fields_ = [
        ("entries", ctypes.POINTER(_MdbGroupEntryF)),
        ("count",   ctypes.c_uint32),
    ]

class _MdbJoinPair(ctypes.Structure):
    _fields_ = [("left_row_id", ctypes.c_uint32), ("right_row_id", ctypes.c_uint32)]

class _MdbJoinResult(ctypes.Structure):
    _fields_ = [
        ("pairs", ctypes.POINTER(_MdbJoinPair)),
        ("count", ctypes.c_uint32),
    ]

# ── C function signatures ──────────────────────────────────────────────────────

_lib.mdb_open.restype  = ctypes.c_void_p
_lib.mdb_open.argtypes = []

_lib.mdb_close.restype  = None
_lib.mdb_close.argtypes = [ctypes.c_void_p]

_lib.mdb_last_error.restype  = ctypes.c_char_p
_lib.mdb_last_error.argtypes = [ctypes.c_void_p]

_lib.mdb_create_table.restype  = ctypes.c_int
_lib.mdb_create_table.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p,
    ctypes.POINTER(ctypes.c_int), ctypes.c_uint32,
]

_lib.mdb_insert.restype  = ctypes.c_int
_lib.mdb_insert.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p,
    ctypes.POINTER(_MdbValue), ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint32),
]

_lib.mdb_delete.restype  = ctypes.c_int
_lib.mdb_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32]

_lib.mdb_fetch_row.restype  = ctypes.c_int
_lib.mdb_fetch_row.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32,
    ctypes.POINTER(_MdbValue), ctypes.c_uint32,
]

_lib.mdb_scan_eq.restype  = ctypes.POINTER(_MdbRowSet)
_lib.mdb_scan_eq.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16, ctypes.c_uint32,
]

_lib.mdb_scan_between.restype  = ctypes.POINTER(_MdbRowSet)
_lib.mdb_scan_between.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p,
    ctypes.c_uint16, ctypes.c_uint32, ctypes.c_uint32,
]

_lib.mdb_scan_eq_string.restype  = ctypes.POINTER(_MdbRowSet)
_lib.mdb_scan_eq_string.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16, ctypes.c_char_p,
]

_lib.mdb_where_and.restype  = ctypes.POINTER(_MdbRowSet)
_lib.mdb_where_and.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p,
    ctypes.POINTER(_MdbPredicate), ctypes.c_uint32,
]

_lib.mdb_where_or.restype  = ctypes.POINTER(_MdbRowSet)
_lib.mdb_where_or.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p,
    ctypes.POINTER(_MdbPredicate), ctypes.c_uint32,
]

_lib.mdb_sum.restype  = ctypes.c_int
_lib.mdb_sum.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16,
    ctypes.POINTER(ctypes.c_uint32),
]

_lib.mdb_min.restype  = ctypes.c_int
_lib.mdb_min.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16,
    ctypes.POINTER(ctypes.c_uint32),
]

_lib.mdb_max.restype  = ctypes.c_int
_lib.mdb_max.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16,
    ctypes.POINTER(ctypes.c_uint32),
]

_lib.mdb_group_count.restype  = ctypes.POINTER(_MdbGroupResult)
_lib.mdb_group_count.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16]

_lib.mdb_group_sum.restype  = ctypes.POINTER(_MdbGroupResult)
_lib.mdb_group_sum.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16, ctypes.c_uint16,
]

_lib.mdb_group_min.restype  = ctypes.POINTER(_MdbGroupResult)
_lib.mdb_group_min.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16, ctypes.c_uint16,
]

_lib.mdb_group_max.restype  = ctypes.POINTER(_MdbGroupResult)
_lib.mdb_group_max.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16, ctypes.c_uint16,
]

_lib.mdb_group_avg.restype  = ctypes.POINTER(_MdbGroupResultF)
_lib.mdb_group_avg.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint16, ctypes.c_uint16,
]

_lib.mdb_join.restype  = ctypes.POINTER(_MdbJoinResult)
_lib.mdb_join.argtypes = [
    ctypes.c_void_p,
    ctypes.c_char_p, ctypes.c_uint16,
    ctypes.c_char_p, ctypes.c_uint16,
]

_lib.mdb_free_rows.restype  = None
_lib.mdb_free_rows.argtypes = [ctypes.POINTER(_MdbRowSet)]

_lib.mdb_free_group.restype  = None
_lib.mdb_free_group.argtypes = [ctypes.POINTER(_MdbGroupResult)]

_lib.mdb_free_groupf.restype  = None
_lib.mdb_free_groupf.argtypes = [ctypes.POINTER(_MdbGroupResultF)]

_lib.mdb_free_join.restype  = None
_lib.mdb_free_join.argtypes = [ctypes.POINTER(_MdbJoinResult)]

# ── Internal helpers ───────────────────────────────────────────────────────────

def _last_error_msg(handle) -> str:
    b = _lib.mdb_last_error(handle)
    return b.decode() if b else "unknown error"

def _check(rc: int, handle) -> None:
    if rc != 0:
        raise MdbError(_last_error_msg(handle))

def _drain_rowset(rs_ptr) -> Optional[List[int]]:
    """Copy rows out of *rs_ptr and free it.  Returns None if rs_ptr is null."""
    if not rs_ptr:
        return None
    try:
        rs = rs_ptr.contents
        return [rs.row_ids[i] for i in range(rs.count)]
    finally:
        _lib.mdb_free_rows(rs_ptr)

def _drain_group_result(gr_ptr) -> Optional[Dict[int, int]]:
    if not gr_ptr:
        return None
    try:
        result = gr_ptr.contents
        return {result.entries[i].key: result.entries[i].value for i in range(result.count)}
    finally:
        _lib.mdb_free_group(gr_ptr)

def _drain_group_resultf(gr_ptr) -> Optional[Dict[int, float]]:
    if not gr_ptr:
        return None
    try:
        result = gr_ptr.contents
        return {result.entries[i].key: result.entries[i].value for i in range(result.count)}
    finally:
        _lib.mdb_free_groupf(gr_ptr)

def _drain_join_result(jr_ptr) -> Optional[List[Tuple[int, int]]]:
    if not jr_ptr:
        return None
    try:
        result = jr_ptr.contents
        return [
            (result.pairs[i].left_row_id, result.pairs[i].right_row_id)
            for i in range(result.count)
        ]
    finally:
        _lib.mdb_free_join(jr_ptr)

def _require_open_handle(handle) -> None:
    if not handle:
        raise MdbError("engine is closed")

def _encode_name(name: str, what: str = "table") -> bytes:
    if not isinstance(name, str) or not name:
        raise ValueError(f"{what} name must be a non-empty string")
    return name.encode("utf-8")

def _normalize_schema(col_types: Iterable[int]) -> List[int]:
    schema = list(col_types)
    if not schema:
        raise ValueError("schema must contain at least one column")
    for i, t in enumerate(schema):
        if t not in _VALID_COL_TYPES:
            raise ValueError(f"column {i}: unknown type {t}")
    return schema

def _require_registered_schema(schemas: Dict[str, List[int]], table: str) -> List[int]:
    schema = schemas.get(table)
    if schema is None:
        raise MdbError(f"unknown table '{table}': call create_table or open_table first")
    return schema

def _normalize_col_index(col: int, what: str = "column") -> int:
    if not isinstance(col, int):
        raise ValueError(f"{what} index must be an integer")
    if col < 0 or col > 0xFFFF:
        raise ValueError(f"{what} index must be in [0, 65535]")
    return col

def _require_predicates(predicates: Iterable["Predicate"]) -> List["Predicate"]:
    items = list(predicates)
    for i, predicate in enumerate(items):
        if not isinstance(predicate, Predicate):
            raise ValueError(f"predicate {i} must be a Predicate instance")
    return items

def _validate_value_for_type(value, col_type: int, idx: int) -> None:
    if col_type == UINT32:
        if not isinstance(value, int):
            raise ValueError(f"column {idx}: expected int for UINT32")
        if value < 0 or value > 0xFFFF_FFFF:
            raise ValueError(f"column {idx}: UINT32 value out of range")
        return
    if col_type == INT64:
        if not isinstance(value, int):
            raise ValueError(f"column {idx}: expected int for INT64")
        if value < -(1 << 63) or value > (1 << 63) - 1:
            raise ValueError(f"column {idx}: INT64 value out of range")
        return
    if col_type == FLOAT or col_type == DOUBLE:
        if not isinstance(value, (int, float)):
            raise ValueError(f"column {idx}: expected int or float")
        return
    if col_type == STRING:
        if not isinstance(value, (str, bytes, bytearray)):
            raise ValueError(f"column {idx}: expected str/bytes for STRING")
        return
    raise ValueError(f"column {idx}: unknown type {col_type}")

def _make_value_array(values: list, schema: List[int]):
    """
    Build a (_MdbValue * n) array from Python values + schema types.

    Returns (array, str_refs) where str_refs is a list of bytes objects that
    must remain alive for as long as the array is passed to C code.
    """
    n = len(values)
    arr = (_MdbValue * n)()
    str_refs: List[bytes] = []
    for i, (v, t) in enumerate(zip(values, schema)):
        _validate_value_for_type(v, t, i)
        arr[i].type = t
        arr[i].str  = None
        if t == UINT32:
            arr[i]._val.u32 = int(v) & 0xFFFF_FFFF
        elif t == INT64:
            arr[i]._val.i64 = int(v)
        elif t == FLOAT:
            arr[i]._val.f32 = float(v)
        elif t == DOUBLE:
            arr[i]._val.f64 = float(v)
        elif t == STRING:
            b = v.encode("utf-8") if isinstance(v, str) else bytes(v)
            str_refs.append(b)
            arr[i].str = b     # pointer; b kept alive via str_refs
        else:
            raise ValueError(f"column {i}: unknown type {t}")
    return arr, str_refs

def _mdb_value_to_py(mv: _MdbValue):
    t = mv.type
    if t == UINT32: return mv._val.u32
    if t == INT64:  return mv._val.i64
    if t == FLOAT:  return mv._val.f32
    if t == DOUBLE: return mv._val.f64
    if t == STRING: return mv.str.decode("utf-8") if mv.str else None
    raise ValueError(f"unknown MdbColType {t}")

# ── Predicate ──────────────────────────────────────────────────────────────────

class Predicate:
    """
    An immutable scan predicate.  Build with the class-method factories:

        Predicate.eq(col, val)
        Predicate.between(col, lo, hi)
        Predicate.eq_string(col, needle)
    """

    def __init__(self, struct: _MdbPredicate, _str_ref: Optional[bytes] = None):
        self._struct   = struct
        self._str_ref  = _str_ref   # keeps needle bytes alive

    @classmethod
    def eq(cls, col: int, val: int) -> "Predicate":
        p = _MdbPredicate()
        p.col_idx = _normalize_col_index(col)
        p.kind    = 0             # MDB_PRED_EQ
        p.lo      = int(val) & 0xFFFF_FFFF
        p.hi      = 0
        p.needle  = None
        return cls(p)

    @classmethod
    def between(cls, col: int, lo: int, hi: int) -> "Predicate":
        p = _MdbPredicate()
        p.col_idx = _normalize_col_index(col)
        p.kind    = 1             # MDB_PRED_BETWEEN
        p.lo      = int(lo) & 0xFFFF_FFFF
        p.hi      = int(hi) & 0xFFFF_FFFF
        p.needle  = None
        return cls(p)

    @classmethod
    def eq_string(cls, col: int, needle: str) -> "Predicate":
        if not isinstance(needle, (str, bytes, bytearray)):
            raise ValueError("string predicate needle must be str/bytes")
        b = needle.encode("utf-8") if isinstance(needle, str) else bytes(needle)
        p = _MdbPredicate()
        p.col_idx = _normalize_col_index(col)
        p.kind    = 2             # MDB_PRED_EQ_STRING
        p.lo      = 0
        p.hi      = 0
        p.needle  = b            # pointer into b; b kept alive via _str_ref
        return cls(p, _str_ref=b)

# ── Exceptions ─────────────────────────────────────────────────────────────────

class MdbError(RuntimeError):
    """Raised when a MetalDB operation fails."""

# ── Engine class ───────────────────────────────────────────────────────────────

class Engine:
    """
    A MetalDB engine instance.  Supports use as a context manager::

        with Engine() as e:
            e.create_table(...)

    Table schemas (column type lists) must be registered via create_table()
    or open_table() before calling insert() or fetch_row().
    """

    def __init__(self):
        handle = _lib.mdb_open()
        if not handle:
            raise MdbError("mdb_open failed")
        self._h: int = handle
        self._schemas: Dict[str, List[int]] = {}

    def close(self) -> None:
        if self._h:
            _lib.mdb_close(self._h)
            self._h = 0

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()

    # ── DDL ──────────────────────────────────────────────────────────────────

    def create_table(self, name: str, col_types: List[int]) -> None:
        """Create (or reset) a table with the given column-type schema."""
        _require_open_handle(self._h)
        name_b = _encode_name(name)
        schema = _normalize_schema(col_types)
        n   = len(schema)
        arr = (ctypes.c_int * n)(*schema)
        _check(_lib.mdb_create_table(
            self._h, name_b, arr, ctypes.c_uint32(n)), self._h)
        self._schemas[name] = schema

    def open_table(self, name: str, col_types: List[int]) -> None:
        """Register schema for an on-disk table created in a previous session."""
        _require_open_handle(self._h)
        _encode_name(name)
        self._schemas[name] = _normalize_schema(col_types)

    # ── DML ───────────────────────────────────────────────────────────────────

    def insert(self, table: str, values: list) -> int:
        """Insert a row; values must match the registered schema.  Returns row ID."""
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        schema = _require_registered_schema(self._schemas, table)
        if len(values) != len(schema):
            raise ValueError(f"expected {len(schema)} values, got {len(values)}")
        arr, str_refs = _make_value_array(values, schema)
        rid = ctypes.c_uint32(0)
        _check(_lib.mdb_insert(
            self._h, table_b, arr, len(values),
            ctypes.byref(rid)), self._h)
        return rid.value

    def delete(self, table: str, row_id: int) -> None:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        _check(_lib.mdb_delete(
            self._h, table_b, ctypes.c_uint32(row_id)), self._h)

    def fetch_row(self, table: str, row_id: int) -> list:
        """Return a list of Python values for the given row."""
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        schema = _require_registered_schema(self._schemas, table)
        n   = len(schema)
        arr = (_MdbValue * n)()
        _check(_lib.mdb_fetch_row(
            self._h, table_b, ctypes.c_uint32(row_id),
            arr, ctypes.c_uint32(n)), self._h)
        return [_mdb_value_to_py(arr[i]) for i in range(n)]

    # ── Scans ──────────────────────────────────────────────────────────────────

    def scan_eq(self, table: str, col: int, val: int) -> List[int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        col = _normalize_col_index(col)
        rs = _drain_rowset(_lib.mdb_scan_eq(
            self._h, table_b, ctypes.c_uint16(col), ctypes.c_uint32(val)))
        if rs is None:
            raise MdbError(_last_error_msg(self._h))
        return rs

    def scan_between(self, table: str, col: int, lo: int, hi: int) -> List[int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        col = _normalize_col_index(col)
        rs = _drain_rowset(_lib.mdb_scan_between(
            self._h, table_b,
            ctypes.c_uint16(col), ctypes.c_uint32(lo), ctypes.c_uint32(hi)))
        if rs is None:
            raise MdbError(_last_error_msg(self._h))
        return rs

    def scan_eq_string(self, table: str, col: int, needle: str) -> List[int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        col = _normalize_col_index(col)
        if not isinstance(needle, (str, bytes, bytearray)):
            raise ValueError("needle must be str/bytes")
        b  = needle.encode("utf-8") if isinstance(needle, str) else bytes(needle)
        rs = _drain_rowset(_lib.mdb_scan_eq_string(
            self._h, table_b, ctypes.c_uint16(col), b))
        if rs is None:
            raise MdbError(_last_error_msg(self._h))
        return rs

    def where_and(self, table: str, predicates: List[Predicate]) -> List[int]:
        """Compound AND scan.  Build predicates with Predicate.eq / .between / .eq_string."""
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        predicates = _require_predicates(predicates)
        n   = len(predicates)
        arr = (_MdbPredicate * n)(*[p._struct for p in predicates])
        # Keep Predicate objects (and their _str_ref) alive across the call.
        rs  = _drain_rowset(_lib.mdb_where_and(
            self._h, table_b, arr, ctypes.c_uint32(n)))
        _ = predicates  # explicit keep-alive
        if rs is None:
            raise MdbError(_last_error_msg(self._h))
        return rs

    def where_or(self, table: str, predicates: List[Predicate]) -> List[int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        predicates = _require_predicates(predicates)
        n   = len(predicates)
        arr = (_MdbPredicate * n)(*[p._struct for p in predicates])
        rs  = _drain_rowset(_lib.mdb_where_or(
            self._h, table_b, arr, ctypes.c_uint32(n)))
        _ = predicates
        if rs is None:
            raise MdbError(_last_error_msg(self._h))
        return rs

    # ── Aggregations ───────────────────────────────────────────────────────────

    def sum(self, table: str, col: int) -> int:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        col = _normalize_col_index(col)
        out = ctypes.c_uint32(0)
        _check(_lib.mdb_sum(
            self._h, table_b, ctypes.c_uint16(col),
            ctypes.byref(out)), self._h)
        return out.value

    def min(self, table: str, col: int) -> int:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        col = _normalize_col_index(col)
        out = ctypes.c_uint32(0)
        _check(_lib.mdb_min(
            self._h, table_b, ctypes.c_uint16(col),
            ctypes.byref(out)), self._h)
        return out.value

    def max(self, table: str, col: int) -> int:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        col = _normalize_col_index(col)
        out = ctypes.c_uint32(0)
        _check(_lib.mdb_max(
            self._h, table_b, ctypes.c_uint16(col),
            ctypes.byref(out)), self._h)
        return out.value

    # ── GroupBy ────────────────────────────────────────────────────────────────

    def group_count(self, table: str, key_col: int) -> Dict[int, int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        key_col = _normalize_col_index(key_col, "key")
        gr = _drain_group_result(_lib.mdb_group_count(
            self._h, table_b, ctypes.c_uint16(key_col)))
        if gr is None:
            raise MdbError(_last_error_msg(self._h))
        return gr

    def group_sum(self, table: str, key_col: int, val_col: int) -> Dict[int, int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        key_col = _normalize_col_index(key_col, "key")
        val_col = _normalize_col_index(val_col, "value")
        gr = _drain_group_result(_lib.mdb_group_sum(
            self._h, table_b,
            ctypes.c_uint16(key_col), ctypes.c_uint16(val_col)))
        if gr is None:
            raise MdbError(_last_error_msg(self._h))
        return gr

    def group_min(self, table: str, key_col: int, val_col: int) -> Dict[int, int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        key_col = _normalize_col_index(key_col, "key")
        val_col = _normalize_col_index(val_col, "value")
        gr = _drain_group_result(_lib.mdb_group_min(
            self._h, table_b,
            ctypes.c_uint16(key_col), ctypes.c_uint16(val_col)))
        if gr is None:
            raise MdbError(_last_error_msg(self._h))
        return gr

    def group_max(self, table: str, key_col: int, val_col: int) -> Dict[int, int]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        key_col = _normalize_col_index(key_col, "key")
        val_col = _normalize_col_index(val_col, "value")
        gr = _drain_group_result(_lib.mdb_group_max(
            self._h, table_b,
            ctypes.c_uint16(key_col), ctypes.c_uint16(val_col)))
        if gr is None:
            raise MdbError(_last_error_msg(self._h))
        return gr

    def group_avg(self, table: str, key_col: int, val_col: int) -> Dict[int, float]:
        _require_open_handle(self._h)
        table_b = _encode_name(table)
        key_col = _normalize_col_index(key_col, "key")
        val_col = _normalize_col_index(val_col, "value")
        gr = _drain_group_resultf(_lib.mdb_group_avg(
            self._h, table_b,
            ctypes.c_uint16(key_col), ctypes.c_uint16(val_col)))
        if gr is None:
            raise MdbError(_last_error_msg(self._h))
        return gr

    # ── Join ───────────────────────────────────────────────────────────────────

    def join(self, left: str, left_col: int,
             right: str, right_col: int) -> List[Tuple[int, int]]:
        _require_open_handle(self._h)
        left_b = _encode_name(left, "left table")
        right_b = _encode_name(right, "right table")
        left_col = _normalize_col_index(left_col, "left")
        right_col = _normalize_col_index(right_col, "right")
        jr = _drain_join_result(_lib.mdb_join(
            self._h,
            left_b,  ctypes.c_uint16(left_col),
            right_b, ctypes.c_uint16(right_col)))
        if jr is None:
            raise MdbError(_last_error_msg(self._h))
        return jr
