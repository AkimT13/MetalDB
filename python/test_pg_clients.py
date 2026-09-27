"""
test_pg_clients.py — `mdb pgserve` against real PostgreSQL drivers.

Starts the server from the given build directory and exercises psycopg 3 (extended
query protocol: server-side parameter binding, binary results, prepared statements,
named cursors) and psycopg2 (simple query protocol with client-side quoting). Each
driver is skipped if it is not installed.

    python3 python/test_pg_clients.py src/build-cpu      # or src/ for the Metal build
"""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

_failed = 0


def check(cond, msg):
    global _failed
    if not cond:
        print(f"  FAIL  {msg}", file=sys.stderr)
        _failed += 1


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def run_psycopg3(port):
    try:
        import psycopg
    except ImportError:
        print("SKIP psycopg 3 not installed")
        return
    conn = psycopg.connect(host="127.0.0.1", port=port, user="app", dbname="x", autocommit=True)
    cur = conn.cursor()
    cur.execute("CREATE TABLE people (UINT32, STRING, DOUBLE, INT64)")
    cur.executemany("INSERT INTO people VALUES (%s, %s, %s, %s)",
                    [(i, f"name{i}", i * 1.5, -i * 10_000_000_000) for i in range(1, 51)])
    cur.execute("INSERT INTO people VALUES (%s, %s, %s, %s)", (99, "O'Hara", 0.1, 0))
    check(cur.rowcount == 1, "psycopg3 rowcount")

    cur.execute("SELECT c0, c1, c2, c3 FROM people WHERE c0 BETWEEN %s AND %s ORDER BY c0", (2, 3))
    check([d.type_code for d in cur.description] == [20, 25, 701, 20], "psycopg3 column OIDs")
    check(cur.fetchall() == [(2, "name2", 3.0, -20_000_000_000), (3, "name3", 4.5, -30_000_000_000)],
          "psycopg3 typed rows")

    cur.execute("SELECT c1 FROM people WHERE c1 = %s", ("O'Hara",))
    check(cur.fetchall() == [("O'Hara",)], "psycopg3 string parameter with quote")

    bcur = conn.cursor(binary=True)
    bcur.execute("SELECT c0, c2, count(*) FROM people WHERE c0 <= %s GROUP BY c0, c2 ORDER BY c0", (2,))
    check(bcur.fetchall() == [(1, 1.5, 1), (2, 3.0, 1)], "psycopg3 binary results")

    for i in range(3):
        cur.execute("SELECT count(*) FROM people WHERE c0 > %s", (i,), prepare=True)
        check(cur.fetchone() == (51 - i,), f"psycopg3 prepared statement run {i}")

    with conn.cursor(name="stream") as sc:
        sc.itersize = 7
        sc.execute("SELECT c0 FROM people WHERE c0 <= %s ORDER BY c0", (20,))
        check([r[0] for r in sc] == list(range(1, 21)), "psycopg3 named cursor")

    # Catalog introspection through bound parameters (extended protocol).
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", ("people",))
    check(cur.fetchall() == [("c0", "bigint"), ("c1", "text"), ("c2", "double precision"), ("c3", "bigint")],
          "psycopg3 information_schema.columns")
    cur.execute("SELECT version()")
    check("MetalDB" in cur.fetchone()[0], "psycopg3 version()")

    try:
        cur.execute("SELECT * FROM missing WHERE c0 = %s", (1,))
        check(False, "psycopg3 missing table should raise")
    except psycopg.Error as ex:
        check(ex.sqlstate == "42P01", "psycopg3 SQLSTATE")
    conn.close()
    print("PASS psycopg 3")


def run_psycopg2(port):
    try:
        import psycopg2
    except ImportError:
        print("SKIP psycopg2 not installed")
        return
    conn = psycopg2.connect(host="127.0.0.1", port=port, user="app", dbname="x")
    cur = conn.cursor()
    cur.execute("SELECT count(*), sum(c0) FROM people WHERE c1 != %s", ("O'Hara",))
    check(cur.fetchone() == (50, 1275), "psycopg2 aggregate")
    conn.commit()
    conn.close()
    print("PASS psycopg2")


def main():
    build_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "src")
    mdb = os.path.abspath(os.path.join(build_dir, "mdb"))
    data = tempfile.mkdtemp(prefix="mdb_pgclients_")
    port = free_port()
    server = subprocess.Popen([mdb, "pgserve", str(port), "--data-dir", data], stdout=subprocess.DEVNULL)
    try:
        wait_for(port)
        run_psycopg3(port)
        run_psycopg2(port)
    finally:
        server.terminate()
        code = server.wait(timeout=10)
        check(code == 0, f"server exit code {code}")
        shutil.rmtree(data, ignore_errors=True)
    if _failed:
        print(f"\n{_failed} check(s) FAILED", file=sys.stderr)
        sys.exit(1)
    print("All PostgreSQL client tests passed.")


if __name__ == "__main__":
    main()
