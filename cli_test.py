"""
aiolibsql — Comprehensive CLI Test
Tests every exposed function and property of the aiolibsql library.
"""

import asyncio
import aiolibsql

PASS = "✅"
FAIL = "❌"
results = []


def report(name: str, passed: bool, detail: str = ""):
    status = PASS if passed else FAIL
    results.append((name, passed))
    msg = f"  {status} {name}"
    if detail:
        msg += f"  →  {detail}"
    print(msg)


async def main():
    print("=" * 60)
    print("  aiolibsql — Full API Test Suite")
    print("=" * 60)

    # ──────────────────────────────────────────────
    # 1. connect()
    # ──────────────────────────────────────────────
    print("\n── Module Functions ──")

    try:
        conn = await aiolibsql.connect(":memory:")
        report("connect()", True, "in-memory database opened")
    except Exception as e:
        report("connect()", False, str(e))
        return

    # ──────────────────────────────────────────────
    # 2. Connection properties
    # ──────────────────────────────────────────────
    print("\n── Connection Properties ──")

    try:
        il = conn.isolation_level
        report("isolation_level (getter)", True, f"value={il!r}")
    except Exception as e:
        report("isolation_level (getter)", False, str(e))

    try:
        ac = conn.autocommit
        report("autocommit (getter)", True, f"value={ac}")
    except Exception as e:
        report("autocommit (getter)", False, str(e))

    try:
        conn.autocommit = 1
        assert conn.autocommit == 1
        conn.autocommit = 0
        assert conn.autocommit == 0
        conn.autocommit = aiolibsql.LEGACY_TRANSACTION_CONTROL
        report("autocommit (setter)", True, "set to 1, 0, and LEGACY")
    except Exception as e:
        report("autocommit (setter)", False, str(e))

    try:
        it = conn.in_transaction
        report("in_transaction (getter)", True, f"value={it}")
    except Exception as e:
        report("in_transaction (getter)", False, str(e))

    # ──────────────────────────────────────────────
    # 3. Connection.cursor()
    # ──────────────────────────────────────────────
    print("\n── Cursor Creation ──")

    try:
        cur = conn.cursor()
        report("cursor()", True, f"type={type(cur).__name__}")
    except Exception as e:
        report("cursor()", False, str(e))

    # ──────────────────────────────────────────────
    # 4. Connection.execute() — CREATE TABLE
    # ──────────────────────────────────────────────
    print("\n── Connection.execute() ──")

    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                name  TEXT NOT NULL,
                age   INTEGER,
                score REAL,
                data  BLOB
            )
        """)
        report("execute() — CREATE TABLE", True)
    except Exception as e:
        report("execute() — CREATE TABLE", False, str(e))

    # INSERT with tuple params
    try:
        cursor = await conn.execute(
            "INSERT INTO test_table (name, age, score) VALUES (?, ?, ?)",
            ("Alice", 28, 95.5),
        )
        report("execute() — INSERT (tuple params)", True)
    except Exception as e:
        report("execute() — INSERT (tuple params)", False, str(e))

    # INSERT with list params
    try:
        cursor = await conn.execute(
            "INSERT INTO test_table (name, age, score) VALUES (?, ?, ?)",
            ["Bob", 34, 88.0],
        )
        report("execute() — INSERT (list params)", True)
    except Exception as e:
        report("execute() — INSERT (list params)", False, str(e))

    # INSERT with NULL
    try:
        await conn.execute(
            "INSERT INTO test_table (name, age, score) VALUES (?, ?, ?)",
            ("Charlie", None, None),
        )
        report("execute() — INSERT (NULL values)", True)
    except Exception as e:
        report("execute() — INSERT (NULL values)", False, str(e))

    # INSERT with BLOB
    try:
        await conn.execute(
            "INSERT INTO test_table (name, age, score, data) VALUES (?, ?, ?, ?)",
            ("Diana", 22, 91.0, b"\x00\x01\x02\x03"),
        )
        report("execute() — INSERT (BLOB data)", True)
    except Exception as e:
        report("execute() — INSERT (BLOB data)", False, str(e))

    # SELECT
    try:
        cursor = await conn.execute("SELECT * FROM test_table")
        report("execute() — SELECT", True)
    except Exception as e:
        report("execute() — SELECT", False, str(e))

    # UPDATE
    try:
        await conn.execute(
            "UPDATE test_table SET age = ? WHERE name = ?", (29, "Alice")
        )
        report("execute() — UPDATE", True)
    except Exception as e:
        report("execute() — UPDATE", False, str(e))

    # DELETE
    try:
        await conn.execute("DELETE FROM test_table WHERE name = ?", ("Charlie",))
        report("execute() — DELETE", True)
    except Exception as e:
        report("execute() — DELETE", False, str(e))

    # ──────────────────────────────────────────────
    # 5. Connection.executemany()
    # ──────────────────────────────────────────────
    print("\n── Connection.executemany() ──")

    try:
        await conn.executemany(
            "INSERT INTO test_table (name, age, score) VALUES (?, ?, ?)",
            [
                ("Eve", 30, 77.0),
                ("Frank", 41, 62.5),
                ("Grace", 25, 99.9),
            ],
        )
        report("executemany()", True, "inserted 3 rows")
    except Exception as e:
        report("executemany()", False, str(e))

    # ──────────────────────────────────────────────
    # 6. Connection.executescript()
    # ──────────────────────────────────────────────
    print("\n── Connection.executescript() ──")

    try:
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS script_table (x INTEGER);
            INSERT INTO script_table VALUES (1);
            INSERT INTO script_table VALUES (2);
            INSERT INTO script_table VALUES (3);
        """)
        report("executescript()", True, "ran multi-statement script")
    except Exception as e:
        report("executescript()", False, str(e))

    # ──────────────────────────────────────────────
    # 7. Cursor.execute()
    # ──────────────────────────────────────────────
    print("\n── Cursor.execute() ──")

    try:
        cur = conn.cursor()
        cur = await cur.execute("SELECT * FROM test_table ORDER BY id")
        report("Cursor.execute()", True)
    except Exception as e:
        report("Cursor.execute()", False, str(e))

    # ──────────────────────────────────────────────
    # 8. Cursor.fetchall()
    # ──────────────────────────────────────────────
    print("\n── Cursor.fetchall() ──")

    try:
        rows = await cur.fetchall()
        report("Cursor.fetchall()", True, f"got {len(rows)} rows")
        print("    Data:")
        for row in rows:
            print(f"      {row}")
    except Exception as e:
        report("Cursor.fetchall()", False, str(e))

    # ──────────────────────────────────────────────
    # 9. Cursor.fetchone()
    # ──────────────────────────────────────────────
    print("\n── Cursor.fetchone() ──")

    try:
        cur = conn.cursor()
        cur = await cur.execute("SELECT * FROM test_table ORDER BY id LIMIT 3")
        row = await cur.fetchone()
        report("Cursor.fetchone()", True, f"first row = {row}")

        # Fetch remaining to exhaust, then check None
        await cur.fetchone()  # 2nd
        await cur.fetchone()  # 3rd
        none_row = await cur.fetchone()  # should be None
        report("Cursor.fetchone() — exhausted", none_row is None, f"value={none_row!r}")
    except Exception as e:
        report("Cursor.fetchone()", False, str(e))

    # ──────────────────────────────────────────────
    # 10. Cursor.fetchmany()
    # ──────────────────────────────────────────────
    print("\n── Cursor.fetchmany() ──")

    try:
        cur = conn.cursor()
        cur = await cur.execute("SELECT * FROM test_table ORDER BY id")
        batch = await cur.fetchmany(2)
        report("Cursor.fetchmany(size=2)", True, f"got {len(batch)} rows")

        # Default size (arraysize=1)
        batch2 = await cur.fetchmany()
        report("Cursor.fetchmany() — default size", True, f"got {len(batch2)} rows")
    except Exception as e:
        report("Cursor.fetchmany()", False, str(e))

    # ──────────────────────────────────────────────
    # 11. Cursor.arraysize
    # ──────────────────────────────────────────────
    print("\n── Cursor.arraysize ──")

    try:
        cur = conn.cursor()
        default = cur.arraysize
        cur.arraysize = 5
        report("Cursor.arraysize (get/set)", True, f"default={default}, set to {cur.arraysize}")
    except Exception as e:
        report("Cursor.arraysize", False, str(e))

    # ──────────────────────────────────────────────
    # 12. Cursor.description
    # ──────────────────────────────────────────────
    print("\n── Cursor.description ──")

    try:
        cur = conn.cursor()
        cur = await cur.execute("SELECT id, name, age, score FROM test_table LIMIT 1")
        desc = cur.description
        report("Cursor.description", desc is not None, f"columns={[d[0] for d in desc] if desc else None}")
    except Exception as e:
        report("Cursor.description", False, str(e))

    # ──────────────────────────────────────────────
    # 13. Cursor.lastrowid
    # ──────────────────────────────────────────────
    print("\n── Cursor.lastrowid ──")

    try:
        cur = conn.cursor()
        cur = await cur.execute(
            "INSERT INTO test_table (name, age, score) VALUES (?, ?, ?)",
            ("Heidi", 27, 85.0),
        )
        rid = cur.lastrowid
        report("Cursor.lastrowid", rid is not None, f"value={rid}")
    except Exception as e:
        report("Cursor.lastrowid", False, str(e))

    # ──────────────────────────────────────────────
    # 14. Cursor.rowcount
    # ──────────────────────────────────────────────
    print("\n── Cursor.rowcount ──")

    try:
        rc = cur.rowcount
        report("Cursor.rowcount", True, f"value={rc}")
    except Exception as e:
        report("Cursor.rowcount", False, str(e))

    # ──────────────────────────────────────────────
    # 15. Cursor.executemany()
    # ──────────────────────────────────────────────
    print("\n── Cursor.executemany() ──")

    try:
        cur = conn.cursor()
        cur = await cur.executemany(
            "INSERT INTO test_table (name, age, score) VALUES (?, ?, ?)",
            [("Ivan", 33, 70.0), ("Judy", 29, 80.0)],
        )
        report("Cursor.executemany()", True, "inserted 2 rows via cursor")
    except Exception as e:
        report("Cursor.executemany()", False, str(e))

    # ──────────────────────────────────────────────
    # 16. Cursor.executescript()
    # ──────────────────────────────────────────────
    print("\n── Cursor.executescript() ──")

    try:
        cur = conn.cursor()
        cur = await cur.executescript("""
            CREATE TABLE IF NOT EXISTS cur_script (val TEXT);
            INSERT INTO cur_script VALUES ('hello');
            INSERT INTO cur_script VALUES ('world');
        """)
        report("Cursor.executescript()", True)
    except Exception as e:
        report("Cursor.executescript()", False, str(e))

    # ──────────────────────────────────────────────
    # 17. Connection.commit()
    # ──────────────────────────────────────────────
    print("\n── Connection.commit() ──")

    try:
        await conn.commit()
        report("commit()", True)
    except Exception as e:
        report("commit()", False, str(e))

    # ──────────────────────────────────────────────
    # 18. Connection.rollback()
    # ──────────────────────────────────────────────
    print("\n── Connection.rollback() ──")

    try:
        await conn.rollback()
        report("rollback()", True)
    except Exception as e:
        report("rollback()", False, str(e))

    # ──────────────────────────────────────────────
    # 19. Cursor.close()
    # ──────────────────────────────────────────────
    print("\n── Cursor.close() ──")

    try:
        cur = conn.cursor()
        await cur.close()
        report("Cursor.close()", True)
    except Exception as e:
        report("Cursor.close()", False, str(e))

    # ──────────────────────────────────────────────
    # 20. Connection.close()
    # ──────────────────────────────────────────────
    print("\n── Connection.close() ──")

    try:
        await conn.close()
        report("Connection.close()", True)
    except Exception as e:
        report("Connection.close()", False, str(e))

    # ──────────────────────────────────────────────
    # 21. async with (context manager)
    # ──────────────────────────────────────────────
    print("\n── Async Context Manager ──")

    try:
        async with await aiolibsql.connect(":memory:") as ctx_conn:
            await ctx_conn.execute("CREATE TABLE ctx_test (id INTEGER)")
            await ctx_conn.execute("INSERT INTO ctx_test VALUES (?)", (42,))
            cursor = await ctx_conn.execute("SELECT * FROM ctx_test")
            rows = await cursor.fetchall()
            assert rows[0][0] == 42
        report("async with (__aenter__/__aexit__)", True, "context manager works")
    except Exception as e:
        report("async with (__aenter__/__aexit__)", False, str(e))

    # ──────────────────────────────────────────────
    # 22. Module constants
    # ──────────────────────────────────────────────
    print("\n── Module Constants ──")

    try:
        ltc = aiolibsql.LEGACY_TRANSACTION_CONTROL
        report("LEGACY_TRANSACTION_CONTROL", ltc == -1, f"value={ltc}")
    except Exception as e:
        report("LEGACY_TRANSACTION_CONTROL", False, str(e))

    try:
        ps = aiolibsql.paramstyle
        report("paramstyle", ps == "qmark", f"value={ps!r}")
    except Exception as e:
        report("paramstyle", False, str(e))

    try:
        svi = aiolibsql.sqlite_version_info
        report("sqlite_version_info", True, f"value={svi}")
    except Exception as e:
        report("sqlite_version_info", False, str(e))

    try:
        err = aiolibsql.Error
        report("Error exception class", err is not None, f"type={err}")
    except Exception as e:
        report("Error exception class", False, str(e))

    try:
        ver = aiolibsql.VERSION
        report("VERSION", isinstance(ver, str) and len(ver) > 0, f"value={ver!r}")
    except Exception as e:
        report("VERSION", False, str(e))

    # ──────────────────────────────────────────────
    # Summary
    # ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    passed = sum(1 for _, p in results if p)
    total = len(results)
    print(f"  Results: {passed}/{total} passed")
    if passed == total:
        print("  🎉 ALL TESTS PASSED!")
    else:
        print("  ⚠️  Some tests failed:")
        for name, p in results:
            if not p:
                print(f"    {FAIL} {name}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
