#!/usr/bin/env python3
"""
aiolibsql — Async Test Suite (pytest + pytest-asyncio)
Tests every exposed function and property of the aiolibsql library.

Run with: pytest tests/test_suite.py -v
"""

import sys
import pytest
import pytest_asyncio
import aiolibsql


@pytest_asyncio.fixture
async def conn():
    c = await aiolibsql.connect(":memory:", autocommit=1)
    yield c
    await c.close()


@pytest.mark.asyncio
async def test_connect():
    conn = await aiolibsql.connect(":memory:")
    assert conn is not None
    await conn.close()


@pytest.mark.asyncio
async def test_connect_timeout():
    conn = await aiolibsql.connect(":memory:", timeout=1.0)
    await conn.close()


@pytest.mark.asyncio
async def test_connect_local(tmp_path):
    db = str(tmp_path / "test.db")
    conn = await aiolibsql.connect(db)
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.execute("INSERT INTO t VALUES (1)")
    cursor = await conn.execute("SELECT * FROM t")
    rows = await cursor.fetchall()
    assert rows == [(1,)]
    await conn.close()


@pytest.mark.asyncio
async def test_execute_create(conn):
    await conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    row = await cursor.fetchone()
    assert row[0] == "users"


@pytest.mark.asyncio
async def test_execute_insert_tuple_params(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    cursor = await conn.execute("INSERT INTO t VALUES (?, ?)", (1, "Alice"))
    assert cursor.lastrowid == 1


@pytest.mark.asyncio
async def test_execute_insert_list_params(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    cursor = await conn.execute("INSERT INTO t VALUES (?, ?)", [2, "Bob"])
    assert cursor.lastrowid == 1


@pytest.mark.asyncio
async def test_execute_insert_null(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    await conn.execute("INSERT INTO t VALUES (?, ?)", (1, None))
    cursor = await conn.execute("SELECT * FROM t")
    row = await cursor.fetchone()
    assert row == (1, None)


@pytest.mark.asyncio
async def test_execute_insert_blob(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, data BLOB)")
    await conn.execute("INSERT INTO t VALUES (?, ?)", (1, b"\x00\x01\x02"))
    cursor = await conn.execute("SELECT * FROM t")
    row = await cursor.fetchone()
    assert row[0] == 1
    assert row[1] == b"\x00\x01\x02"


@pytest.mark.asyncio
async def test_execute_update(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    await conn.execute("INSERT INTO t VALUES (1, 'old')")
    await conn.execute("UPDATE t SET name = ? WHERE id = ?", ("new", 1))
    cursor = await conn.execute("SELECT name FROM t WHERE id = 1")
    row = await cursor.fetchone()
    assert row[0] == "new"


@pytest.mark.asyncio
async def test_execute_delete(conn):
    await conn.execute("CREATE TABLE t (id INTEGER)")
    await conn.execute("INSERT INTO t VALUES (1)")
    await conn.execute("INSERT INTO t VALUES (2)")
    await conn.execute("DELETE FROM t WHERE id = ?", (1,))
    cursor = await conn.execute("SELECT COUNT(*) FROM t")
    row = await cursor.fetchone()
    assert row[0] == 1


@pytest.mark.asyncio
async def test_executemany(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    await conn.executemany(
        "INSERT INTO t VALUES (?, ?)",
        [(1, "Alice"), (2, "Bob"), (3, "Carol")],
    )
    cursor = await conn.execute("SELECT COUNT(*) FROM t")
    row = await cursor.fetchone()
    assert row[0] == 3


@pytest.mark.asyncio
async def test_executescript(conn):
    await conn.executescript("""
        CREATE TABLE t1 (x INTEGER);
        INSERT INTO t1 VALUES (1);
        INSERT INTO t1 VALUES (2);
        CREATE TABLE t2 (y TEXT);
        INSERT INTO t2 VALUES ('hello');
    """)
    cursor = await conn.execute("SELECT COUNT(*) FROM t1")
    assert (await cursor.fetchone())[0] == 2
    cursor = await conn.execute("SELECT * FROM t2")
    assert (await cursor.fetchone())[0] == "hello"


@pytest.mark.asyncio
async def test_cursor_execute(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    cur = conn.cursor()
    await cur.execute("INSERT INTO t VALUES (?, ?)", (1, "Alice"))
    await cur.execute("SELECT * FROM t")
    row = await cur.fetchone()
    assert row == (1, "Alice")


@pytest.mark.asyncio
async def test_cursor_executemany(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    cur = conn.cursor()
    await cur.executemany(
        "INSERT INTO t VALUES (?, ?)",
        [(1, "Alice"), (2, "Bob")],
    )
    await cur.execute("SELECT COUNT(*) FROM t")
    assert (await cur.fetchone())[0] == 2


@pytest.mark.asyncio
async def test_cursor_executescript(conn):
    cur = conn.cursor()
    await cur.executescript("""
        CREATE TABLE t (x INTEGER);
        INSERT INTO t VALUES (10);
        INSERT INTO t VALUES (20);
    """)
    await cur.execute("SELECT * FROM t ORDER BY x")
    rows = await cur.fetchall()
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_fetchone(conn):
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.execute("INSERT INTO t VALUES (1)")
    await conn.execute("INSERT INTO t VALUES (2)")
    cursor = await conn.execute("SELECT * FROM t ORDER BY x")
    assert (await cursor.fetchone()) == (1,)
    assert (await cursor.fetchone()) == (2,)
    assert (await cursor.fetchone()) is None


@pytest.mark.asyncio
async def test_fetchall(conn):
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
    cursor = await conn.execute("SELECT * FROM t ORDER BY x")
    rows = await cursor.fetchall()
    assert rows == [(1,), (2,), (3,)]


@pytest.mark.asyncio
async def test_fetchmany(conn):
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(5)])
    cursor = await conn.execute("SELECT * FROM t ORDER BY x")
    batch1 = await cursor.fetchmany(2)
    assert len(batch1) == 2
    batch2 = await cursor.fetchmany(2)
    assert len(batch2) == 2
    batch3 = await cursor.fetchmany(2)
    assert len(batch3) == 1
    batch4 = await cursor.fetchmany(2)
    assert len(batch4) == 0


@pytest.mark.asyncio
async def test_fetchmany_default_arraysize(conn):
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(3)])
    cur = conn.cursor()
    cur.arraysize = 2
    await cur.execute("SELECT * FROM t ORDER BY x")
    batch = await cur.fetchmany()
    assert len(batch) == 2


@pytest.mark.asyncio
async def test_description(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
    await conn.execute("INSERT INTO t VALUES (1, 'Alice', 95.5)")
    cursor = await conn.execute("SELECT id, name, score FROM t")
    desc = cursor.description
    assert desc is not None
    col_names = [d[0] for d in desc]
    assert col_names == ["id", "name", "score"]


@pytest.mark.asyncio
async def test_lastrowid(conn):
    await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    cursor = await conn.execute("INSERT INTO t (name) VALUES (?)", ("Alice",))
    assert cursor.lastrowid == 1
    cursor = await conn.execute("INSERT INTO t (name) VALUES (?)", ("Bob",))
    assert cursor.lastrowid == 2


@pytest.mark.asyncio
async def test_rowcount(conn):
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(5)])
    cursor = await conn.execute("DELETE FROM t WHERE x < 3")
    assert cursor.rowcount == 3


@pytest.mark.asyncio
async def test_arraysize(conn):
    cur = conn.cursor()
    assert cur.arraysize == 1
    cur.arraysize = 10
    assert cur.arraysize == 10


@pytest.mark.asyncio
async def test_cursor_close(conn):
    cur = conn.cursor()
    await cur.close()


@pytest.mark.asyncio
async def test_connection_close():
    conn = await aiolibsql.connect(":memory:")
    await conn.close()


@pytest.mark.asyncio
async def test_int64(conn):
    await conn.execute("CREATE TABLE t (id INTEGER, big INTEGER)")
    big_val = 1099511627776  # 1 << 40
    await conn.execute("INSERT INTO t VALUES (1, ?)", (big_val,))
    cursor = await conn.execute("SELECT big FROM t")
    row = await cursor.fetchone()
    assert row[0] == big_val


@pytest.mark.asyncio
async def test_discord_id(conn):
    """Test that large Discord snowflake IDs work correctly as TEXT"""
    await conn.execute("CREATE TABLE t (uid TEXT PRIMARY KEY, coins INTEGER)")
    uid = "521723267762094111"
    await conn.execute("INSERT INTO t VALUES (?, ?)", (uid, 100))
    cursor = await conn.execute("SELECT uid, coins FROM t")
    row = await cursor.fetchone()
    assert row == (uid, 100)


@pytest.mark.asyncio
async def test_async_context_manager():
    async with await aiolibsql.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (x INTEGER)")
        await conn.execute("INSERT INTO t VALUES (42)")
        cursor = await conn.execute("SELECT * FROM t")
        rows = await cursor.fetchall()
        assert rows == [(42,)]


@pytest.mark.asyncio
async def test_commit_rollback():
    conn = await aiolibsql.connect(":memory:", autocommit=0)
    await conn.execute("CREATE TABLE t (x INTEGER)")
    await conn.execute("INSERT INTO t VALUES (1)")
    await conn.commit()
    cursor = await conn.execute("SELECT COUNT(*) FROM t")
    assert (await cursor.fetchone())[0] == 1
    await conn.execute("INSERT INTO t VALUES (2)")
    await conn.rollback()
    await conn.close()


@pytest.mark.asyncio
async def test_isolation_level():
    conn = await aiolibsql.connect(":memory:")
    il = conn.isolation_level
    assert il is not None or il is None  # just check it's accessible
    await conn.close()


@pytest.mark.asyncio
async def test_autocommit_property():
    conn = await aiolibsql.connect(":memory:", autocommit=1)
    assert conn.autocommit == 1
    conn.autocommit = 0
    assert conn.autocommit == 0
    conn.autocommit = aiolibsql.LEGACY_TRANSACTION_CONTROL
    assert conn.autocommit == aiolibsql.LEGACY_TRANSACTION_CONTROL
    await conn.close()


@pytest.mark.asyncio
async def test_in_transaction():
    conn = await aiolibsql.connect(":memory:", autocommit=1)
    it = conn.in_transaction
    assert isinstance(it, bool)
    await conn.close()


def test_module_constants():
    assert aiolibsql.LEGACY_TRANSACTION_CONTROL == -1
    assert aiolibsql.paramstyle == "qmark"
    assert aiolibsql.sqlite_version_info == (3, 42, 0)
    assert aiolibsql.Error is not None
    assert hasattr(aiolibsql, "VERSION")
    assert isinstance(aiolibsql.VERSION, str)


def test_error_exception():
    with pytest.raises(aiolibsql.Error):
        raise aiolibsql.Error("test error")


@pytest.mark.asyncio
async def test_all_param_types(conn):
    await conn.execute("CREATE TABLE t (a, b, c, d, e)")
    await conn.execute(
        "INSERT INTO t VALUES (?, ?, ?, ?, ?)",
        (None, "hello", 42, 3.14, b"\xde\xad"),
    )
    cursor = await conn.execute("SELECT * FROM t")
    row = await cursor.fetchone()
    assert row[0] is None
    assert row[1] == "hello"
    assert row[2] == 42
    assert row[3] == pytest.approx(3.14)
    assert row[4] == b"\xde\xad"
