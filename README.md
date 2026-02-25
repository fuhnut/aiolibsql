# aiolibsql

A high-performance, fully asynchronous Python wrapper for [libSQL](https://github.com/tursodatabase/libsql), built with Rust and PyO3.

This is an async fork of the official `libsql-python` SDK, designed to fix database locking issues in `asyncio` applications like Discord bots.

## Features

- **True Async/Await** — All database operations are non-blocking coroutines
- **High-Performance Connection Pool** — Rust-native pooling with automatic read/write splitting
- **True Concurrency** — Round-robin multiple reader connections while writing
- **Async Context Manager** — `async with await aiolibsql.connect(...) as conn:`
- **Thread-Safe** — Uses `Arc<Mutex>` for concurrent access
- **Multiple Backends** — Local files, remote Turso, embedded replicas, encrypted databases
- **Full Type Support** — `TEXT`, `INTEGER`, `REAL`, `BLOB`, `NULL`
- **DB-API 2.0 Style** — Familiar `execute`, `fetchone`, `fetchall`, `cursor` interface
- **SQLAlchemy Compatible** — Use with `create_engine("sqlite+aiolibsql://")`

## Installation

### From GitHub (recommended)

```bash
pip install "aiolibsql @ git+https://github.com/fuhnut/aiolibsql"
```

### Build Requirements

Since this package compiles native Rust code, you need the following installed:

| Dependency | Install Command (Debian/Ubuntu) |
|---|---|
| **Rust & Cargo** | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| **CMake** | `apt install cmake` |
| **C/C++ Compiler** | `apt install build-essential` |
| **Python Dev Headers** | `apt install python3-dev` |

> [!NOTE]
> On macOS, install Xcode Command Line Tools (`xcode-select --install`) and CMake (`brew install cmake`).

## Quick Start

```python
import asyncio
import aiolibsql

async def main():
    async with await aiolibsql.connect("hello.db") as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))

        cursor = await conn.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print(rows)

asyncio.run(main())
```

## Connection Modes

```python
# Local file
conn = await aiolibsql.connect("data.db")

# In-memory
conn = await aiolibsql.connect(":memory:")

# Remote Turso
conn = await aiolibsql.connect("libsql://your-db.turso.io", auth_token="...")

# Embedded replica (local + remote sync)
conn = await aiolibsql.connect("local.db", sync_url="libsql://your-db.turso.io", auth_token="...")

# Encrypted local
conn = await aiolibsql.connect("secret.db", encryption_key="my-key")

# Connection Pool (Recommended for high load)
# Size=10 creates 1 writer + 9 reader connections
pool = await aiolibsql.create_pool("bot.db", size=10)
```

## Scaling with ConnectionPool

`aiolibsql` v0.2.0 introduces a native Rust `ConnectionPool` designed for major scalability on local `.db` files.

- **Read/Write Splitting**: SELECT queries are round-robbined across reader connections. INSERT/UPDATE/DELETE are funneled through a high-priority writer.
- **Concurrent Reads**: Multiple readers can run truly in parallel without blocking the writer (WAL mode).
- **Atomic Batches**: Use `pool.executebatch()` to group multiple writes into a single high-speed transaction.
- **Auto-Optimization**: Pools auto-apply optimal PRAGMAs (`WAL`, `synchronous=NORMAL`, `mmap_size`, etc.).

**Estimated Scale**: A single `.db` file can now comfortably support **2,000 - 3,000 discord servers** (depending on hardware/IOPS) without `SQLITE_BUSY` errors.

## API Reference

### `connect()` Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `database` | `str` | *required* | Path, `:memory:`, or `libsql://` URL |
| `timeout` | `float` | `5.0` | Busy timeout in seconds |
| `isolation_level` | `str \| None` | `"DEFERRED"` | Transaction isolation mode |
| `sync_url` | `str \| None` | `None` | Remote URL for embedded replica |
| `sync_interval` | `float \| None` | `None` | Auto-sync interval (seconds) |
| `offline` | `bool` | `False` | Read-only replica mode |
| `auth_token` | `str \| None` | `None` | Auth token for Turso |
| `encryption_key` | `str \| None` | `None` | AES encryption key |
| `autocommit` | `int` | `-1` | `1` (on), `0` (off), `-1` (legacy) |

### Module Constants

| Constant | Value |
|---|---|
| `aiolibsql.VERSION` | `"0.2.0"` |
| `aiolibsql.paramstyle` | `"qmark"` |
| `aiolibsql.sqlite_version_info` | `(3, 42, 0)` |
| `aiolibsql.LEGACY_TRANSACTION_CONTROL` | `-1` |
| `aiolibsql.Error` | Base exception class |

### Connection

| Method / Property | Description |
|---|---|
| `await conn.execute(sql, params?)` | Execute a SQL statement, returns `Cursor` |
| `await conn.executemany(sql, params_list)` | Execute for each param set |
| `await conn.executescript(script)` | Execute multiple statements at once |
| `await conn.commit()` | Commit the current transaction |
| `await conn.rollback()` | Rollback the current transaction |
| `await conn.sync()` | Sync with remote (replicas only) |
| `await conn.close()` | Close the connection |
| `conn.cursor()` | Create a new `Cursor` *(sync)* |
| `conn.isolation_level` | Current isolation level (read-only) |
| `conn.in_transaction` | `True` if inside a transaction |
| `conn.autocommit` | Get/set autocommit mode |

### ConnectionPool (v0.2.0+)

| Method / Property | Description |
|---|---|
| `await pool.execute(sql, params?)` | Execute via pool, returns `PoolCursor` |
| `await pool.executemany(sql, ps)` | Execute for each param set in one tx |
| `await pool.executebatch(ops)` | Execute list of `(sql, params)` in one tx |
| `await pool.close()` | Close all pooled connections |
| `pool.size` | Total connections in pool |
| `pool.reader_count` | Number of reader connections |

### Cursor

| Method / Property | Description |
|---|---|
| `await cursor.execute(sql, params?)` | Execute a statement |
| `await cursor.executemany(sql, params_list)` | Execute for each param set |
| `await cursor.executescript(script)` | Execute multiple statements |
| `await cursor.fetchone()` | Fetch the next row (or `None`) |
| `await cursor.fetchmany(size?)` | Fetch `size` rows (default: `arraysize`) |
| `await cursor.fetchall()` | Fetch all remaining rows |
| `await cursor.close()` | Close the cursor |
| `cursor.description` | Column metadata (after SELECT) |
| `cursor.lastrowid` | Row ID of last INSERT |
| `cursor.rowcount` | Number of rows affected |
| `cursor.arraysize` | Default fetch size (get/set) |

### Supported Parameter Types

| Python | SQLite |
|---|---|
| `None` | `NULL` |
| `str` | `TEXT` |
| `int` | `INTEGER` |
| `float` | `REAL` |
| `bytes` | `BLOB` |

## SQLAlchemy Integration

aiolibsql includes a SQLAlchemy dialect. Use `sqlite+aiolibsql://` as the connection URL:

```python
import aiolibsql_sqlalchemy  # registers the dialect

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

engine = create_engine("sqlite+aiolibsql://", echo=True)  # in-memory
# engine = create_engine("sqlite+aiolibsql:///data.db")    # local file

with Session(engine) as session:
    session.execute(text("CREATE TABLE t (x INTEGER)"))
    session.execute(text("INSERT INTO t VALUES (:x)"), {"x": 42})
    session.commit()
    result = session.execute(text("SELECT * FROM t"))
    print(result.fetchall())
```

See [`examples/sqlalchemy/`](examples/sqlalchemy/) for a full ORM example.

## Differences from `libsql`

1. **Import**: `import aiolibsql` instead of `import libsql`
2. **Await everything**: Every database call is a coroutine
3. **Connection**: `aiolibsql.connect()` returns a coroutine — use `await`
4. **Context manager**: Use `async with` instead of `with`

## License

MIT
