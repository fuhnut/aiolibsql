# `aiolibsql` API Reference

## Module

### `await aiolibsql.connect(database, ...) → Connection`

Opens a database connection. Returns a coroutine that resolves to a `Connection`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `database` | `str` | *required* | Path to local file, `:memory:`, or `libsql://` URL |
| `timeout` | `float` | `5.0` | Busy timeout in seconds |
| `isolation_level` | `str \| None` | `"DEFERRED"` | `"DEFERRED"`, `"IMMEDIATE"`, `"EXCLUSIVE"`, or `None` (autocommit) |
| `sync_url` | `str \| None` | `None` | Remote URL for embedded replica sync |
| `sync_interval` | `float \| None` | `None` | Auto-sync interval in seconds |
| `offline` | `bool` | `False` | If `True`, replica is read-only (no remote writes) |
| `auth_token` | `str \| None` | `None` | Auth token for Turso / remote connections |
| `encryption_key` | `str \| None` | `None` | AES encryption key for local databases |
| `autocommit` | `int` | `-1` | `1` (on), `0` (off), or `-1` (legacy mode) |

**Connection modes:**

```python
# Local file
conn = await aiolibsql.connect("data.db")

# In-memory
conn = await aiolibsql.connect(":memory:")

# Remote (Turso)
conn = await aiolibsql.connect("libsql://your-db.turso.io", auth_token="...")

# Embedded replica (local + remote sync)
conn = await aiolibsql.connect("local.db", sync_url="libsql://your-db.turso.io", auth_token="...")

# Encrypted local
conn = await aiolibsql.connect("secret.db", encryption_key="my-key")
```

### Module Constants

| Constant | Value | Description |
|---|---|---|
| `aiolibsql.VERSION` | `"0.1.14-stable"` | Library version |
| `aiolibsql.LEGACY_TRANSACTION_CONTROL` | `-1` | Legacy autocommit mode |
| `aiolibsql.paramstyle` | `"qmark"` | Use `?` for parameter placeholders |
| `aiolibsql.sqlite_version_info` | `(3, 42, 0)` | Underlying SQLite version |
| `aiolibsql.Error` | Exception | Base exception class |

---

## `Connection`

### Methods

| Method | Description |
|---|---|
| `await conn.execute(sql, params?)` | Execute a SQL statement, returns `Cursor` |
| `await conn.executemany(sql, params_list)` | Execute for each param set, returns `Cursor` |
| `await conn.executescript(script)` | Execute multiple `;`-separated statements |
| `conn.cursor()` | Create a new `Cursor` *(sync — no await)* |
| `await conn.commit()` | Commit the current transaction |
| `await conn.rollback()` | Rollback the current transaction |
| `await conn.sync()` | Sync embedded replica with remote |
| `await conn.close()` | Close the connection |

### Properties

| Property | Type | Access | Description |
|---|---|---|---|
| `conn.isolation_level` | `str \| None` | read | Current isolation level |
| `conn.in_transaction` | `bool` | read | `True` if inside a transaction |
| `conn.autocommit` | `int` | read/write | Autocommit mode (`0`, `1`, or `-1`) |

### Async Context Manager

```python
async with await aiolibsql.connect("data.db") as conn:
    await conn.execute("INSERT INTO t VALUES (?)", (1,))
    # auto-commits on clean exit, auto-rollbacks on exception
```

---

## `Cursor`

### Methods

| Method | Description |
|---|---|
| `await cursor.execute(sql, params?)` | Execute a statement, returns self |
| `await cursor.executemany(sql, params_list)` | Execute for each param set |
| `await cursor.executescript(script)` | Execute multiple statements |
| `await cursor.fetchone()` | Fetch next row as `tuple` (or `None`) |
| `await cursor.fetchmany(size?)` | Fetch `size` rows (default: `arraysize`) |
| `await cursor.fetchall()` | Fetch all remaining rows as list of tuples |
| `await cursor.close()` | Release cursor resources |

### Properties

| Property | Type | Access | Description |
|---|---|---|---|
| `cursor.description` | `tuple \| None` | read | Column metadata (name, ...) after SELECT |
| `cursor.lastrowid` | `int` | read | Row ID of last INSERT |
| `cursor.rowcount` | `int` | read | Number of rows affected |
| `cursor.arraysize` | `int` | read/write | Default batch size for `fetchmany()` |

---

## Supported Parameter Types

Parameters are passed as a `list` or `tuple` using `?` placeholders:

```python
await conn.execute("INSERT INTO t VALUES (?, ?, ?, ?, ?)", (
    None,           # NULL
    "hello",        # TEXT
    42,             # INTEGER
    3.14,           # REAL
    b"\xde\xad",   # BLOB
))
```

| Python Type | SQLite Type |
|---|---|
| `None` | `NULL` |
| `str` | `TEXT` |
| `int` | `INTEGER` |
| `float` | `REAL` |
| `bytes` | `BLOB` |
