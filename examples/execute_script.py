"""📘 **execute_script.py**

This module contains **fully explained, step‑by‑step examples** showing how to
use ``aiolibsql`` to run a *SQL script* that contains multiple statements.
The goal is to help newcomers see every detail, including error handling,
transactions, and how to read a file from disk.  

> The examples are written in plain Python 3.11+ and use ``asyncio``.
> Run this file directly with ``python examples/execute_script.py``.

---

### What this example covers

1. Opening a connection to an in‑memory or on‑disk database.
2. Reading a SQL script from a file.
3. Executing the script via ``conn.executescript``.
4. Committing changes (or rolling back on error).
5. Verifying the results with a simple ``SELECT`` query.
6. Demonstrating a second example that intentionally causes an error to
   show how rollback works.

All code is heavily commented so that each line is explained.
"""

import os
import asyncio
import aiolibsql


async def execute_script(conn: aiolibsql.Connection, file_path: os.PathLike):
    """Execute every statement in ``file_path`` on ``conn``.

    Parameters
    ----------
    conn
        An open :class:`aiolibsql.Connection` obtained via
        ``await aiolibsql.connect(...)``.
    file_path
        Path to a UTF-8 encoded text file containing one or more SQL
        statements separated by ``;``.  ``executescript`` will execute the
        whole blob of text exactly as if you pasted it into the SQLite CLI.

    The function **always commits** after the script runs successfully.  If an
    error is raised by ``executescript`` it will propagate and the caller is
    responsible for handling the rollback (see :func:`main_with_error`).
    """

    # ``open`` and ``read`` are synchronous because the file is expected to
    # be small.  If you're reading a HUGE script you could use ``aiofiles``
    # instead, but keep it simple here.
    with open(file_path, "r", encoding="utf-8") as file:
        script = file.read()

    # ``executescript`` runs all of the SQL in a single call.  It is roughly
    # equivalent to running ``conn.executescript`` in the sqlite3 standard
    # library.
    await conn.executescript(script)
    # Always commit.  ``executescript`` does NOT implicitly commit.
    await conn.commit()


async def main():
    """Run the normal happy‑path example using an in‑memory database.

    The file ``examples/statements.sql`` contains the following SQL::

        CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
        INSERT INTO users (name) VALUES ("Alice"), ("Bob");

    This function:

    1. connects to ``:memory:`` (a fresh database each run),
    2. executes the script, and
    3. selects and prints the contents of ``users``.
    """

    # ``async with`` ensures the connection is closed when we exit this block.
    async with await aiolibsql.connect(":memory:") as conn:
        script_path = os.path.join(os.path.dirname(__file__), "statements.sql")
        # ``execute_script`` reads and runs the SQL from disk.
        await execute_script(conn, script_path)

        # Verify the script did what we expected.
        cursor = await conn.cursor()
        await cursor.execute("SELECT id, name FROM users ORDER BY id")
        rows = await cursor.fetchall()

        print("\nData in the 'users' table after script execution:")
        for row in rows:
            # ``row`` behaves like a tuple; we can also index by column name.
            print(f"id={row[0]} name={row[1]}")


async def main_with_error():
    """Demonstrate error handling by running a broken script.

    The temporary script created in this function contains one valid
    statement and one invalid statement.  ``executescript`` will raise a
    ``sqlite3.OperationalError`` and nothing will be committed.  We catch the
    exception and explicitly roll back the transaction, then show that the
    table is still empty.
    """

    bad_script = os.path.join(os.path.dirname(__file__), "bad_statements.sql")
    # create a small file on the fly
    with open(bad_script, "w", encoding="utf-8") as f:
        f.write(
            """
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT);
            -- the next line has a typo (INSRT instead of INSERT)
            INSRT INTO items (value) VALUES ('oops');
            """
        )

    async with await aiolibsql.connect(":memory:") as conn:
        try:
            await execute_script(conn, bad_script)
        except Exception as exc:  # pragma: no cover - demonstration only
            print("Caught error executing bad script:", exc)
            # ``conn`` is still open and in a transaction; roll it back.
            await conn.rollback()

        # confirm that the table was not created
        cursor = await conn.cursor()
        await cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='items'"
        )
        exists = await cursor.fetchone()
        print("\nTable 'items' exists after failed script?", bool(exists))


if __name__ == "__main__":
    # We drive both examples sequentially with ``asyncio.run``.  The
    # first example is the normal case; the second shows error handling.
    asyncio.run(main())
    asyncio.run(main_with_error())
