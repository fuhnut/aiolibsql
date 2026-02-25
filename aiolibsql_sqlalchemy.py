"""
aiolibsql SQLAlchemy dialect — async support via create_async_engine

Usage:
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("sqlite+aiolibsql://", echo=True)

    # or with a file:
    engine = create_async_engine("sqlite+aiolibsql:///data.db")

Register the dialect before use:
    import aiolibsql_sqlalchemy  # auto-registers on import

Or register manually:
    from sqlalchemy.dialects import registry
    registry.register("sqlite.aiolibsql", "aiolibsql_sqlalchemy", "dialect")
"""

import asyncio
import aiolibsql

from sqlalchemy import util
from sqlalchemy.dialects import registry as _registry
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.pool import StaticPool


_registry.register("sqlite.aiolibsql", "aiolibsql_sqlalchemy", "dialect")


def _run(coro):
    """Run an async coroutine synchronously. SQLAlchemy's async engine calls
    DBAPI methods from a worker thread, so we need a dedicated event loop."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, coro).result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


class _Cursor:
    """Sync wrapper around aiolibsql.Cursor for DBAPI compatibility."""

    def __init__(self, conn):
        self._conn = conn
        self._cursor = None
        self._description = None
        self.arraysize = 1
        self.lastrowid = None
        self.rowcount = -1

    @property
    def description(self):
        if self._cursor is not None:
            return self._cursor.description
        return self._description

    def execute(self, sql, parameters=None):
        if parameters:
            params = tuple(parameters)
            self._cursor = _run(self._conn.execute(sql, params))
        else:
            self._cursor = _run(self._conn.execute(sql))
        self.lastrowid = self._cursor.lastrowid
        self.rowcount = self._cursor.rowcount
        self._description = self._cursor.description
        return self

    def executemany(self, sql, parameters):
        params_list = [tuple(p) for p in parameters]
        self._cursor = _run(self._conn.executemany(sql, params_list))
        self.lastrowid = self._cursor.lastrowid
        self.rowcount = self._cursor.rowcount
        return self

    def executescript(self, script):
        _run(self._conn.executescript(script))
        return self

    def fetchone(self):
        if self._cursor is None:
            return None
        return _run(self._cursor.fetchone())

    def fetchmany(self, size=None):
        if self._cursor is None:
            return []
        s = size if size is not None else self.arraysize
        return _run(self._cursor.fetchmany(s))

    def fetchall(self):
        if self._cursor is None:
            return []
        return _run(self._cursor.fetchall())

    def close(self):
        if self._cursor is not None:
            _run(self._cursor.close())
            self._cursor = None

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class _Connection:
    """Sync wrapper around aiolibsql.Connection for DBAPI compatibility."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _Cursor(self._conn)

    def execute(self, sql, parameters=None):
        cur = _Cursor(self._conn)
        cur.execute(sql, parameters)
        return cur

    def commit(self):
        _run(self._conn.commit())

    def rollback(self):
        _run(self._conn.rollback())

    def close(self):
        _run(self._conn.close())

    @property
    def isolation_level(self):
        return self._conn.isolation_level

    @property
    def in_transaction(self):
        return self._conn.in_transaction


# DBAPI module-level attributes
paramstyle = "qmark"
apilevel = "2.0"
threadsafety = 1
Error = aiolibsql.Error
sqlite_version_info = aiolibsql.sqlite_version_info


def connect(database=":memory:", timeout=5.0, isolation_level="DEFERRED",
            check_same_thread=True, uri=False, **kwargs):
    """DBAPI connect() — wraps aiolibsql.connect() synchronously."""
    conn = _run(aiolibsql.connect(
        database=database,
        timeout=timeout,
        isolation_level=isolation_level,
        auth_token=kwargs.get("auth_token"),
        sync_url=kwargs.get("sync_url"),
        encryption_key=kwargs.get("encryption_key"),
        autocommit=kwargs.get("autocommit", -1),
    ))
    return _Connection(conn)


class SQLiteDialect_aiolibsql(SQLiteDialect_pysqlite):
    driver = "aiolibsql"
    supports_statement_cache = SQLiteDialect_pysqlite.supports_statement_cache

    @classmethod
    def import_dbapi(cls):
        import aiolibsql_sqlalchemy
        return aiolibsql_sqlalchemy

    @classmethod
    def dbapi(cls):
        import aiolibsql_sqlalchemy
        return aiolibsql_sqlalchemy

    def on_connect(self):
        return None

    def create_connect_args(self, url):
        opts = {}
        if url.database:
            database = url.database
        else:
            database = ":memory:"

        if url.query:
            for key, val in url.query.items():
                if key == "timeout":
                    opts["timeout"] = float(val)
                elif key == "auth_token":
                    opts["auth_token"] = val
                elif key == "sync_url":
                    opts["sync_url"] = val
                elif key == "encryption_key":
                    opts["encryption_key"] = val

        opts.setdefault("check_same_thread", False)
        return ([database], opts)


dialect = SQLiteDialect_aiolibsql
