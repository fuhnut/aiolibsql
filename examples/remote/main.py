"""🌐 **remote/main.py**

Connect to a **remote Turso/libsql database** and run simple DDL/DML
actions.  This example shows how to supply the connection URL and
authentication token via environment variables and how to ``commit`` the
changes.

Before running set the following environment variables:

```bash
export TURSO_DATABASE_URL="libsql://your-db.turso.io"
export TURSO_AUTH_TOKEN="eyJhbGciOi..."
```

Then run the script with ``python examples/remote/main.py``.  It will create
or recreate the ``users`` table on the remote database and print the rows.
"""

import aiolibsql
import os
import asyncio


async def main():
    # read connection info from environment variables
    url = os.getenv("TURSO_DATABASE_URL")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    # remote connections require an auth token
    async with await aiolibsql.connect(url, auth_token=auth_token) as conn:
        # perform several operations; these are sent over the network to the
        # remote database.
        await conn.execute("drop table if exists users;")
        await conn.execute("create table if not exists users (name text);")
        await conn.execute("insert into users values ('first@example.com');")
        await conn.execute("insert into users values ('second@example.com');")
        await conn.execute("insert into users values ('third@example.com');")

        # remote writes are buffered until you commit explicitly.
        await conn.commit()

        # read back what we just inserted
        cursor = await conn.execute("select * from users")
        rows = await cursor.fetchall()
        print("Remote rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
