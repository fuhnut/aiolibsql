"""local file example: open or create local.db, run a few statements, print users.
"""

import asyncio
import aiolibsql


async def main():
    # async with closes connection on exit.
    async with await aiolibsql.connect("local.db") as conn:
        # create table if it doesn't exist.
        await conn.execute("create table if not exists users (name text);")

        # insert a few rows.
        await conn.execute("insert into users values ('first@example.com');")
        await conn.execute("insert into users values ('second@example.com');")
        await conn.execute("insert into users values ('third@example.com');")

        # select and fetch all rows.
        cursor = await conn.execute("select * from users")
        rows = await cursor.fetchall()

        print("rows in local.db:")
        for row in rows:
            print(row)


if __name__ == "__main__":
    asyncio.run(main())
