"""⏳ **transaction/main.py**

Illustrates manual transaction control using ``rollback`` and ``commit``.
The example performs a few operations, then rolls the transaction back so that
those changes are discarded, and finally executes another statement to show
that only the later work persisted.

This is useful when you need to abort a sequence of operations due to an
error or business rule.
"""

import asyncio
import aiolibsql


async def main():
    async with await aiolibsql.connect("local.db") as conn:
        # drop/recreate the table so the script is repeatable
        await conn.execute("drop table if exists users;")
        await conn.execute("create table users (name text);")
        await conn.execute("insert into users values ('first@example.com');")
        await conn.execute("insert into users values ('second@example.com');")

        # rollback discards the two INSERTs above
        await conn.rollback()

        # this insert happens in a new transaction
        await conn.execute("insert into users values ('third@example.com');")

        cursor = await conn.execute("select * from users")
        rows = await cursor.fetchall()
        print("Transaction example rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
