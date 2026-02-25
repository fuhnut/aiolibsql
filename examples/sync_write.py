"""
A short example showing how to create an embedded replica, make writes and then read them

Set the LIBSQL_URL and LIBSQL_AUTH_TOKEN environment variables to point to a database.
"""
import os
import asyncio
import aiolibsql

async def main():
    print(F"syncing with {os.getenv('LIBSQL_URL')}")
    async with await aiolibsql.connect("hello.db", sync_url=os.getenv("LIBSQL_URL"),
                          auth_token=os.getenv("LIBSQL_AUTH_TOKEN")) as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER);")
        await conn.execute("INSERT INTO users(id) VALUES (1);")
        await conn.commit()
        await conn.sync()

        cursor = await conn.execute("select * from users")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
