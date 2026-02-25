"""
A short example showing how to connect to a remote libsql or Turso database

Set the LIBSQL_URL and LIBSQL_AUTH_TOKEN environment variables to point to a database.
"""
import os
import asyncio
import aiolibsql

async def main():
    print(F"connecting to {os.getenv('LIBSQL_URL')}")
    async with await aiolibsql.connect(database=os.getenv('LIBSQL_URL'),
                          auth_token=os.getenv("LIBSQL_AUTH_TOKEN")) as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER);")
        await conn.execute("INSERT INTO users(id) VALUES (10);")
        await conn.commit()

        cursor = await conn.execute("select * from users")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
