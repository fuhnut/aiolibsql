import asyncio
import aiolibsql

async def main():
    async with await aiolibsql.connect("hello.db") as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT);")
        await conn.execute("INSERT INTO users VALUES (1, 'alice@example.com')")

        cursor = await conn.execute("SELECT * FROM users")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
