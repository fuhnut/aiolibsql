import asyncio
import aiolibsql

async def main():
    async with await aiolibsql.connect("local.db") as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT);")
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        cursor = await conn.execute("select * from users")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
