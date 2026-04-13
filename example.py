import asyncio
import aiolibsql

async def main():
    async with await aiolibsql.connect("hello.db") as conn:
        await conn.execute("create table if not exists users (id integer, email text);")
        await conn.execute("insert into users values (1, 'alice@example.com')")
        cursor = await conn.execute("select * from users")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
