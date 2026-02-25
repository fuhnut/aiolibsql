import asyncio
import aiolibsql

async def main():
    async with await aiolibsql.connect("local.db") as conn:
        cur = await conn.cursor()

        await cur.executescript(
            """
                DROP TABLE IF EXISTS users;
                CREATE TABLE users (id INTEGER, name TEXT);
                INSERT INTO users VALUES (1, 'first@example.org');
                INSERT INTO users VALUES (2, 'second@example.org');
                INSERT INTO users VALUES (3, 'third@example.org');
            """
        )

        cursor = await conn.execute("select * from users")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
