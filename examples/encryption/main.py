import asyncio
import aiolibsql

async def main():
    # You should set the ENCRYPTION_KEY in a environment variable
    # For demo purposes, we're using a fixed key
    encryption_key= "my-safe-encryption-key"

    async with await aiolibsql.connect("local.db", encryption_key=encryption_key) as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (name TEXT);")
        await conn.execute("INSERT INTO users VALUES ('first@example.com');")
        await conn.execute("INSERT INTO users VALUES ('second@example.com');")
        await conn.execute("INSERT INTO users VALUES ('third@example.com');")

        cursor = await conn.execute("select * from users")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
