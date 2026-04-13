import asyncio
import aiolibsql

async def main():
    key = "super-secret-encryption-key"
    
    async with await aiolibsql.connect("encrypted.db", encryption_key=key) as conn:
        await conn.execute("create table if not exists secrets (id integer, data text);")
        await conn.execute("insert into secrets values (?, ?)", (1, "hidden_data"))
        
        cursor = await conn.execute("select * from secrets")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
