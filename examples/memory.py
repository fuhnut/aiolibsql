import asyncio
import aiolibsql

async def main():
    async with await aiolibsql.connect(
        "file:memdb?mode=memory&cache=shared", 
        _uri=True
    ) as conn:
        await conn.execute("create table if not exists items (id integer primary key, val blob);")
        await conn.execute("insert into items (val) values (?)", (b"binary_data",))
        
        cursor = await conn.execute("select * from items")
        print(await cursor.fetchall())

if __name__ == "__main__":
    asyncio.run(main())
