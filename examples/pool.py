import asyncio
import aiolibsql
import os

async def main():
    pool = await aiolibsql.create_pool("local_pool.db", size=4, timeout=5.0)
    
    await pool.execute("create table if not exists users (id integer primary key, name text);")
    
    cursor = await pool.execute("insert into users (name) values (?)", ("alice",))
    print(f"inserted row: {cursor.lastrowid}")
    
    cursor = await pool.execute("select * from users;")
    rows = await cursor.fetchall()
    print(f"fetched from pool: {rows}")
    
    operations = [
        ("insert into users (name) values (?)", ("bob",)),
        ("insert into users (name) values (?)", ("charlie",))
    ]
    await pool.executebatch(operations)
    
    cursor = await pool.execute("select count(*) from users;")
    count = await cursor.fetchone()
    print(f"total users: {count[0]}")
    
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
