import asyncio
import aiolibsql

async def main():
    try:
        async with await aiolibsql.connect(
            "file:memdb_all?mode=memory&cache=shared", 
            _uri=True,
            timeout=5.0,
            isolation_level="deferred",
            autocommit=-1
        ) as conn:
            
            # properties
            print(conn.isolation_level)
            print(conn.in_transaction)
            print(conn.autocommit)
            
            # functions
            await conn.execute("create table if not exists users (id integer primary key, name text);")
            await conn.executescript("insert into users (name) values ('alice'); insert into users (name) values ('bob');")
            await conn.executemany("insert into users (name) values (?)", [("charlie",), ("diana",)])
            
            cursor = await conn.execute("select * from users")
            print(cursor.description)
            print(cursor.rowcount)
            print(cursor.lastrowid)
            
            row = await cursor.fetchone()
            rows_batch = await cursor.fetchmany(1)
            rows_all = await cursor.fetchall()
            
            async for r in cursor:
                pass
                
            await cursor.close()
            
            await conn.commit()
            await conn.execute("insert into users (name) values ('oops')")
            await conn.rollback()
            
            # sync mock
            # await conn.sync()
            
        pool = await aiolibsql.create_pool("file:pool_all?mode=memory&cache=shared", size=2, timeout=5.0)
        print(pool.size)
        print(pool.reader_count)
        
        await pool.execute("create table if not exists items (id integer primary key);")
        await pool.executemany("insert into items (id) values (?)", [(1,), (2,)])
        await pool.executebatch([
            ("insert into items (id) values (?)", (3,)),
            ("insert into items (id) values (?)", (4,))
        ])
        
        cursor = await pool.execute("select * from items")
        print(cursor.description)
        await pool.close()
        
    except aiolibsql.Error as e:
        print(f"error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
