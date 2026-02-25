"""
A short example showing how to execute a script containing a bunch of sql statements.
"""
import os
import asyncio
import aiolibsql

async def execute_script(conn, file_path: os.PathLike):
    with open(file_path, 'r') as file:
        script = file.read()

    await conn.executescript(script)
    await conn.commit()

async def main():
    async with await aiolibsql.connect(':memory:') as conn:
        script_path = os.path.join(os.path.dirname(__file__), 'statements.sql')
        await execute_script(conn, script_path)

        # Retrieve the data from the 'users' table and print it
        cursor = await conn.cursor()
        await cursor.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        print("Data in the 'users' table:")
        for row in rows:
            print(row)

if __name__ == "__main__":
    asyncio.run(main())
