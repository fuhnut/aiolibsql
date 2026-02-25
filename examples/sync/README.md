# Embedded Replica (Sync)

This example demonstrates how to create an embedded replica that syncs with a remote Turso database.

## Install

```bash
pip install "aiolibsql @ git+https://github.com/fuhnut/aiolibsql"
```

## Configuration

```bash
export TURSO_DATABASE_URL="libsql://your-db.turso.io"
export TURSO_AUTH_TOKEN="your-token"
```

## Running

```bash
python3 main.py
```
