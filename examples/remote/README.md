# Remote

This example demonstrates how to connect to a remote Turso / libSQL database.

## Install

```bash
pip install "aiolibsql @ git+https://github.com/fuhnut/aiolibsql"
```

## Configuration

Set the following environment variables:

```bash
export TURSO_DATABASE_URL="libsql://your-db.turso.io"
export TURSO_AUTH_TOKEN="your-token"
```

## Running

```bash
python3 main.py
```
