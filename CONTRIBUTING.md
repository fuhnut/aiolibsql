# Contributing to aiolibsql

## Developing

Setup the development environment:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install maturin pytest pytest-asyncio
```

Build the development version:

```sh
maturin develop
```

Run the quick example:

```sh
python example.py
```

## Testing

Run the comprehensive CLI test:

```sh
python cli_test.py
```

Run the pytest suite:

```sh
pytest tests/
```
