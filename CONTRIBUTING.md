# Contributing to aiolibsql

## Developing

Setup the development environment:

```sh
uv sync --group dev
```

Build or refresh the native extension:

```sh
uv run maturin develop
```

Run the quick example:

```sh
uv run python example.py
```

## Testing

Run the comprehensive CLI test:

```sh
uv run python cli_test.py
```

Run the pytest suite:

```sh
uv run pytest tests/
```
