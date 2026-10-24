# OrangeOrder in Python

[中文](README.zh.md)

This is the original implementation. Python uses the standard library and sqlite3. Code is in `python/src/`. Tests are in `python/tests/`. `python/run.sh` puts `python/src` on `PYTHONPATH` and starts `python -m supervisor`.

## Run

```bash
python/run.sh --root .
```

```bash
python/run.sh test
```

Shared rules for ports, acceptance, and the file map are on the [homepage](../README.md).
