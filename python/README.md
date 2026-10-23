# OrangeOrder in Python

[中文](README.zh.md)

This is the original implementation. It does the same job as `rust/`, `java/`, `cpp/`, `go/`, and `ts/`: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/technical-design.md). Language differences are in [ADR 0004](../docs/adr/0004-stdlib-sqlite.md).

Python uses the standard library and sqlite3. Code is in `python/src/orange/`. Tests are in `python/tests/`. The package name is `orange`. The supervisor starts `python -m orange.xxx`. `python/run.sh` puts `python/src` on `PYTHONPATH`.

Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

From the repository root:

```bash
python/run.sh --root .
```

Callers still use only `http://127.0.0.1:8080`.

```bash
python/run.sh test
```

The acceptance test starts the supervisor and every child. It takes about a minute or two. The ports are fixed, so two stacks cannot listen together.

The file map for all six implementations is on the [homepage](../README.md).
