# OrangeOrder in TypeScript

[中文](README.zh.md)

TypeScript runs on Node. HTTP is the standard-library `node:http`. The recommendation and the deadline wait together on the event loop. Store transactions use synchronous `better-sqlite3`, so the lock is still the write itself. `better-sqlite3` is compiled by `npm install` and is not committed.

## Run

Node 20 is required.

```bash
ts/run.sh --root .
```

```bash
ts/run.sh test
```

`ts/run.sh` installs dependencies, compiles into `ts/dist/`, and runs `node dist/main.js`.

Shared rules for ports, acceptance, and the file map are on the [homepage](../README.md).
