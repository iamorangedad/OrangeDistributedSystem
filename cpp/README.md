# OrangeOrder in C++

[中文](README.zh.md)

This implementation does the same job as Python, Rust, Java, and Go: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/technical-design.md). Language differences are in [ADR 0004](../docs/adr/0004-stdlib-sqlite.md).

C++ does not use a web framework. HTTP is POSIX sockets, one thread per connection, with hand-written JSON. The store is still one lock plus sqlite. The machine has `libsqlite3.so.0` but no `sqlite3.h` and no unversioned `libsqlite3.so` for the linker. The build script downloads the 3.37.2 header into `cpp/third_party/` and links `libsqlite3.so.0` directly. The header and build output are not committed.

Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

g++ and `libsqlite3` are required. From the repository root:

```bash
cpp/build.sh --root .
```

Acceptance:

```bash
cpp/build.sh test
```

`cpp/build.sh` compiles `cpp/build/orange`. With no subcommand the program is the supervisor. It re-executes itself as `store`, `gateway`, `order`, `recommender`, or `worker`.

The file map for all six implementations is on the [homepage](../README.md).
