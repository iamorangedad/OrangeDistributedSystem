# OrangeOrder in C++

[中文](README.zh.md)

HTTP is POSIX sockets, one thread per connection, with hand-written JSON. The store is still one lock plus sqlite. The machine has `libsqlite3.so.0` but no `sqlite3.h` and no unversioned `libsqlite3.so` for the linker. The build script downloads the 3.37.2 header into `cpp/third_party/` and links `libsqlite3.so.0` directly. The header and build output are not committed.

## Run

g++ and `libsqlite3` are required.

```bash
cpp/build.sh --root .
```

```bash
cpp/build.sh test
```

`cpp/build.sh` compiles `cpp/build/orange`.

Shared rules for ports, acceptance, and the file map are on the [homepage](../README.md).
