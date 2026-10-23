# OrangeOrder in Java

[中文](README.zh.md)

This implementation does the same job as Python, Rust, C++, and Go: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/technical-design.md). Language differences are in [ADR 0004](../docs/adr/0004-stdlib-sqlite.md).

Java does not use a web framework. HTTP is the JDK `com.sun.net.httpserver`. JSON is the hand-written `Json` class. Each store process still uses one lock. The JDK has no sqlite, so the only extra dependency is `sqlite-jdbc`. The build script downloads the jar when it is missing. The jar is not committed.

The supervisor starts `orange.Main` on the same classpath and selects the process with a subcommand. Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

JDK 21 is required. The script uses `JAVA_HOME`, or `~/.local/jdk` when that variable is unset.

From the repository root:

```bash
java/build.sh --root .
```

Callers still use only `http://127.0.0.1:8080`.

```bash
java/build.sh test
```

The acceptance test starts the supervisor and every child. It takes about a minute or two. The ports are fixed, so two stacks cannot listen together.

The file map for all six implementations is on the [homepage](../README.md).
