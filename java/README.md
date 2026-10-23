# OrangeOrder in Java

[中文](README.zh.md)

HTTP is the JDK `com.sun.net.httpserver`. JSON is the hand-written `Json` class. Each store process still uses one lock. The JDK has no sqlite, so the only extra dependency is `sqlite-jdbc`. The build script downloads the jar when it is missing. The jar is not committed.

## Run

JDK 21 is required. The script uses `JAVA_HOME`, or `~/.local/jdk` when that variable is unset.

```bash
java/build.sh --root .
```

```bash
java/build.sh test
```

Shared rules for ports, acceptance, and the file map are on the [homepage](../README.md).
