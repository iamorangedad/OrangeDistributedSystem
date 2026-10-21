#!/bin/sh
# 编译并运行 Java 版。JDK 用 JAVA_HOME，没有就用 ~/.local/jdk。
set -eu
cd "$(dirname "$0")"
if [ -z "${JAVA_HOME:-}" ] && [ -x "$HOME/.local/jdk/bin/javac" ]; then
    JAVA_HOME="$HOME/.local/jdk"
fi
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "$JAVA_HOME/bin/javac" ]; then
    echo "找不到 javac。请安装 JDK 21，或把 JAVA_HOME 指到它。" >&2
    exit 1
fi
if [ ! -f lib/sqlite-jdbc.jar ]; then
    mkdir -p lib
    curl -fsSL -o lib/sqlite-jdbc.jar "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.47.2.0/sqlite-jdbc-3.47.2.0.jar"
fi
mkdir -p build
find src test -name '*.java' > build/sources.txt
"$JAVA_HOME/bin/javac" --release 21 -encoding UTF-8 -cp lib/sqlite-jdbc.jar -d build @build/sources.txt
CP="build:lib/sqlite-jdbc.jar"
if [ "${1:-}" = "test" ]; then
    exec "$JAVA_HOME/bin/java" -cp "$CP" orange.Acceptance
fi
exec "$JAVA_HOME/bin/java" -cp "$CP" orange.Main "$@"
