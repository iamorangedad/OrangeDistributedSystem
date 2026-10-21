#!/bin/sh
# 编译 C++ 版 Orange。sqlite3.h 不在系统头文件里，所以第一次运行会下载 3.37.2 的合并头，
# 再链接系统里的 libsqlite3。不要把下载的头文件提交进仓库。
set -eu
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
CXX="${CXX:-g++}"
mkdir -p "$ROOT/cpp/build" "$ROOT/cpp/third_party"
HEADER="$ROOT/cpp/third_party/sqlite3.h"
if [ ! -f "$HEADER" ]; then
    ZIP="$ROOT/cpp/build/sqlite-amalgamation.zip"
    curl -fsSL -o "$ZIP" "https://www.sqlite.org/2022/sqlite-amalgamation-3370200.zip"
    unzip -p "$ZIP" "sqlite-amalgamation-3370200/sqlite3.h" > "$HEADER"
    rm -f "$ZIP"
fi
SRC="$ROOT/cpp/src"
INC="-I$ROOT/cpp/third_party -I$SRC"
FLAGS="-std=c++17 -O2 -pthread $INC"
SQLITE_LIB="$(ls /usr/lib/*/libsqlite3.so.0 /lib/*/libsqlite3.so.0 2>/dev/null | head -n 1)"
if [ -z "$SQLITE_LIB" ]; then
    echo "找不到 libsqlite3.so.0" >&2
    exit 1
fi
"$CXX" $FLAGS \
    "$SRC/json.cpp" "$SRC/common.cpp" "$SRC/http.cpp" \
    "$SRC/store.cpp" "$SRC/order.cpp" "$SRC/gateway.cpp" \
    "$SRC/recommender.cpp" "$SRC/worker.cpp" "$SRC/supervisor.cpp" "$SRC/main.cpp" \
    "$SQLITE_LIB" -o "$ROOT/cpp/build/orange"
"$CXX" $FLAGS \
    "$ROOT/cpp/tests/acceptance.cpp" \
    "$SRC/json.cpp" "$SRC/common.cpp" "$SRC/http.cpp" \
    -o "$ROOT/cpp/build/acceptance"
if [ "${1:-}" = "test" ]; then
    shift
    exec "$ROOT/cpp/build/acceptance" "$ROOT/cpp/build/orange" "$@"
fi
if [ "${1:-}" = "--root" ] || [ "${1:-}" = "supervisor" ]; then
    exec "$ROOT/cpp/build/orange" "$@"
fi
