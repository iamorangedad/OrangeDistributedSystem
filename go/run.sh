#!/bin/sh
# 编译并启动 Go 版 OrangeOrder。test 先编译，再跑单元检查和 AC-1 到 AC-12。
set -eu
cd "$(dirname "$0")"
mkdir -p bin
go build -o bin/orange .
if [ "${1:-}" = "test" ]; then
    shift
    go test -count=1 -timeout 30s .
    ORANGE_BIN="$(pwd)/bin/orange" go test ./acceptance -count=1 -timeout 240s "$@"
    exit 0
fi
exec ./bin/orange "$@"
