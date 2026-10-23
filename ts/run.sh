#!/bin/sh
# 编译并启动 TypeScript 版 OrangeOrder。test 先编译，再跑单元检查和 AC-1 到 AC-12。
set -eu
cd "$(dirname "$0")"
if [ ! -d node_modules/better-sqlite3 ]; then
  npm install
fi
npx tsc
chmod +x bin/orange
if [ "${1:-}" = "test" ]; then
  shift
  node --test dist/common.test.js
  ORANGE_BIN="$(pwd)/bin/orange" node --test dist/acceptance.test.js "$@"
  exit 0
fi
exec node dist/main.js "$@"
