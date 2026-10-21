#!/bin/sh
# 从仓库根目录启动或测试 Python 版。源码在 python/src，包名仍是 orange。
set -eu
cd "$(dirname "$0")/.."
export PYTHONPATH="$(pwd)/python/src${PYTHONPATH:+:$PYTHONPATH}"
if [ "${1:-}" = "test" ]; then
    shift
    exec python3 -m unittest discover -s python/tests "$@"
fi
exec python3 -m orange.supervisor "$@"
