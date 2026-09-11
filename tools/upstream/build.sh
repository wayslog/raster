#!/bin/sh
# 构建固定上游核心源和本仓库的行为驱动器，不修改或替换上游实现。
set -eu
if [ "$#" -ne 2 ]; then
  echo '用法：build.sh 上游检出目录 构建目录' >&2
  exit 2
fi
upstream=$1
output=$2
expected=321d872eabda6a0345c8bd76419f89723ed864ae
actual=$(git -C "$upstream" rev-parse HEAD)
if [ "$actual" != "$expected" ]; then
  echo '上游提交与固定基线不符' >&2
  exit 1
fi
if [ -n "$(git -C "$upstream" status --porcelain --untracked-files=no)" ]; then
  echo '上游已跟踪源文件存在未提交修改' >&2
  exit 1
fi
mkdir -p "$output"
${CXX:-g++} -std=c++17 -O2 -pthread -I"$upstream/cc/src" \
  tools/upstream/replay.cc \
  "$upstream/cc/src/core/address.cc" \
  "$upstream/cc/src/core/lss_allocator.cc" \
  "$upstream/cc/src/core/thread.cc" \
  "$upstream/cc/src/environment/file_linux.cc" \
  -laio -luuid -ltbb -lstdc++fs -o "$output/faster-replay"
