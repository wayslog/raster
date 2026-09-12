#!/bin/sh
# Build the direct benchmark against the unmodified, pinned upstream core.
set -eu
upstream=$1
output=$2
test "$(git -C "$upstream" rev-parse HEAD)" = 321d872eabda6a0345c8bd76419f89723ed864ae
test -z "$(git -C "$upstream" status --porcelain --untracked-files=no)"
mkdir -p "$output"
${CXX:-g++} -std=c++17 -O3 -DNDEBUG -pthread -I"$upstream/cc/src" \
  tools/performance/memory.cc "$upstream/cc/src/core/address.cc" \
  "$upstream/cc/src/core/lss_allocator.cc" "$upstream/cc/src/core/thread.cc" \
  "$upstream/cc/src/environment/file_linux.cc" \
  -laio -luuid -ltbb -lstdc++fs -o "$output/faster-memory"
