#!/bin/sh
# The original upstream was built separately from a reference copy that only fixed forced tombstone protection.,Keep two executable files.
set -eu
if [ "$#" -ne 2 ]; then
  echo 'Usage:build.sh Upstream checkout directory Build directory' >&2
  exit 2
fi
upstream=$1
output=$2
expected=321d872eabda6a0345c8bd76419f89723ed864ae
actual=$(git -C "$upstream" rev-parse HEAD)
if [ "$actual" != "$expected" ]; then
  echo 'Upstream commit does not match fixed baseline' >&2
  exit 1
fi
if [ -n "$(git -C "$upstream" status --porcelain --untracked-files=no)" ]; then
  echo 'The upstream tracked source file has uncommitted modifications' >&2
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
python3 tools/upstream/retain_forced.py "$upstream" "$output/retained-src"
${CXX:-g++} -std=c++17 -O2 -pthread -I"$output/retained-src" \
  tools/upstream/replay.cc \
  "$output/retained-src/core/address.cc" \
  "$output/retained-src/core/lss_allocator.cc" \
  "$output/retained-src/core/thread.cc" \
  "$output/retained-src/environment/file_linux.cc" \
  -laio -luuid -ltbb -lstdc++fs -o "$output/faster-replay-retained"
