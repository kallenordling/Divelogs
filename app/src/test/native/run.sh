#!/usr/bin/env bash
# Builds and runs shearwater_petrel_test.c on the host, twice:
#
#   fixed     DeepLog's driver (app/src/main/cpp/libdc) — must pass
#   upstream  the submodule's untouched driver — must FAIL the scenarios the
#             fixes are for, which is what shows the tests detect those bugs
#             rather than passing by construction
#
# Uses the same libdivecomputer source list as the Android build, read from
# CMakeLists.txt, minus shearwater_common.c (the test stands in for it).
#
#   app/src/test/native/run.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
LIBDC="$ROOT/libdivecomputer"
CMAKE="$ROOT/app/src/main/cpp/CMakeLists.txt"
OUT="${TMPDIR:-/tmp}/deeplog-native-test"
CC="${CC:-cc}"

if [ ! -f "$LIBDC/src/aes.c" ]; then
  echo "libdivecomputer submodule is empty — run: git submodule update --init --recursive" >&2
  exit 1
fi
if [ ! -f "$LIBDC/include/libdivecomputer/version.h" ]; then
  echo "missing libdivecomputer/include/libdivecomputer/version.h (see .github/workflows/build.yml)" >&2
  exit 1
fi

mkdir -p "$OUT"

# Every libdivecomputer source the app compiles, as plain filenames.
mapfile -t SOURCES < <(grep -oE '\$\{LIBDC\}/src/[a-z0-9_]+\.c' "$CMAKE" \
                       | sed 's|${LIBDC}/src/||' | sort -u)

CFLAGS=(-std=gnu99 -O1 -g -w
        -I"$LIBDC/include" -I"$LIBDC/src"
        -DENABLE_LOGGING -DHAVE_VERSION_SUFFIX -DHAVE_PTHREAD_H -DHAVE_STRERROR_R
        -DHAVE_CLOCK_GETTIME -DHAVE_LOCALTIME_R -DHAVE_GMTIME_R -DHAVE_TIMEGM
        -DHAVE_STRUCT_TM_TM_GMTOFF
        # Host only: serial_posix.c compiles this block out on Android via
        # !defined(__ANDROID__), but glibc needs the header for it.
        -DHAVE_LINUX_SERIAL_H)

build () {  # $1 = label, $2 = path to the shearwater_petrel.c to test
  local label="$1" petrel="$2" objs=()
  local dir="$OUT/$label"
  mkdir -p "$dir"

  # Shared objects are the same for both builds; compile them once.
  for src in "${SOURCES[@]}"; do
    case "$src" in
      shearwater_common.c|shearwater_petrel.c) continue ;;
    esac
    local obj="$OUT/common-${src%.c}.o"
    [ -f "$obj" ] || "$CC" "${CFLAGS[@]}" -c "$LIBDC/src/$src" -o "$obj"
    objs+=("$obj")
  done

  "$CC" "${CFLAGS[@]}" -c "$petrel" -o "$dir/shearwater_petrel.o"
  "$CC" "${CFLAGS[@]}" -c "$HERE/shearwater_petrel_test.c" -o "$dir/test.o"
  "$CC" -o "$dir/test" "$dir/test.o" "$dir/shearwater_petrel.o" "${objs[@]}" -lpthread -lm
}

echo "== building (${#SOURCES[@]} libdivecomputer sources) =="
build fixed    "$ROOT/app/src/main/cpp/libdc/shearwater_petrel.c"
build upstream "$LIBDC/src/shearwater_petrel.c"

echo
echo "== DeepLog's driver =="
fixed_rc=0
"$OUT/fixed/test" || fixed_rc=$?

echo
echo "== upstream driver (expected to fail the bug scenarios) =="
upstream_out="$("$OUT/upstream/test" || true)"
echo "$upstream_out"

# The upstream build must fail exactly the scenarios the fixes address. If it
# passes one, that test is not actually detecting its bug.
#
# "every dive refused" is deliberately absent: upstream aborts on the first
# refusal and so already reports failure there. That test guards the *fix* —
# skipping refused dives must not turn a systemic failure into a clean, empty
# success — rather than detecting an upstream bug.
expect_upstream_fails=(
  "one deleted record"
  "three deleted records"
  "log ends on a page boundary"
  "log ends on the second page boundary"
  "one refused dive"
  "fingerprint with a deleted record ahead of it"
  "deleted record on a full first page"
)
detect_rc=0
echo
echo "== does each test detect its bug? =="
for name in "${expect_upstream_fails[@]}"; do
  if grep -q "^FAIL  $name" <<<"$upstream_out"; then
    echo "ok    upstream fails: $name"
  else
    echo "FAIL  upstream passes '$name' — that test does not detect the bug"
    detect_rc=1
  fi
done

echo
if [ "$fixed_rc" -eq 0 ] && [ "$detect_rc" -eq 0 ]; then
  echo "native tests: all passed"
else
  echo "native tests: FAILED"
  exit 1
fi
