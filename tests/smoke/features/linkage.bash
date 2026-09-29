#!/bin/bash
# Linkage of the shipped memgraph binary.
#
# conanfile.py builds every dependency statically except a few that are shared
# on purpose: OpenSSL, so the binary uses whatever libcrypto the image provides
# rather than one frozen at build time, plus libseccomp and zlib. Everything
# else takes conan's shared=False default and is compiled in.
#
# The check runs both ways round. A missing entry catches those regressions; an
# unexpected one catches a dependency that has gone shared without anyone
# meaning it to, which would leave the package short a runtime dependency.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../utils.bash"

MEMGRAPH_BINARY="${MEMGRAPH_BINARY:-/usr/lib/memgraph/memgraph}"

# Conan dependencies that must be linked dynamically.
MEMGRAPH_REQUIRED_DYNAMIC=(libssl.so.3 libcrypto.so.3 libseccomp.so.2 libz.so.1)

# Static Conan dependencies that must NOT appear in DT_NEEDED.
MEMGRAPH_MUST_BE_STATIC=(
  libboost_ libfmt libprotobuf librdkafka libjemalloc libarrow librocksdb
  libabsl_ libantlr4-runtime libcurl libsnappy libbz2 libspdlog libgflags
  libmgclient libpulsar libsimdjson libnuraft libbcrypt librdtsc
  libaws-
)

test_binary_linkage() {
  echo "FEATURE: memgraph binary linkage"
  local needed rc=0 lib
  command -v readelf >/dev/null \
    || { echo "FAIL: readelf not found on the host (apt install binutils)"; return 1; }
  needed="$(container_dt_needed "$MEMGRAPH_BINARY")" \
    || { echo "FAIL: could not read DT_NEEDED from $MEMGRAPH_BINARY"; return 1; }
  [ -n "$needed" ] \
    || { echo "FAIL: $MEMGRAPH_BINARY reported no DT_NEEDED entries at all"; return 1; }

  for lib in "${MEMGRAPH_REQUIRED_DYNAMIC[@]}"; do
    if echo "$needed" | grep -qx "$lib"; then
      echo "  dynamic: $lib"
    else
      echo "FAIL: $lib is not in DT_NEEDED - conanfile.py asks for it shared, so it"
      echo "      has been linked statically and the binary carries its own copy."
      rc=1
    fi
  done

  local prefix
  while read -r lib; do
    [ -n "$lib" ] || continue
    for prefix in "${MEMGRAPH_MUST_BE_STATIC[@]}"; do
      case "$lib" in
        "$prefix"*)
          echo "FAIL: $lib is linked dynamically but conanfile.py builds it static."
          echo "      Either it was switched to shared - in which case the package needs"
          echo "      a matching runtime dependency - or this list needs updating."
          rc=1
        ;;
      esac
    done
  done <<< "$needed"

  [ $rc -eq 0 ] && echo "  linkage is as conanfile.py declares"
  return $rc
}
