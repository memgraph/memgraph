#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export RUST_LOG=warn
export TMPDIR="/tmp" # To make std::filesystem::temp_directory_path returns path under /tmp
rm -rf /tmp/text_search_index_*
print_help() {
  echo "$0 [--full] [--release]"
  exit 1
}

MGCXX_TEXT_SEARCH_CI_FULL=false
MGCXX_TEXT_SEARCH_CI_RELEASE=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --full)
      MGCXX_TEXT_SEARCH_CI_FULL=true
      shift
    ;;
    --release)
      MGCXX_TEXT_SEARCH_CI_RELEASE=true
      shift
    ;;
    *)
      print_help
    ;;
  esac
done
echo "Run config:"
echo "  full   : $MGCXX_TEXT_SEARCH_CI_FULL"
echo "  release: $MGCXX_TEXT_SEARCH_CI_RELEASE"

cd "$SCRIPT_DIR/.."
FILES_TO_FIX=$({ git diff --name-only ; git diff --name-only --staged ; } | sort | uniq | egrep "\.c$|\.cpp$|.cxx$|\.h$|\.hpp$|\.hxx$" || true)
if [ ! -z "$FILES_TO_FIX" ]; then
  for file in "${FILES_TO_FIX}"; do
    clang-format -i -verbose ${file}
  done
fi
cd "$SCRIPT_DIR"
cargo fmt

mkdir -p "$SCRIPT_DIR/../build"
cd "$SCRIPT_DIR/../build"
if [ "$MGCXX_TEXT_SEARCH_CI_FULL" = true ]; then
  rm -rf ./* && rm -rf .cache
  # Added here because Cargo.lock is ignored for libraries, but it's not
  # located under build folder. Rebuilding from scratch should also start clean
  # from cargo perspective.
  rm "$SCRIPT_DIR/Cargo.lock" || true
else
  rm -rf index*
fi

if [ "$MGCXX_TEXT_SEARCH_CI_RELEASE" = true ]; then
  cmake -DCMAKE_BUILD_TYPE=Release ..
else
  cmake ..
fi
make -j8

cd "$SCRIPT_DIR/../build/text_search"
./test_unit
./test_bench
# ./test_bench --benchmark_filter="MyFixture2/BM_BenchLookup"
./test_stress

rm -rf /tmp/text_search_index_*
