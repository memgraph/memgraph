#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."
BASE_BRANCH="origin/master"
THREADS=${THREADS:-$(nproc)}

if [[ "$#" -gt 0 ]]; then
  case "$1" in
    --base-branch)
      BASE_BRANCH=$2
    ;;
    *)
      echo "Error: Unknown flag '$1'"
      exit 1
    ;;
  esac
fi

cd $PROJECT_ROOT

# Debug: Show include paths that clang-tidy will use
echo "=== DEBUG: Checking clang-tidy include paths ==="
echo "Testing clang-tidy on text_index.cpp to show include resolution..."
./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j 1 -path build -extra-arg=-v -regex "text_index\.cpp" < <(echo "--- a/src/storage/v2/indices/text_index.cpp
+++ b/src/storage/v2/indices/text_index.cpp
@@ -1 +1 @@
-// dummy diff to trigger clang-tidy
+// dummy diff to trigger clang-tidy") 2>&1 | head -100 || true
echo "=== END DEBUG ==="

git diff -U0 $BASE_BRANCH... -- src | ./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j $THREADS -path build  -regex ".+\.cpp" | tee ./build/clang_tidy_output.txt
# Fail if any warning is reported
if  cat ./build/clang_tidy_output.txt | ./tools/github/clang-tidy/grep_error_lines.sh > /dev/null; then
    exit 1
fi

cd $SCRIPT_DIR
