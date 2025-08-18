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
echo "Compile command for text_index.cpp:"
if [ -f "build/compile_commands.json" ]; then
  grep -A5 -B5 "text_index.cpp" build/compile_commands.json | head -20 || echo "text_index.cpp not found in compile_commands.json"
else
  echo "compile_commands.json not found"
fi
echo ""
echo "Running clang-tidy directly on text_index.cpp to show include resolution..."
if [ -f "src/storage/v2/indices/text_index.cpp" ]; then
  # Run clang-tidy directly with verbose output
  clang-tidy -p build --extra-arg=-v src/storage/v2/indices/text_index.cpp 2>&1 | head -50 || true
else
  echo "text_index.cpp not found"
fi
echo "=== END DEBUG ==="

git diff -U0 $BASE_BRANCH... -- src | ./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j $THREADS -path build  -regex ".+\.cpp" | tee ./build/clang_tidy_output.txt
# Fail if any warning is reported
if  cat ./build/clang_tidy_output.txt | ./tools/github/clang-tidy/grep_error_lines.sh > /dev/null; then
    exit 1
fi

cd $SCRIPT_DIR
