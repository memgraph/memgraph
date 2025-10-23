#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."
BASE_BRANCH="origin/master"
THREADS=${THREADS:-$(nproc)}
# Directories to exclude from clang-tidy analysis
EXCLUSIONS=":!src/planner/test :!src/planner/bench"

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

# Run clang-tidy-diff and capture its exit status
set +e  # Temporarily disable exit on error to capture exit status
git diff -U0 $BASE_BRANCH... -- src $EXCLUSIONS | ./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j $THREADS -path build  -regex ".+\.cpp" | tee ./build/clang_tidy_output.txt
CLANG_TIDY_EXIT_CODE=${PIPESTATUS[1]}  # Get exit code of clang-tidy-diff.py (second command in pipeline)
set -e  # Re-enable exit on error

# Fail if clang-tidy crashed
if [ $CLANG_TIDY_EXIT_CODE -ne 0 ]; then
    echo "clang-tidy crashed with exit code: $CLANG_TIDY_EXIT_CODE"
    exit 1
fi

# Fail if any warning is reported
if  cat ./build/clang_tidy_output.txt | ./tools/github/clang-tidy/grep_error_lines.sh > /dev/null; then
    exit 1
fi

cd $SCRIPT_DIR
