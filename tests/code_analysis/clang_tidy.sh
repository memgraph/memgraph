#!/bin/bash
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
git diff -U0 $BASE_BRANCH... -- src | ./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j $THREADS -path build  -regex ".+\.cpp" | tee ./build/clang_tidy_output.txt
# Fail if any warning is reported
! cat ./build/clang_tidy_output.txt | ./tools/github/clang-tidy/grep_error_lines.sh > /dev/null
cd $SCRIPT_DIR
