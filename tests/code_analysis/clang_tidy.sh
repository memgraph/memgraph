#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."
BASE_BRANCH="origin/master"
THREADS=${THREADS:-$(nproc)}
# Directories to exclude from clang-tidy analysis
EXCLUSIONS=":!src/planner/test :!src/planner/bench"
VENV_DIR="${VENV_DIR:-env}"

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

if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  echo "Error: Virtual environment not found at $VENV_DIR/bin/activate"
  echo "Please create a virtual environment first with: python3 -m venv $VENV_DIR"
  exit 1
fi
source "$VENV_DIR/bin/activate"
trap 'deactivate 2>/dev/null' EXIT ERR

if [[ ! -f "build/generators/conanbuild.sh" ]]; then
  echo "Error: Conan build environment not found at build/generators/conanbuild.sh"
  echo "Please run the build configuration first with: ./build.sh --config-only"
  exit 1
fi
source build/generators/conanbuild.sh

if ! command -v ninja &> /dev/null; then
  echo "Error: ninja build tool not found in PATH"
  echo "Please ensure ninja is installed and the Conan environment is activated"
  exit 1
fi
ninja -C build -t inputs | grep -E '\.cppm\.o$|\.o\.modmap$' | xargs -r ninja -C build

git diff -U0 $BASE_BRANCH... -- src $EXCLUSIONS | ./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j $THREADS -path build  -regex ".+\.cppm?" | tee ./build/clang_tidy_output.txt
# Fail if any warning is reported
if  cat ./build/clang_tidy_output.txt | ./tools/github/clang-tidy/grep_error_lines.sh > /dev/null; then
    exit 1
fi
