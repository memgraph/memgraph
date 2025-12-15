#!/bin/bash
set -euo pipefail

# Error handler to provide better diagnostics
error_handler() {
    local exit_code=$?
    local line_number=$1
    echo ""
    echo "Error: Script failed at line $line_number with exit code $exit_code"
    case $exit_code in
        127)
            echo "Exit code 127 indicates 'command not found' - a process may have been killed by OOM killer"
            echo "Check 'dmesg | tail -20' for OOM messages"
            echo "Try running with fewer threads: THREADS=4 $0 $*"
            ;;
        137)
            echo "Exit code 137 indicates process killed by SIGKILL (likely OOM killer)"
            echo "Try running with fewer threads: THREADS=4 $0 $*"
            ;;
        139)
            echo "Exit code 139 indicates segmentation fault"
            ;;
    esac
    exit $exit_code
}
trap 'error_handler $LINENO' ERR

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."
BASE_BRANCH="origin/master"
# Limit threads based on available memory (clang-tidy can use 2GB+ per process)
# Default to nproc but cap based on available memory (assume 2GB per job)
MAX_THREADS_BY_CPU=$(nproc)
AVAILABLE_MEM_GB=$(awk '/MemAvailable/ {printf "%.0f", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo "16")
MAX_THREADS_BY_MEM=$((AVAILABLE_MEM_GB / 2))
MAX_THREADS_BY_MEM=$((MAX_THREADS_BY_MEM > 0 ? MAX_THREADS_BY_MEM : 1))
DEFAULT_THREADS=$((MAX_THREADS_BY_CPU < MAX_THREADS_BY_MEM ? MAX_THREADS_BY_CPU : MAX_THREADS_BY_MEM))
THREADS=${THREADS:-$DEFAULT_THREADS}
# Directories to exclude from clang-tidy analysis
EXCLUSIONS=":!src/planner/test :!src/planner/bench :!src/csv/fuzz"
VENV_DIR="${VENV_DIR:-env}"
# until https://github.com/conan-io/conan/issues/19285 is fixed
CLASSPATH=
DYLD_LIBRARY_PATH=
LD_LIBRARY_PATH=

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

# Add toolchain to PATH for clang-tidy
MG_TOOLCHAIN_ROOT="${MG_TOOLCHAIN_ROOT:-/opt/toolchain-v7}"
if [[ -d "$MG_TOOLCHAIN_ROOT/bin" ]]; then
  export PATH="$MG_TOOLCHAIN_ROOT/bin:$PATH"
else
  echo "Warning: Toolchain bin directory not found at $MG_TOOLCHAIN_ROOT/bin"
fi

if ! command -v ninja &> /dev/null; then
  echo "Error: ninja build tool not found in PATH"
  echo "Please ensure ninja is installed and the Conan environment is activated"
  exit 1
fi

if ! command -v clang-tidy &> /dev/null; then
  echo "Error: clang-tidy not found in PATH"
  echo "Please ensure the toolchain is installed at $MG_TOOLCHAIN_ROOT"
  exit 1
fi
ninja -C build -t inputs | grep -E '\.cppm\.o$|\.o\.modmap$' | xargs -r ninja -C build

echo "Running clang-tidy on changed files..."
echo "Using clang-tidy: $(command -v clang-tidy)"
echo "clang-tidy version: $(clang-tidy --version | head -1)"
echo "Parallel jobs: $THREADS (CPU cores: $MAX_THREADS_BY_CPU, Memory limit: $MAX_THREADS_BY_MEM based on ${AVAILABLE_MEM_GB}GB available)"

git diff -U0 $BASE_BRANCH... -- src $EXCLUSIONS | ./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j $THREADS -path build  -regex ".+\.cppm?" | tee ./build/clang_tidy_output.txt

echo ""
echo "Checking for errors in clang-tidy output..."
# Fail if any warning is reported
if ./tools/github/clang-tidy/grep_error_lines.sh < ./build/clang_tidy_output.txt > /dev/null; then
    echo "Error: clang-tidy found issues:"
    ./tools/github/clang-tidy/grep_error_lines.sh < ./build/clang_tidy_output.txt
    exit 1
fi
echo "No clang-tidy errors found."
