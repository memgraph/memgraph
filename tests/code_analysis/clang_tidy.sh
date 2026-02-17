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
# Limit threads based on available memory (clang-tidy can use 3GB+ per process)
# Default to nproc but cap based on available memory (assume 3GB per job)
MAX_THREADS_BY_CPU=$(nproc)
AVAILABLE_MEM_GB=$(awk '/MemAvailable/ {printf "%.0f", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo "16")
MAX_THREADS_BY_MEM=$((AVAILABLE_MEM_GB / 3))
MAX_THREADS_BY_MEM=$((MAX_THREADS_BY_MEM > 0 ? MAX_THREADS_BY_MEM : 1))
DEFAULT_THREADS=$((MAX_THREADS_BY_CPU < MAX_THREADS_BY_MEM ? MAX_THREADS_BY_CPU : MAX_THREADS_BY_MEM))
THREADS=${THREADS:-$DEFAULT_THREADS}
# Directories to exclude from clang-tidy analysis
EXCLUSIONS=":!src/planner/test :!src/planner/bench :!src/csv/fuzz :!mage/cpp/community_detection_module/grappolo :!mage/cpp/text_module/utf8 :!mage/cpp/util_module/algorithm/md5.hpp :!mage/cpp/util_module/algorithm/md5.cpp"
VENV_DIR="${VENV_DIR:-env}"
FIX_FLAG=""
CONFIG_FILE=".clang-tidy"

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --base-branch)
      BASE_BRANCH=$2
      shift 2
    ;;
    --fix)
      FIX_FLAG="-fix"
      shift
    ;;
    *)
      echo "Error: Unknown flag '$1'"
      echo "Usage: $0 [--base-branch BRANCH] [--fix]"
      exit 1
    ;;
  esac
done

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

# Merge mage's compile_commands.json into the main one if it exists
if [[ -f "mage/cpp/build/compile_commands.json" ]]; then
  echo "Merging mage compile_commands.json into main build..."
  if [[ -f "build/compile_commands.json" ]]; then
    # Use Python script to merge the two JSON arrays and filter out GCC-specific flags
    # that clang-tidy doesn't recognize
    python3 "$PROJECT_ROOT/tools/github/clang-tidy/merge_compile_commands.py"
  else
    echo "Warning: Main build/compile_commands.json not found, cannot merge mage compile commands"
  fi
fi

echo "Running clang-tidy on changed files..."
echo "Using clang-tidy: $(command -v clang-tidy)"
echo "clang-tidy version: $(clang-tidy --version | head -1)"
echo "Config file: $CONFIG_FILE"
echo "Parallel jobs: $THREADS (CPU cores: $MAX_THREADS_BY_CPU, Memory limit: $MAX_THREADS_BY_MEM based on ${AVAILABLE_MEM_GB}GB available)"

FIXES_DIR="./build/clang_tidy_fixes"
FIXES_FILE="$FIXES_DIR/fixes.yaml"
EXPORT_FIXES_FLAG=""
if [[ -n "$FIX_FLAG" ]]; then
  echo "Fix mode: ENABLED (using export-fixes to handle parallel processing)"
  rm -rf "$FIXES_DIR"
  mkdir -p "$FIXES_DIR"
  EXPORT_FIXES_FLAG="-export-fixes $FIXES_FILE"
fi

git diff -U0 $BASE_BRANCH... -- src mage/cpp $EXCLUSIONS | ./tools/github/clang-tidy/clang-tidy-diff.py -p 1 -j $THREADS -path build -regex ".+\.cppm?" -config-file "$CONFIG_FILE" $EXPORT_FIXES_FLAG 2>&1 | tee ./build/clang_tidy_output.txt

echo ""

# Apply fixes using clang-apply-replacements if we exported fixes
if [[ -n "$FIX_FLAG" && -f "$FIXES_FILE" && -s "$FIXES_FILE" ]]; then
    echo "Applying fixes from $FIXES_FILE..."
    clang-apply-replacements --style=file "$FIXES_DIR" 2>&1 || true
    echo "Fixes applied."
fi

echo "Checking for errors in clang-tidy output..."
# Fail if any warning is reported
if ./tools/github/clang-tidy/grep_error_lines.sh < ./build/clang_tidy_output.txt > /dev/null; then
    echo "Error: clang-tidy found issues:"
    ./tools/github/clang-tidy/grep_error_lines.sh < ./build/clang_tidy_output.txt
    exit 1
fi
echo "No clang-tidy errors found."
