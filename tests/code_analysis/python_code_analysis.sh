#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."
BASE_BRANCH="origin/master"

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
CHANGED_FILES=$(git diff -U0 $BASE_BRANCH... --name-only --diff-filter=d)
for file in ${CHANGED_FILES}; do
  echo ${file}
  if [[ ${file} == *.py ]]; then
    python3 -m black --check --diff ${file}
    python3 -m isort --profile black --check-only --diff ${file}
  fi
done
cd $SCRIPT_DIR
