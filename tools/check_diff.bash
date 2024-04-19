#!/bin/bash

# This script checks if the diff between the current branch and the master branch
# contains only changes to the files in the .github/workflows directory.
# If so, it returns exit code 1, otherwise it returns exit code 0.
# Additionally, if there are changes to files starting with 'diff' it returns 0.

diff=$(git diff --name-only origin/master)
regex_workflow='^\.github/workflows/.*\.yaml$'
regex_diff='^\.github/workflows/diff.*\.yaml$'
echo $diff
for file in $diff; do
  if [[ $file =~ $regex_workflow ]] && ! [[ $file =~ $regex_diff ]]; then
    continue
  else
    exit 0
  fi
done
exit 1
