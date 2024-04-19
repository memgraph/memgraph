#!/bin/bash
# This script checks if the diff workflow should be run.
# Exit code 0 means the workflow should run, exit code 1 means it should not.
# If only the files in .github/workflows directory are changed, the workflow should not run.
# The only exception is the diff workflow itself and the workflows that are used to run the diff workflow.

diff=$(git diff --name-only origin/master)
regex_workflow='^\.github/workflows/.*\.yaml$'
regex_diff='^\.github/workflows/diff.*\.yaml$'
echo "Changed files in comparison to master: $diff"
for file in $diff; do
  if [[ $file =~ $regex_workflow ]] && ! [[ $file =~ $regex_diff ]]; then
    continue
  else
    exit 0
  fi
done
exit 1
