#!/bin/bash

# Matches timestamp like "2021-03-25T17:06:42.2621697Z"
TIMESTAMP_PATTERN="\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}Z"

# Matches absolute file paths with line and column identifier like
# "/opt/actions-runner/_work/memgraph/memgraph/src/utils/exceptions.hpp:71:11:"
FILE_ABSOLUTE_PATH_PATTERN="/[^:]+:\d+:\d+:"

ERROR_OR_WARNING_PATTERN="(error|warning):"

grep -P "^($TIMESTAMP_PATTERN )?$FILE_ABSOLUTE_PATH_PATTERN $ERROR_OR_WARNING_PATTERN.*$"
