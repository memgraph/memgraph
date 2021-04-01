#!/bin/bash

# the first sort | uniq is necessary, because the same occurrence of the same error
# can be reported from headers when they are included in multiple source files
`dirname ${BASH_SOURCE[0]}`/grep_error_lines.sh |
    sort | uniq |
    sed -E 's/.*\[(.*)\]\r?$/\1/g' | # extract the check name from [check-name]
    sort | uniq -c |                 # count each type of check
    sort -nr                         # sort them into descending order
