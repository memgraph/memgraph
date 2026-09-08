#!/bin/bash
# Retries a flaky driver download: a transient network error on the runner
# (e.g. "Network is unreachable") says nothing about the driver under test.
wget_retry() {
    local attempt
    for attempt in 1 2 3 4 5; do
        (( attempt == 1 )) || sleep $(( attempt * 5 ))
        wget -nv "$@" && return 0
        echo "wget attempt $attempt/5 failed: $*" >&2
    done
    return 1
}
