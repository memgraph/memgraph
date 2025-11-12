#!/bin/bash


RUN_ID=$1
RUN_NUMBER=$2
BRANCH_NAME=$3

if [ -z "$RUN_ID" ] || [ -z "$RUN_NUMBER" ] || [ -z "$BRANCH_NAME" ]; then
    echo "Usage: $0 <run_id> <run_number> <branch_name>"
    exit 1
fi

TOOLCHAIN=${TOOLCHAIN:-v7}
OS=${OS:-ubuntu-24.04}
ARCH=${ARCH:-amd}
MEMGRAPH_ENTERPRISE_LICENSE=${MEMGRAPH_ENTERPRISE_LICENSE:-}
MEMGRAPH_ORGANIZATION_NAME=${MEMGRAPH_ORGANIZATION_NAME:-}
LOOP_COUNT=${LOOP_COUNT:-10}

if [ -z "$MEMGRAPH_ENTERPRISE_LICENSE" ] || [ -z "$MEMGRAPH_ORGANIZATION_NAME" ]; then
    echo "MEMGRAPH_ENTERPRISE_LICENSE and MEMGRAPH_ORGANIZATION_NAME must be set"
    exit 1
fi

FAIL_COUNT=0
for ((i=1; i<=LOOP_COUNT; i++)); do
    if ! ./release/package/mgbuild.sh \
        --toolchain $TOOLCHAIN \
        --os $OS \
        --arch $ARCH \
        --enterprise-license $MEMGRAPH_ENTERPRISE_LICENSE \
        --organization-name $MEMGRAPH_ORGANIZATION_NAME \
        test-memgraph macro-benchmark; then
        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi

    if ! ./release/package/mgbuild.sh \
        --toolchain $TOOLCHAIN \
        --os $OS \
        --arch $ARCH \
        --enterprise-license $MEMGRAPH_ENTERPRISE_LICENSE \
        --organization-name $MEMGRAPH_ORGANIZATION_NAME \
        test-memgraph upload-to-bench-graph \
        --benchmark-name "macro_benchmark" \
        --benchmark-results "../../tests/macro_benchmark/.harness_summary" \
        --github-run-id $RUN_ID \
        --github-run-number $RUN_NUMBER \
        --head-branch-name $BRANCH_NAME; then
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
done

if [ $FAIL_COUNT -gt 0 ]; then
    echo "Failed $FAIL_COUNT times"
    exit 1
fi

echo "Succeeded $LOOP_COUNT times"
exit 0
