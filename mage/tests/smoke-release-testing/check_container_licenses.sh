#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

check_container_licenses() {
    # Check that the MEL.pdf, BSL.txt and APL.txt licenses are installed under /usr/share/doc/memgraph/
    echo "Checking container licenses..."

    licenses=(
        "MEL.pdf"
        "BSL.txt"
        "APL.txt"
    )
    for license in "${licenses[@]}"; do
        docker exec -u root memgraph_next_data bash -c "test -f /usr/share/doc/memgraph/$license" 2>&1 > /dev/null
        if [ $? -ne 0 ]; then
            echo "Error: $license license not found"
            exit 1
        else
            echo "License $license found"
        fi
    done
    echo "Container licenses found"
}
