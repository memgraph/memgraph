#!/bin/bash

# Wait for docker images pulled by pull-e2e-docker-images.sh to be available
# Timeout: 2 minutes (120 seconds)

IMAGES=("mysql:8.0" "postgres:15.14" "neo4j:5.10.0")
TIMEOUT=120
INTERVAL=2
ELAPSED=0

check_images() {
    for image in "${IMAGES[@]}"; do
        if ! docker image inspect "$image" >/dev/null 2>&1; then
            return 1
        fi
    done
    return 0
}

echo "Waiting for docker images to be available (timeout: ${TIMEOUT}s)..."
echo "Images: ${IMAGES[*]}"

while [ $ELAPSED -lt $TIMEOUT ]; do
    if check_images; then
        echo "All docker images are available!"
        exit 0
    fi

    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
    echo "Still waiting... (${ELAPSED}s/${TIMEOUT}s)"
done

echo "Timeout: Docker images not available after ${TIMEOUT} seconds"
exit 1
