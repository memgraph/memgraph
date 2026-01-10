#!/bin/bash

# Docker Cleanup Script for Memgraph
# This script performs a thorough Docker cleanup while preserving specific images
# and provides system disk usage information.

set -uo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running or not accessible. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running and accessible"
}

# Function to safely execute Docker commands with error handling
safe_docker_cmd() {
    local cmd="$1"
    local description="$2"
    local continue_on_error="${3:-true}"

    if eval "$cmd" 2>/dev/null; then
        return 0
    else
        local exit_code=$?
        if [ "$continue_on_error" = "true" ]; then
            print_warning "$description failed (exit code: $exit_code) - continuing..."
            return 0
        else
            print_error "$description failed (exit code: $exit_code)"
            return $exit_code
        fi
    fi
}

# Function to get current Docker system usage
show_docker_usage() {
    print_status "Current Docker system usage:"
    docker system df
    echo
}

# Function to remove all images except the ones we want to preserve
remove_unwanted_images() {
    local preserved_images=(
        "memgraph/mgbuild:v7_ubuntu-24.04"
        "memgraph/mgbuild:v7_ubuntu-24.04-arm"
        "memgraph/mgbuild:v7_debian-12"
    )

    print_status "Removing unwanted images (preserving specific ones)..."

    # Get all image IDs
    local all_images=$(docker images -q 2>/dev/null || true)

    if [ -z "$all_images" ]; then
        print_status "No images to remove"
        return
    fi

    # Create a list of images to preserve
    local preserve_ids=()

    # Add specific images to preserve list
    for image in "${preserved_images[@]}"; do
        local image_id=$(docker images --format "{{.ID}}" "$image" 2>/dev/null)
        if [ -n "$image_id" ]; then
            preserve_ids+=("$image_id")
            print_status "Will preserve: $image"
        fi
    done

    # Add mgdeps-cache images to preserve list
    local mgdeps_images=$(docker images --format "{{.Repository}}:{{.Tag}} {{.ID}}" 2>/dev/null | grep "mgdeps-cache" | awk '{print $2}' || true)
    for image_id in $mgdeps_images; do
        if [ -n "$image_id" ]; then
            preserve_ids+=("$image_id")
            local image_name=$(docker images --format "{{.Repository}}:{{.Tag}}" "$image_id" 2>/dev/null || echo "mgdeps-cache:unknown")
            print_status "Will preserve: $image_name"
        fi
    done

    # Remove images that are not in the preserve list
    local removed_count=0
    for image_id in $all_images; do
        local should_preserve=false
        for preserve_id in "${preserve_ids[@]}"; do
            if [ "$image_id" = "$preserve_id" ]; then
                should_preserve=true
                break
            fi
        done

        if [ "$should_preserve" = false ]; then
            local image_name=$(docker images --format "{{.Repository}}:{{.Tag}}" "$image_id" 2>/dev/null || echo "unknown")
            print_status "Removing: $image_name"
            if safe_docker_cmd "docker rmi '$image_id'" "Remove image $image_name"; then
                removed_count=$((removed_count + 1))
            else
                print_warning "Failed to remove $image_name (may be in use or already removed by another process)"
            fi
        fi
    done

    print_success "Removed $removed_count unwanted images"
}

# Function to perform Docker cleanup
perform_cleanup() {
    print_status "Starting Docker cleanup..."

    # Stop and remove containers (except those using mgdeps-cache images)
    print_status "Processing containers (preserving mgdeps-cache containers)..."

    # Get all containers (including stopped ones) with mgdeps-cache in the name
    local mgdeps_containers=$(docker ps -a --format "{{.ID}}" --filter "name=mgdeps-cache" 2>/dev/null || true)

    # Process all containers
    local all_containers=$(docker ps -aq 2>/dev/null || true)
    if [ -n "$all_containers" ]; then
        local stopped_count=0
        local removed_count=0
        local preserved_count=0

        for container_id in $all_containers; do
            local is_mgdeps=false
            for mgdeps_container in $mgdeps_containers; do
                if [ "$container_id" = "$mgdeps_container" ]; then
                    is_mgdeps=true
                    break
                fi
            done

            local container_name=$(docker ps -a --format "{{.Names}}" --filter "id=$container_id" 2>/dev/null || echo "unknown")
            local container_status=$(docker ps -a --format "{{.Status}}" --filter "id=$container_id" 2>/dev/null || echo "unknown")

            if [ "$is_mgdeps" = false ]; then
                # Stop if running, then remove
                if [[ "$container_status" == Up* ]]; then
                    print_status "Stopping container: $container_name"
                    if safe_docker_cmd "docker stop '$container_id'" "Stop container $container_name"; then
                        stopped_count=$((stopped_count + 1))
                    fi
                fi
                print_status "Removing container: $container_name"
                if safe_docker_cmd "docker rm '$container_id'" "Remove container $container_name"; then
                    removed_count=$((removed_count + 1))
                else
                    print_warning "Container $container_name may have been auto-removed (--rm flag) or already removed by another process"
                fi
            else
                print_status "Preserving mgdeps-cache container: $container_name"
                preserved_count=$((preserved_count + 1))
            fi
        done

        print_success "Container processing complete: $stopped_count stopped, $removed_count removed, $preserved_count preserved"
    else
        print_status "No containers to process"
    fi


    # Remove all volumes
    print_status "Removing all volumes..."
    local volumes_to_remove=$(docker volume ls -q 2>/dev/null || true)
    if [ -n "$volumes_to_remove" ]; then
        local removed_volumes=0
        local failed_volumes=0
        for volume in $volumes_to_remove; do
            if safe_docker_cmd "docker volume rm '$volume'" "Remove volume $volume"; then
                removed_volumes=$((removed_volumes + 1))
            else
                failed_volumes=$((failed_volumes + 1))
            fi
        done
        if [ $failed_volumes -eq 0 ]; then
            print_success "All volumes removed ($removed_volumes total)"
        else
            print_warning "Volume removal completed: $removed_volumes removed, $failed_volumes failed (may have been removed by another process)"
        fi
    else
        print_status "No volumes to remove"
    fi

    # Remove all networks (except default ones)
    print_status "Removing custom networks..."
    local custom_networks=$(docker network ls --format "{{.Name}}" 2>/dev/null | grep -v -E "^(bridge|host|none)$" || true)
    if [ -n "$custom_networks" ]; then
        local removed_networks=0
        local failed_networks=0
        while IFS= read -r network; do
            if [ -n "$network" ]; then
                if safe_docker_cmd "docker network rm '$network'" "Remove network $network"; then
                    removed_networks=$((removed_networks + 1))
                else
                    failed_networks=$((failed_networks + 1))
                fi
            fi
        done <<< "$custom_networks"
        if [ $failed_networks -eq 0 ]; then
            print_success "Custom networks removed ($removed_networks total)"
        else
            print_warning "Network removal completed: $removed_networks removed, $failed_networks failed (may have been removed by another process)"
        fi
    else
        print_status "No custom networks to remove"
    fi

    # Clean up build cache
    print_status "Cleaning up build cache..."
    if safe_docker_cmd "docker builder prune -af" "Clean up build cache"; then
        print_success "Build cache cleaned up"
    else
        print_warning "Build cache cleanup had issues but continuing..."
    fi

    # System prune (without removing images, since we handle that separately)
    print_status "Running docker system prune..."
    if safe_docker_cmd "docker system prune -f --volumes" "System prune"; then
        print_success "System prune completed"
    else
        print_warning "System prune had issues but continuing..."
    fi
}

# Function to get disk usage information
get_disk_usage() {
    print_status "Disk usage information:"

    # Check if /var/lib/docker has its own partition
    local docker_partition=$(df -h /var/lib/docker 2>/dev/null | tail -n 1 | awk '{print $1}')
    local root_partition=$(df -h / 2>/dev/null | tail -n 1 | awk '{print $1}')

    if [ "$docker_partition" != "$root_partition" ]; then
        print_status "/var/lib/docker has its own partition:"
        df -h /var/lib/docker
    else
        print_status "/var/lib/docker is on the root partition:"
        df -h /
    fi
    echo
}

# Main execution
main() {
    print_status "Starting Docker cleanup process..."
    echo

    # Check Docker availability
    check_docker
    echo

    # Show initial Docker usage
    print_status "Initial Docker system usage:"
    show_docker_usage

    # Perform cleanup (containers, volumes, networks, system prune)
    perform_cleanup
    echo

    # Remove unwanted images (preserving specific ones)
    remove_unwanted_images
    echo

    # Show final Docker usage
    print_status "Final Docker system usage:"
    show_docker_usage

    # Show disk usage
    get_disk_usage

    print_success "Docker cleanup completed successfully!"
    print_status "Preserved images:"
    docker images --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}" 2>/dev/null || print_warning "Could not list final images"
}

# Run main function
main "$@"
