#!/bin/bash

# Docker Cleanup Script for Memgraph
# This script performs a thorough Docker cleanup while preserving specific images
# and provides system disk usage information.

set -euo pipefail
set -v
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

# Function to get current Docker system usage
show_docker_usage() {
    print_status "Current Docker system usage:"
    docker system df
    echo
}

# Function to remove all images except the ones we want to preserve
remove_unwanted_images() {
    local preserved_images=(
        "memgraph/mgbuild:v6_ubuntu-24.04"
        "memgraph/mgbuild:v7_ubuntu-24.04"
        "memgraph/mgbuild:v6_debian-12"
    )
    
    print_status "Removing unwanted images (preserving specific ones)..."
    
    # Get all image IDs
    local all_images=$(docker images -q)
    
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
    local mgdeps_images=$(docker images --format "{{.Repository}}:{{.Tag}} {{.ID}}" | grep "mgdeps-cache" | awk '{print $2}')
    for image_id in $mgdeps_images; do
        if [ -n "$image_id" ]; then
            preserve_ids+=("$image_id")
            local image_name=$(docker images --format "{{.Repository}}:{{.Tag}}" "$image_id")
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
            local image_name=$(docker images --format "{{.Repository}}:{{.Tag}}" "$image_id")
            print_status "Removing: $image_name"
            docker rmi "$image_id" 2>/dev/null || print_warning "Failed to remove $image_name (may be in use)"
            removed_count=$((removed_count + 1))
        fi
    done
    
    print_success "Removed $removed_count unwanted images"
}

# Function to perform Docker cleanup
perform_cleanup() {
    print_status "Starting Docker cleanup..."
    
    # Stop and remove containers (except those using mgdeps-cache images)
    print_status "Processing containers (preserving mgdeps-cache containers)..."
    
    # Get all containers (including stopped ones) using mgdeps-cache images
    local mgdeps_containers=$(docker ps -a --format "{{.ID}}" --filter "ancestor=mgdeps-cache:4.1")
    
    # Process all containers
    local all_containers=$(docker ps -aq)
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
            
            local container_name=$(docker ps -a --format "{{.Names}}" --filter "id=$container_id")
            local container_status=$(docker ps -a --format "{{.Status}}" --filter "id=$container_id")
            
            if [ "$is_mgdeps" = false ]; then
                # Stop if running, then remove
                if [[ "$container_status" == Up* ]]; then
                    print_status "Stopping container: $container_name"
                    docker stop "$container_id"
                    stopped_count=$((stopped_count + 1))
                fi
                print_status "Removing container: $container_name"
                if docker rm "$container_id" 2>/dev/null; then
                    removed_count=$((removed_count + 1))
                else
                    print_warning "Container $container_name may have been auto-removed (--rm flag) or already removed"
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
    if [ "$(docker volume ls -q)" ]; then
        docker volume rm $(docker volume ls -q)
        print_success "All volumes removed"
    else
        print_status "No volumes to remove"
    fi
    
    # Remove all networks (except default ones)
    print_status "Removing custom networks..."
    local custom_networks=$(docker network ls --format "{{.Name}}" | grep -v -E "^(bridge|host|none)$" || true)
    if [ -n "$custom_networks" ]; then
        echo "$custom_networks" | while read -r network; do
            if [ -n "$network" ]; then
                docker network rm "$network" 2>/dev/null || true
            fi
        done
        print_success "Custom networks removed"
    else
        print_status "No custom networks to remove"
    fi
    
    # Clean up build cache
    print_status "Cleaning up build cache..."
    docker builder prune -af
    print_success "Build cache cleaned up"
    
    # System prune (without removing images, since we handle that separately)
    print_status "Running docker system prune..."
    docker system prune -f --volumes
    print_success "System prune completed"
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
    docker images --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}"
}

# Run main function
main "$@"
