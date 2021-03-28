#!/bin/bash

# TODO(gitbuda): To build Docker image, just use Debian10 package + the release/docker/package_deb_docker.
# TODO(gitbuda): Test the target packages somehow.

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SUPPORTED_OFFERING=(community enterprise)
SUPPORTED_OS=(centos-7 debian-9 debian-10 ubuntu-18.04)
PROJECT_ROOT="$SCRIPT_DIR/../.."
ACTIVATE_TOOLCHAIN="source /opt/toolchain-v2/activate"

print_help () {
    echo "$0 init|package {offering} {os}"
    echo ""
    echo "    offerings: ${SUPPORTED_OFFERING[*]}"
    echo "    OSs: ${SUPPORTED_OS[*]}"
    exit 1
}

make_package () {
    offering="$1"
    offering_flag=""
    if [[ "$offering" == "community" ]]; then
        offering_flag=" -DMG_ENTERPRISE=OFF "
    fi
    os="$2"
    package_command=""
    if [[ "$os" =~ ^"centos".* ]]; then
        package_command=" cpack -G RPM --config ../CPackConfig.cmake && rpmlint memgraph*.rpm "
    fi
    if [[ "$os" =~ ^"debian".* ]]; then
        package_command=" cpack -G DEB --config ../CPackConfig.cmake "
    fi
    if [[ "$os" =~ ^"ubuntu".* ]]; then
        package_command=" cpack -G DEB --config ../CPackConfig.cmake "
    fi
    build_container="legacy-mgbuild_$os"
    echo "Building Memgraph $offering for $os on $build_container..."

    echo "Copying project files..."
    # Required here because Docker build container can't access remote.
    cd "$PROJECT_ROOT" && git fetch origin master:master
    docker exec "$build_container" mkdir -p /memgraph
    docker cp "$PROJECT_ROOT/." "$build_container:/memgraph/"

    container_build_dir="/memgraph/build"
    container_output_dir="$container_build_dir/output"

    # TODO(gitbuda): TOOLCHAIN_RUN_DEPS should be installed during the Docker
    # image build phase, but that is not easy at this point because the
    # environment/os/{os}.sh does not come within the toolchain package. When
    # migrating to the next version of toolchain do that, and remove the
    # TOOLCHAIN_RUN_DEPS installation from here.
    echo "Installing dependencies..."
    docker exec "$build_container" bash -c "/memgraph/environment/os/$os.sh install TOOLCHAIN_RUN_DEPS"
    docker exec "$build_container" bash -c "/memgraph/environment/os/$os.sh install MEMGRAPH_BUILD_DEPS"

    echo "Building targeted package..."
    docker exec "$build_container" bash -c "cd /memgraph && ./init"
    docker exec "$build_container" bash -c "cd $container_build_dir && rm -rf ./*"
    docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN && cmake -DCMAKE_BUILD_TYPE=release $offering_flag .."
    # ' is used instead of " because we need to run make within the allowed
    # container resources.
    # shellcheck disable=SC2016
    docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN "'&& make -j$(nproc)'
    docker exec "$build_container" bash -c "mkdir -p $container_output_dir && cd $container_output_dir && $ACTIVATE_TOOLCHAIN && $package_command"

    echo "Copying targeted package to host..."
    last_package_name=$(docker exec "$build_container" bash -c "cd $container_output_dir && ls -t memgraph* | head -1")
    host_output_dir="$PROJECT_ROOT/build/output"
    mkdir -p "$host_output_dir"
    docker cp "$build_container:$container_output_dir/$last_package_name" "$host_output_dir/$last_package_name"
}

case "$1" in
    init)
        docker-compose build
        docker-compose up -d
    ;;
    package)
        shift 1
        offering="$1"
        shift 1
        is_offering_ok=false
        for supported_offering in "${SUPPORTED_OFFERING[@]}"; do
            if [[ "$supported_offering" == "${offering}" ]]; then
                is_offering_ok=true
            fi
        done
        os="$1"
        shift 1
        is_os_ok=false
        for supported_os in "${SUPPORTED_OS[@]}"; do
            if [[ "$supported_os" == "${os}" ]]; then
                is_os_ok=true
            fi
        done
        if [[ "$is_offering_ok" == true ]] && [[ "$is_os_ok" == true ]]; then
            make_package "$offering" "$os"
        else
            print_help
        fi
    ;;
    *)
        print_help
    ;;
esac
