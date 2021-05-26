#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SUPPORTED_OFFERING=(community enterprise)
SUPPORTED_OS=(centos-7 centos-8 debian-9 debian-10 ubuntu-18.04 ubuntu-20.04)
PROJECT_ROOT="$SCRIPT_DIR/../.."
ACTIVATE_TOOLCHAIN="source /opt/toolchain-v2/activate"
HOST_OUTPUT_DIR="$PROJECT_ROOT/build/output"

print_help () {
    echo "$0 init|package {offering} {os} [--for-docker]|docker|test"
    echo ""
    echo "    offerings: ${SUPPORTED_OFFERING[*]}"
    echo "    OSs: ${SUPPORTED_OS[*]}"
    exit 1
}

make_package () {
    offering="$1"
    offering_flag=" -DMG_ENTERPRISE=OFF "
    if [[ "$offering" == "enterprise" ]]; then
        offering_flag=" -DMG_ENTERPRISE=ON "
    fi
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
    docker_flag=" -DBUILD_FOR_DOCKER=OFF "
    if [[ "$#" -gt 2 ]]; then
        if [[ "$3" == "--for-docker" ]]; then
            docker_flag=" -DBUILD_FOR_DOCKER=ON "
        fi
    fi
    build_container="mgbuild_$os"
    echo "Building Memgraph $offering for $os on $build_container..."

    echo "Copying project files..."
    # If master is not the current branch, fetch it, because the get_version
    # script depends on it. If we are on master, the fetch command is going to
    # fail so that's why there is the explicit check.
    # Required here because Docker build container can't access remote.
    cd "$PROJECT_ROOT"
    if [[ "$(git rev-parse --abbrev-ref HEAD)" != "master" ]]; then
        git fetch origin master:master
    fi
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
    docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN && cmake -DCMAKE_BUILD_TYPE=release $offering_flag $docker_flag .."
    # ' is used instead of " because we need to run make within the allowed
    # container resources.
    # shellcheck disable=SC2016
    docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN "'&& make -j$(nproc)'
    docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN "'&& make -j$(nproc) -B mgconsole'
    docker exec "$build_container" bash -c "mkdir -p $container_output_dir && cd $container_output_dir && $ACTIVATE_TOOLCHAIN && $package_command"

    echo "Copying targeted package to host..."
    last_package_name=$(docker exec "$build_container" bash -c "cd $container_output_dir && ls -t memgraph* | head -1")
    # The operating system folder is introduced because multiple different
    # packages could be preserved during the same build "session".
    mkdir -p "$HOST_OUTPUT_DIR/$os"
    package_host_destination="$HOST_OUTPUT_DIR/$os/$last_package_name"
    docker cp "$build_container:$container_output_dir/$last_package_name" "$package_host_destination"
    echo "Package saved to $package_host_destination."
}

case "$1" in
    init)
        cd "$SCRIPT_DIR"
        docker-compose build
        docker-compose up -d
    ;;

    docker)
        # NOTE: Docker is build on top of Debian 10 package.
        based_on_os="debian-10"
        # shellcheck disable=SC2012
        last_package_name=$(cd "$HOST_OUTPUT_DIR/$based_on_os" && ls -t memgraph* | head -1)
        docker_build_folder="$PROJECT_ROOT/release/docker"
        cd "$docker_build_folder"
        ./package_deb_docker --latest "$HOST_OUTPUT_DIR/$based_on_os/$last_package_name"
        # shellcheck disable=SC2012
        docker_image_name=$(cd "$docker_build_folder" && ls -t memgraph* | head -1)
        docker_host_folder="$HOST_OUTPUT_DIR/docker"
        docker_host_image_path="$docker_host_folder/$docker_image_name"
        mkdir -p "$docker_host_folder"
        cp "$docker_build_folder/$docker_image_name" "$docker_host_image_path"
        echo "Docker images saved to $docker_host_image_path."
    ;;

    package)
        shift 1
        if [[ "$#" -lt 2 ]]; then
            print_help
        fi
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
            make_package "$offering" "$os" "$@"
        else
            print_help
        fi
    ;;

    test)
        echo "TODO(gitbuda): Test all packages on mgtest containers."
    ;;

    *)
        print_help
    ;;
esac
