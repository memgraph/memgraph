#!/bin/bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SUPPORTED_TOOLCHAINS=(
    v4 v5
)

SUPPORTED_OS=(
    centos-7 centos-9
    debian-10 debian-11 debian-11-arm debian-12 debian-12-arm
    ubuntu-18.04 ubuntu-20.04 ubuntu-22.04 ubuntu-22.04-arm
    fedora-36
    amzn-2
)

SUPPORTED_BUILD_TYPES=(
    Debug
    Release
    RelWithDebInfo
)

SUPPORTED_ARCHS=(
    amd
    arm
)

PROJECT_ROOT="$SCRIPT_DIR/../.."
# TODO(gitbuda): Toolchain is now specific for a given OS -> ADJUST:
#   * under init, toolchain version is passed to docker compose -> consider having arch + toolchain version as a folder structure
#   * for a give OS latest possible toolchain should be picked -> there is only one toolchain per OS possible
HOST_OUTPUT_DIR="$PROJECT_ROOT/build/output"

print_help () {
    echo "$0 init {toolchain _version} {arch} [--build-only | --no-build] | docker | build {toolchain _version} {os} | package {toolchain _version} {os} {build_type} [--for-docker|--for-platform]"
    echo ""
    echo "    Archs: ${SUPPORTED_ARCHS[*]}"
    echo "    Build types: ${SUPPORTED_BUILD_TYPES[*]}"
    echo "    OSs: ${SUPPORTED_OS[*]}"
    echo "    Toolchain versions: ${SUPPORTED_TOOLCHAINS[*]}"
    exit 1
}

check_support() {
  local is_supported=false
  case "$1" in
    arch)
      for e in "${SUPPORTED_ARCHS[@]}"; do
        if [[ "$e" == "$2" ]]; then
          is_supported=true
          break
        fi
      done
      if [[ "$is_supported" == false ]]; then
        echo -e "Architecture $2 isn't supported, choose from  ${SUPPORTED_ARCHS[*]}"
        exit 1
      fi
    ;;
    build_type)
      for e in "${SUPPORTED_BUILD_TYPES[@]}"; do
        if [[ "$e" == "$2" ]]; then
          is_supported=true
          break
        fi
      done
      if [[ "$is_supported" == false ]]; then
        echo -e "Build type $2 isn't supported, choose from  ${SUPPORTED_BUILD_TYPES[*]}"
        exit 1
      fi
    ;;
    os)
      for e in "${SUPPORTED_OS[@]}"; do
        if [[ "$e" == "$2" ]]; then
          is_supported=true
          break
        fi
      done
      if [[ "$is_supported" == false ]]; then
        echo -e "OS $2 isn't supported, choose from  ${SUPPORTED_OS[*]}"
        exit 1
      fi
    ;;
    toolchain)
      for e in "${SUPPORTED_TOOLCHAINS[@]}"; do
        if [[ "$e" == "$2" ]]; then
          is_supported=true
          break
        fi
      done
      if [[ "$is_supported" == false ]]; then
        echo -e "Toolchain version $2 isn't supported, choose from  ${SUPPORTED_TOOLCHAINS[*]}"
        exit 1
      fi
    ;;
    *)
      echo -e "This function can only check arch, build_type, os and toolchain version"
      exit 1
    ;;
  esac
}

make_package () {
    toolchain_version="$1"
    os="$2"
    build_type="$3"

    build_container="mgbuild_${toolchain_version}_${os}"
    echo "Building Memgraph for $os on $build_container..."

    local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"

    package_command=""
    if [[ "$os" =~ ^"centos".* ]] || [[ "$os" =~ ^"fedora".* ]] || [[ "$os" =~ ^"amzn".* ]]; then
        docker exec "$build_container" bash -c "yum -y update"
        package_command=" cpack -G RPM --config ../CPackConfig.cmake && rpmlint --file='../../release/rpm/rpmlintrc' memgraph*.rpm "
    fi
    if [[ "$os" =~ ^"debian".* ]]; then
        docker exec "$build_container" bash -c "apt --allow-releaseinfo-change -y update"
        package_command=" cpack -G DEB --config ../CPackConfig.cmake "
    fi
    if [[ "$os" =~ ^"ubuntu".* ]]; then
        docker exec "$build_container" bash -c "apt update"
        package_command=" cpack -G DEB --config ../CPackConfig.cmake "
    fi
    telemetry_id_override_flag=""
    if [[ "$#" -gt 3 ]]; then
        if [[ "$4" == "--for-docker" ]]; then
            telemetry_id_override_flag=" -DMG_TELEMETRY_ID_OVERRIDE=DOCKER "
        elif [[ "$4" == "--for-platform" ]]; then
            telemetry_id_override_flag=" -DMG_TELEMETRY_ID_OVERRIDE=DOCKER-PLATFORM"
        else
          print_help
          exit
        fi
    fi

    echo "Copying project files..."
    # If master is not the current branch, fetch it, because the get_version
    # script depends on it. If we are on master, the fetch command is going to
    # fail so that's why there is the explicit check.
    # Required here because Docker build container can't access remote.
    cd "$PROJECT_ROOT"
    if [[ "$(git rev-parse --abbrev-ref HEAD)" != "master" ]]; then
        git fetch origin master:master
    fi

    # Ensure we have a clean build directory
    docker exec "$build_container" rm -rf /memgraph

    docker exec "$build_container" mkdir -p /memgraph
    # TODO(gitbuda): Revisit copying the whole repo -> makese sense under CI.
    docker cp "$PROJECT_ROOT/." "$build_container:/memgraph/"

    container_build_dir="/memgraph/build"
    container_output_dir="$container_build_dir/output"

    # TODO(gitbuda): TOOLCHAIN_RUN_DEPS should be installed during the Docker
    # image build phase, but that is not easy at this point because the
    # environment/os/{os}.sh does not come within the toolchain package. When
    # migrating to the next version of toolchain do that, and remove the
    # TOOLCHAIN_RUN_DEPS installation from here.
    # TODO(gitbuda): On the other side, having this here allows updating deps
    # wihout reruning the build containers.
    echo "Installing dependencies using '/memgraph/environment/os/$os.sh' script..."
    docker exec "$build_container" bash -c "/memgraph/environment/os/$os.sh install TOOLCHAIN_RUN_DEPS"
    docker exec "$build_container" bash -c "/memgraph/environment/os/$os.sh install MEMGRAPH_BUILD_DEPS"

    echo "Building targeted package..."
    # Fix issue with git marking directory as not safe
    docker exec "$build_container" bash -c "cd /memgraph && git config --global --add safe.directory '*'"
    docker exec "$build_container" bash -c "cd /memgraph && $ACTIVATE_TOOLCHAIN && ./init"
    docker exec "$build_container" bash -c "cd $container_build_dir && rm -rf ./*"
    # Fix cmake failing locally if remote is clone via ssh
    docker exec "$build_container" bash -c "cd /memgraph && git remote set-url origin https://github.com/memgraph/memgraph.git"
    if [[ "$os" =~ "-arm" ]]; then
        docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN && cmake -DCMAKE_BUILD_TYPE=$build_type -DMG_ARCH="ARM64" $telemetry_id_override_flag .."
    else
        docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN && cmake -DCMAKE_BUILD_TYPE=$build_type $telemetry_id_override_flag .."
    fi
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

if [[ "$#" -lt 1 ]]; then
    print_help
fi
case "$1" in
    init)
        shift 1
        if [[ "$#" -lt 2 ]]; then
            print_help
        fi
        toolchain_version="$1"
        check_support toolchain $toolchain_version
        arch="$2"
        check_support arch $arch
        cd "$SCRIPT_DIR"
        if ! which "docker-compose" >/dev/null; then
            docker_compose_cmd="docker compose"
        else
            docker_compose_cmd="docker-compose"
        fi
        if [[ "$#" -gt 2 ]]; then
          if [[ "$3" == "--build-only" ]]; then
            $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml build
          elif [[ "$3" == "--no-build" ]]; then
            $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml up -d
          else
            print_help
          fi
        else
          $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml up -d --build
        fi
    ;;

    docker)
        # NOTE: Docker is build on top of Debian 11 package.
        based_on_os="debian-11"
        # shellcheck disable=SC2012
        last_package_name=$(cd "$HOST_OUTPUT_DIR/$based_on_os" && ls -t memgraph* | head -1)
        docker_build_folder="$PROJECT_ROOT/release/docker"
        cd "$docker_build_folder"
        ./package_docker --latest "$HOST_OUTPUT_DIR/$based_on_os/$last_package_name"
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
        if [[ "$#" -lt 3 ]]; then
            print_help
        fi
        toolchain_version="$1"
        check_support toolchain $toolchain_version
        os="$2"
        check_support os $os
        build_type="$3"
        check_support build_type $arch
        shift 2
        make_package "$toolchain_version" "$os" "$build_type" "$@"
    ;;

    build)
      shift 1
      if [[ "$#" -ne 2 ]]; then
          print_help
      fi
      toolchain_version="$1"
      check_support toolchain $toolchain_version
      os="$2"
      check_support os $os
      cd "$SCRIPT_DIR/$os"
      docker build -f Dockerfile --build-arg TOOLCHAIN_VERSION="toolchain-$toolchain_version" -t "memgraph/memgraph-builder:v${toolchain_version}_$os" .
    ;;

    test)
        echo "TODO(gitbuda): Test all packages on mgtest containers."
    ;;

    *)
        print_help
    ;;
esac
