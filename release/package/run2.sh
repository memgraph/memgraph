#!/bin/bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$SCRIPT_DIR/../.."
HOST_OUTPUT_DIR="$ROOT_DIR/build/output"

DEFAULT_TOOLCHAIN="v5"
SUPPORTED_TOOLCHAINS=(
    v4 v5
)
DEFAULT_OS="all"
SUPPORTED_OS=(
    all
    amzn-2
    centos-7 centos-9
    debian-10 debian-11 debian-11-arm debian-12 debian-12-arm
    fedora-36 fedora-38 fedora-39
    rocky-9
    ubuntu-18.04 ubuntu-20.04 ubuntu-22.04 ubuntu-22.04-arm
)
DEFAULT_BUILD_TYPE="Release"
SUPPORTED_BUILD_TYPES=(
    Debug
    Release
    RelWithDebInfo
)
DEFAULT_ARCH="amd"
SUPPORTED_ARCHS=(
    amd
    arm
)
SUPPORTED_TESTS=(
    unit stress-plain stress-ssl drivers integration durability
    gql-behave cppcheck-and-clang-format leftover-CTest
)
DEFAULT_THREADS=$(nproc)
DEFAULT_ENTERPRISE_LICENSE=""
DEFAULT_ORGANIZATION_NAME="memgraph"
# TODO(deda): create toolchain + OS combinations
print_help () {
  echo -e "\nUsage: ./run.sh [OPTIONS] COMMAND"
  echo -e "\nInteract with mgbuild containers"

  echo -e "\nCommands:"
  echo -e "  build                         Build mgbuild image"
  echo -e "  run                           Run mgbuild container"
  echo -e "  stop OPTIONS                  Stop mgbuild container"
  echo -e "  build-memgraph OPTIONS        Build memgraph inside mgbuild container"
  echo -e "  package-memgraph              Build memgraph and create deb package"
  echo -e "  test-memgraph TEST            Run a specific test on memgraph"
  echo -e "  copy                          Copy an artifact from mgbuild container"

  echo -e "\nGlobal options:"
  echo -e "  --arch string                 Specify target architecture (\"${SUPPORTED_ARCHS[*]}\") (default \"$DEFAULT_ARCH\")"
  echo -e "  --enterprise-license string   Specify the enterprise license (default \"\")"
  echo -e "  --build-type string           Specify build type (\"${SUPPORTED_BUILD_TYPES[*]}\") (default \"$DEFAULT_BUILD_TYPE\")"
  echo -e "  --organization-name string    Specify the organization name (default \"memgraph\")"
  echo -e "  --os string                   Specify operating system (\"${SUPPORTED_OS[*]}\") (default \"$DEFAULT_OS\")"
  echo -e "  --threads int                 Specify the number of threads a command will use (default \"\$(nproc)\")"
  echo -e "  --toolchain string            Specify toolchain version (\"${SUPPORTED_TOOLCHAINS[*]}\") (default \"$DEFAULT_TOOLCHAIN\")"

  echo -e "\nSupported tests:"
  echo -e "  \"${SUPPORTED_TESTS[*]}\""

  echo -e "\nbuild-memgraph options:"
  echo -e "  --for-docker                  <ADD INFO>"
  echo -e "  --for-platform                <ADD INFO>"

  echo -e "\nstop options:"
  echo -e "  --remove                      Remove the stopped mgbuild container"

  # echo -e "\ncopy options:"
  # echo -e "  --binary                 Copy built memgraph binary"
  # echo -e "  --build-output           Copy entire build/output folder"
  # echo -e "  --package                Copy packaged memgraph"
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


##################################################
######## BUILD, COPY AND PACKAGE MEMGRAPH ########
##################################################
build_memgraph () {
  build_container="mgbuild_${toolchain_version}_${os}"
  echo "Building Memgraph for $os on $build_container..."

  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"

  telemetry_id_override_flag=""
  if [[ "$#" -gt 3 ]]; then
      if [[ "$4" == "--for-docker" ]]; then
          telemetry_id_override_flag=" -DMG_TELEMETRY_ID_OVERRIDE=DOCKER "
      elif [[ "$4" == "--for-platform" ]]; then
          telemetry_id_override_flag=" -DMG_TELEMETRY_ID_OVERRIDE=DOCKER-PLATFORM"
      else
        echo "Error: Unknown flag '$4'"
        exit 1
      fi
  fi

  echo "Copying project files..."
  # If master is not the current branch, fetch it, because the get_version
  # script depends on it. If we are on master, the fetch command is going to
  # fail so that's why there is the explicit check.
  # Required here because Docker build container can't access remote.
  cd "$ROOT_DIR"
  if [[ "$(git rev-parse --abbrev-ref HEAD)" != "master" ]]; then
      git fetch origin master:master
  fi

  # Ensure we have a clean build directory
  docker exec "$build_container" rm -rf /memgraph
  docker exec "$build_container" mkdir -p /memgraph

  # TODO(gitbuda): Revisit copying the whole repo -> makese sense under CI.
  docker cp "$ROOT_DIR/." "$build_container:/memgraph/"

  container_build_dir="/memgraph/build"
  container_output_dir="$container_build_dir/output"

  # TODO(gitbuda): TOOLCHAIN_RUN_DEPS should be installed during the Docker
  # image build phase, but that is not easy at this point because the
  # environment/os/{os}.sh does not come within the toolchain package. When
  # migrating to the next version of toolchain do that, and remove the
  # TOOLCHAIN_RUN_DEPS installation from here.
  # TODO(gitbuda): On the other side, having this here allows updating deps
  # wihout reruning the build containers.
  # TODO(gitbuda+deda): Make a decision on this, (deda thinks we should move this to the Dockerfiles to save on time)
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
  docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN "'&& make -j$threads'
  docker exec "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN "'&& make -j$threads -B mgconsole'
}

package_memgraph() {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  container_output_dir="/memgraph/build/output"
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
  docker exec "$build_container" bash -c "mkdir -p $container_output_dir && cd $container_output_dir && $ACTIVATE_TOOLCHAIN && $package_command"
}

copy_memgraph() {
  container_output_dir="/memgraph/build/output"
  echo "Copying targeted package to host..."
  last_package_name=$(docker exec "$build_container" bash -c "cd $container_output_dir && ls -t memgraph* | head -1")
  # The operating system folder is introduced because multiple different
  # packages could be preserved during the same build "session".
  mkdir -p "$HOST_OUTPUT_DIR/$os"
  package_host_destination="$HOST_OUTPUT_DIR/$os/$last_package_name"
  docker cp "$build_container:$container_output_dir/$last_package_name" "$package_host_destination"
  echo "Package saved to $package_host_destination."
}


##################################################
##################### TESTS ######################
##################################################
test_memgraph() {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  local EXPORT_LICENSE="export MEMGRAPH_ENTERPRISE_LICENSE=$enterprise_license"
  local EXPORT_ORG_NAME="export MEMGRAPH_ORGANIZATION_NAME=$organization_name"
  local BUILD_DIR="/memgraph/build"
  local ROOT_DIR="/memgraph"
  build_container="mgbuild_${toolchain_version}_${os}"
  echo "Running $1 test on $build_container..."

  case "$1" in
    unit)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN && ctest -R memgraph__unit --output-on-failure -j$threads"
      echo "DEBUG"
    ;;
    leftover-CTest)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN && ctest -E \"(memgraph__unit|memgraph__benchmark)\" --output-on-failure"
    ;;
    drivers)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $ROOT_DIR && ./tests/drivers/run.sh"
    ;;
    integration)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $ROOT_DIR && tests/integration/run.sh"
    ;;
    cppcheck-and-clang-format)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $ROOT_DIR/tools/github && $ACTIVATE_TOOLCHAIN && ./cppcheck_and_clang_format diff"
    ;;
    stress-plain)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $ROOT_DIR/tests/stress && source ve3/bin/activate && ./continuous_integration"
    ;;
    stress-ssl)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $ROOT_DIR/tests/stress && source ve3/bin/activate && ./continuous_integration --use-ssl"
    ;;
    durability)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $ROOT_DIR/tests/stress && source ve3/bin/activate && python3 durability --num-steps 5"
    ;;
    gql-behave)
      docker exec $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $ROOT_DIR/tests/gql_behave && $ACTIVATE_TOOLCHAIN && ./continuous_integration"
    ;;
    *)
      echo "Error: Unknown test '$1'"
      exit 1
    ;;
  esac
}


##################################################
################### PARSE ARGS ###################
##################################################
if [ "$#" -eq 0 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    print_help
    exit 0
fi
arch=$DEFAULT_ARCH
build_type=$DEFAULT_BUILD_TYPE
enterprise_license=$DEFAULT_ENTERPRISE_LICENSE
organization_name=$DEFAULT_ORGANIZATION_NAME
os=$DEFAULT_OS
threads=$DEFAULT_THREADS
toolchain_version=$DEFAULT_TOOLCHAIN
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch)
        arch=$2
        check_support arch $arch
        shift 2
    ;;
    --build-type)
        build_type=$2
        check_support build_type $build_type
        shift 2
    ;;
    --enterprise-license)
        enterprise_license=$2
        shift 2
    ;;
    --organization-name)
        organization_name=$2
        shift 2
    ;;
    --os)
        os=$2
        check_support os $os
        shift 2
    ;;
    --threads)
        threads=$2
        shift 2
    ;;
    --toolchain)
        toolchain=$2
        check_support toolchain $toolchain
        shift 2
    ;;
    *)
      if [[ "$1" =~ ^--.* ]]; then
        echo -e "Error: Unknown option '$1'"
        exit 1
      else
        command=$1
        shift 1
        break
      fi
    ;;
  esac
done

docker_compose_cmd="docker-compose"
if ! which "docker-compose" >/dev/null; then
    docker_compose_cmd="docker compose"
fi

##################################################
################# PARSE COMMAND ##################
##################################################
case $command in
    build)
      cd $SCRIPT_DIR
      if [[ "$os" == "all" ]]; then
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml build
      else
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml build mgbuild_${toolchain_version}_${os}
      fi
    ;;
    run)
      cd $SCRIPT_DIR
      if [[ "$os" == "all" ]]; then
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml up -d
      else
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml up -d mgbuild_${toolchain_version}_${os}
      fi
    ;;
    stop)
      cd $SCRIPT_DIR
      remove=false
      if [[ "$#" -gt 0 ]]; then
        if [[ "$1" == "--remove" ]]; then
          remove=true
        else
          echo "Error: Unknown flag '$1'"
          exit 1
        fi
      fi
      if [[ "$os" == "all" ]]; then
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml down
      else
        docker stop mgbuild_${toolchain_version}_${os}
        if [[ "$remove" == "true" ]]; then
          docker rm mgbuild_${toolchain_version}_${os}
        fi
      fi
    ;;
    build-memgraph)
      build_memgraph $@
    ;;
    package-memgraph)
      package_memgraph
    ;;
    test-memgraph)
      test_memgraph $1
    ;;
    copy)
      copy_memgraph $@
    ;;
    *)
        echo "Error: Unknown command '$1'"
        exit 1
    ;;
esac    
