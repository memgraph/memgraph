#!/bin/bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."
MGBUILD_HOME_DIR="/home/mg"
MGBUILD_ROOT_DIR="$MGBUILD_HOME_DIR/memgraph"

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
    clang-tidy cppcheck-and-clang-format code-analysis
    code-coverage drivers durability e2e gql-behave
    integration leftover-CTest macro-benchmark
    mgbench stress-plain stress-ssl 
    unit unit-coverage upload-to-bench-graph

)
DEFAULT_THREADS=$(nproc)
DEFAULT_ENTERPRISE_LICENSE=""
DEFAULT_ORGANIZATION_NAME="memgraph"
# TODO(deda): 
# * create toolchain + OS combinations
# * add option for selecting server for push and pull
print_help () {
  echo -e "\nUsage: ./run.sh [OPTIONS] COMMAND"
  echo -e "\nInteract with mgbuild containers"

  echo -e "\nCommands:"
  echo -e "  build                         Build mgbuild image"
  echo -e "  run                           Run mgbuild container"
  echo -e "  stop [OPTIONS]                Stop mgbuild container"
  echo -e "  pull                          Pull mgbuild image from dockerhub"
  echo -e "  push OPTIONS                  Push mgbuild image to dockerhub"
  echo -e "  build-memgraph [OPTIONS]      Build memgraph inside mgbuild container"
  echo -e "  package-memgraph              Build memgraph and create deb package"
  echo -e "  test-memgraph TEST            Run a specific test on memgraph"
  echo -e "  copy OPTIONS                  Copy an artifact from mgbuild container"

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
  echo -e "  --community                   Build community version"
  echo -e "  --init-only                   Only run init script"
  echo -e "  --for-docker                  Add flag -DMG_TELEMETRY_ID_OVERRIDE=DOCKER to cmake"
  echo -e "  --for-platform                Add flag -DMG_TELEMETRY_ID_OVERRIDE=DOCKER-PLATFORM to cmake"

  echo -e "\nstop options:"
  echo -e "  --remove                      Remove the stopped mgbuild container"

  echo -e "\ncopy options:"
  echo -e "  --binary                      Remove the stopped mgbuild container"
  echo -e "  --package                     Remove the stopped mgbuild container"

  echo -e "\npush options:"
  echo -e "  -p, --password string         Specify password for docker login"
  echo -e "  -u, --username string         Specify username for docker login"

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
  local build_container="mgbuild_${toolchain_version}_${os}"
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  local container_build_dir="$MGBUILD_ROOT_DIR/build"
  local container_output_dir="$container_build_dir/output"
  local arm_flag=""
  if [[ "$arch" == "arm" ]] || [[ "$os" =~ "-arm" ]]; then
    arm_flag="-DMG_ARCH="ARM64""
  fi
  local build_type_flag="-DCMAKE_BUILD_TYPE=$build_type"
  local telemetry_id_override_flag=""
  local community_flag=""
  local coverage_flag=""
  local asan_flag=""
  local ubsan_flag=""
  local init_only=false
  local for_docker=false
  local for_platform=false
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --community)
        community_flag="-DMG_ENTERPRISE=OFF"
        shift 1
      ;;
      --init-only)
        init_only=true
        shift 1
      ;;
      --for-docker)
        for_docker=true
        if [[ "$for_platform" == "true" ]]; then
          echo "Error: Cannot combine --for-docker and --for-platform flags"
          exit 1
        fi
        telemetry_id_override_flag=" -DMG_TELEMETRY_ID_OVERRIDE=DOCKER "
        shift 1
      ;;
      --for-platform)
        for_platform=true
        if [[ "$for_docker" == "true" ]]; then
          echo "Error: Cannot combine --for-docker and --for-platform flags"
          exit 1
        fi
        telemetry_id_override_flag=" -DMG_TELEMETRY_ID_OVERRIDE=DOCKER-PLATFORM "
        shift 1
      ;;
      --coverage)
        coverage_flag="-DTEST_COVERAGE=ON"
        shift 1
      ;;
      --asan)
        asan_flag="-DASAN=ON"
        shift 1
      ;;
      --ubsan)
        ubsan_flag="-DUBSAN=ON"
        shift 1
      ;;
      *)
        echo "Error: Unknown flag '$1'"
        exit 1
      ;;
    esac
  done

  echo "Building Memgraph for $os on $build_container..."
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
  docker exec -u mg "$build_container" bash -c "rm -rf $MGBUILD_ROOT_DIR && mkdir -p $MGBUILD_ROOT_DIR"

  # TODO(gitbuda): Revisit copying the whole repo -> makese sense under CI.
  docker cp "$PROJECT_ROOT/." "$build_container:$MGBUILD_ROOT_DIR/"
  # Change ownership of copied files so the mg user inside container can access them
  docker exec -u root $build_container bash -c "chown -R mg:mg $MGBUILD_ROOT_DIR" 

  # TODO(gitbuda): TOOLCHAIN_RUN_DEPS should be installed during the Docker
  # image build phase, but that is not easy at this point because the
  # environment/os/{os}.sh does not come within the toolchain package. When
  # migrating to the next version of toolchain do that, and remove the
  # TOOLCHAIN_RUN_DEPS installation from here.
  # TODO(gitbuda): On the other side, having this here allows updating deps
  # wihout reruning the build containers.
  # TODO(gitbuda+deda): Make a decision on this, (deda thinks we should move this to the Dockerfiles to save on time)
  echo "Installing dependencies using '/memgraph/environment/os/$os.sh' script..."
  docker exec -u root "$build_container" bash -c "$MGBUILD_ROOT_DIR/environment/os/$os.sh check TOOLCHAIN_RUN_DEPS || /environment/os/$os.sh install TOOLCHAIN_RUN_DEPS"
  docker exec -u root "$build_container" bash -c "$MGBUILD_ROOT_DIR/environment/os/$os.sh check MEMGRAPH_BUILD_DEPS || /environment/os/$os.sh install MEMGRAPH_BUILD_DEPS"

  echo "Building targeted package..."
  # Fix issue with git marking directory as not safe
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && git config --global --add safe.directory '*'"
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && $ACTIVATE_TOOLCHAIN && ./init --ci"
  if [[ "$init_only" == "true" ]]; then
    return
  fi

  docker exec -u mg "$build_container" bash -c "cd $container_build_dir && rm -rf ./*"
  # Fix cmake failing locally if remote is clone via ssh
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && git remote set-url origin https://github.com/memgraph/memgraph.git"

  # Define cmake command
  local cmake_cmd="cmake $build_type_flag $arm_flag $community_flag $telemetry_id_override_flag $coverage_flag $asan_flag $ubsan_flag .."
  docker exec -u mg "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN && $cmake_cmd"
  
  # ' is used instead of " because we need to run make within the allowed
  # container resources.
  # shellcheck disable=SC2016
  docker exec -u mg "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN "'&& make -j$threads'
  docker exec -u mg "$build_container" bash -c "cd $container_build_dir && $ACTIVATE_TOOLCHAIN "'&& make -j$threads -B mgconsole'
}

package_memgraph() {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  local build_container="mgbuild_${toolchain_version}_${os}"
  local container_output_dir="$MGBUILD_ROOT_DIR/build/output"
  local package_command=""
  if [[ "$os" =~ ^"centos".* ]] || [[ "$os" =~ ^"fedora".* ]] || [[ "$os" =~ ^"amzn".* ]]; then
      docker exec -u root "$build_container" bash -c "yum -y update"
      package_command=" cpack -G RPM --config ../CPackConfig.cmake && rpmlint --file='../../release/rpm/rpmlintrc' memgraph*.rpm "
  fi
  if [[ "$os" =~ ^"debian".* ]]; then
      docker exec -u root "$build_container" bash -c "apt --allow-releaseinfo-change -y update"
      package_command=" cpack -G DEB --config ../CPackConfig.cmake "
  fi
  if [[ "$os" =~ ^"ubuntu".* ]]; then
      docker exec -u root "$build_container" bash -c "apt update"
      package_command=" cpack -G DEB --config ../CPackConfig.cmake "
  fi
  docker exec -u mg "$build_container" bash -c "mkdir -p $container_output_dir && cd $container_output_dir && $ACTIVATE_TOOLCHAIN && $package_command"
}

copy_memgraph() {
  local build_container="mgbuild_${toolchain_version}_${os}"
  case "$1" in
    --binary)
      echo "Copying memgraph binary to host..."
      local container_output_path="$MGBUILD_ROOT_DIR/build/memgraph"
      local host_output_path="$PROJECT_ROOT/build/memgraph"
      mkdir -p "$PROJECT_ROOT/build"
      docker cp -L $build_container:$container_output_path $host_output_path 
      echo "Binary saved to $host_output_path"
    ;;
    --package)
      echo "Copying memgraph package to host..."
      local container_output_dir="$MGBUILD_ROOT_DIR/build/output"
      local host_output_dir="$PROJECT_ROOT/build/output/$os"
      local last_package_name=$(docker exec -u mg "$build_container" bash -c "cd $container_output_dir && ls -t memgraph* | head -1")
      # The operating system folder is introduced because multiple different
      # packages could be preserved during the same build "session".
      mkdir -p "$host_output_dir"
      docker cp "$build_container:$container_output_dir/$last_package_name" "$host_output_dir/$last_package_name"
      echo "Package saved to $host_output_dir/$last_package_name"
    ;;
    *)
      echo "Error: Unknown flag '$1'"
      exit 1
    ;;
  esac
}


##################################################
##################### TESTS ######################
##################################################
test_memgraph() {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  local ACTIVATE_VENV="./setup.sh /opt/toolchain-${toolchain_version}/activate"
  local EXPORT_LICENSE="export MEMGRAPH_ENTERPRISE_LICENSE=$enterprise_license"
  local EXPORT_ORG_NAME="export MEMGRAPH_ORGANIZATION_NAME=$organization_name"
  local BUILD_DIR="$MGBUILD_ROOT_DIR/build"
  local build_container="mgbuild_${toolchain_version}_${os}"
  echo "Running $1 test on $build_container..."

  case "$1" in
    unit)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN "'&& ctest -R memgraph__unit --output-on-failure -j$threads'
    ;;
    unit-coverage)
      local setup_lsan_ubsan="export LSAN_OPTIONS=suppressions=$BUILD_DIR/../tools/lsan.supp && export UBSAN_OPTIONS=halt_on_error=1"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN && $setup_lsan_ubsan "'&& ctest -R memgraph__unit --output-on-failure -j2'
    ;;
    leftover-CTest)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN "'&& ctest -E "(memgraph__unit|memgraph__benchmark)" --output-on-failure'
    ;;
    drivers)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR "'&& ./tests/drivers/run.sh'
    ;;
    integration)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR "'&& tests/integration/run.sh'
    ;;
    cppcheck-and-clang-format)
      local test_output_path="$MGBUILD_ROOT_DIR/tools/github/cppcheck_and_clang_format.txt"
      local test_output_host_dest="$PROJECT_ROOT/tools/github/cppcheck_and_clang_format.txt"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tools/github && $ACTIVATE_TOOLCHAIN "'&& ./cppcheck_and_clang_format diff'
      docker cp $build_container:$test_output_path $test_output_host_dest
    ;;
    stress-plain)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source ve3/bin/activate "'&& ./continuous_integration'
    ;;
    stress-ssl)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source ve3/bin/activate "'&& ./continuous_integration --use-ssl'
    ;;
    durability)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source ve3/bin/activate "'&& python3 durability --num-steps 5'
    ;;
    gql-behave)
      local test_output_dir="$MGBUILD_ROOT_DIR/tests/gql_behave"
      local test_output_host_dest="$PROJECT_ROOT/tests/gql_behave"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests && $ACTIVATE_VENV && cd $MGBUILD_ROOT_DIR/tests/gql_behave "'&& ./continuous_integration'
      docker cp $build_container:$test_output_dir/gql_behave_status.csv $test_output_host_dest/gql_behave_status.csv
      docker cp $build_container:$test_output_dir/gql_behave_status.html $test_output_host_dest/gql_behave_status.html
    ;;
    macro-benchmark)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && export USER=mg && export LANG=$(echo $LANG) && cd $MGBUILD_ROOT_DIR/tests/macro_benchmark "'&& ./harness QuerySuite MemgraphRunner --groups aggregation 1000_create unwind_create dense_expand match --no-strict'
    ;;
    mgbench)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/mgbench "'&& ./benchmark.py vendor-native --num-workers-for-benchmark 12 --export-results benchmark_result.json pokec/medium/*/*'
    ;;
    upload-to-bench-graph)
      shift 1
      local SETUP_PASSED_ARGS="export PASSED_ARGS=\"$@\""
      local SETUP_VE3_ENV="virtualenv -p python3 ve3 && source ve3/bin/activate && pip install -r requirements.txt"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tools/bench-graph-client && $SETUP_VE3_ENV && $SETUP_PASSED_ARGS "'&& ./main.py $PASSED_ARGS'
    ;;
    code-analysis)
      shift 1
      local SETUP_PASSED_ARGS="export PASSED_ARGS=\"$@\""
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/code_analysis && $SETUP_PASSED_ARGS "'&& ./python_code_analysis.sh $PASSED_ARGS'
    ;;
    code-coverage)
      local test_output_path="$MGBUILD_ROOT_DIR/tools/github/generated/code_coverage.tar.gz"
      local test_output_host_dest="$PROJECT_ROOT/tools/github/generated/code_coverage.tar.gz"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && $ACTIVATE_TOOLCHAIN && cd $MGBUILD_ROOT_DIR/tools/github "'&& ./coverage_convert'
      docker exec -u mg $build_container bash -c "cd $MGBUILD_ROOT_DIR/tools/github/generated && tar -czf code_coverage.tar.gz coverage.json html report.json summary.rmu"
      mkdir -p $PROJECT_ROOT/tools/github/generated
      docker cp $build_container:$test_output_path $test_output_host_dest
    ;;
    clang-tidy)
      shift 1
      local SETUP_PASSED_ARGS="export PASSED_ARGS=\"$@\""
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && export THREADS=$threads && $ACTIVATE_TOOLCHAIN && cd $MGBUILD_ROOT_DIR/tests/code_analysis && $SETUP_PASSED_ARGS "'&& ./clang_tidy.sh $PASSED_ARGS'
    ;;
    e2e)
      local kafka_container="kafka_kafka_1"
      local kafka_hostname="kafka"
      local pulsar_container="pulsar_pulsar_1"
      local pulsar_hostname="pulsar"
      local setup_hostnames="export KAFKA_HOSTNAME=$kafka_hostname && PULSAR_HOSTNAME=$pulsar_hostname"
      local build_container_network=$(docker inspect $build_container --format='{{ .HostConfig.NetworkMode }}')
      docker network connect --alias $kafka_hostname $build_container_network $kafka_container  > /dev/null 2>&1 || echo "Kafka container already inside correct network or something went wrong ..."
      docker network connect --alias $pulsar_hostname $build_container_network $pulsar_container  > /dev/null 2>&1 || echo "Kafka container already inside correct network or something went wrong ..."
      docker exec -u mg $build_container bash -c "pip install networkx & pip3 install networkx"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && $setup_hostnames && cd $MGBUILD_ROOT_DIR/tests && $ACTIVATE_VENV && source ve3/bin/activate_e2e && cd $MGBUILD_ROOT_DIR/tests/e2e "'&& ./run.sh'
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
        toolchain_version=$2
        check_support toolchain $toolchain_version
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
    pull)
      docker pull memgraph/mgbuild_${toolchain_version}_${os}
    ;;
    push)
      docker login $@
      docker push memgraph/mgbuild_${toolchain_version}_${os}
    ;;
    build-memgraph)
      build_memgraph $@
    ;;
    package-memgraph)
      package_memgraph
    ;;
    test-memgraph)
      test_memgraph $@
    ;;
    copy)
      copy_memgraph $@
    ;;
    *)
        echo "Error: Unknown command '$command'"
        exit 1
    ;;
esac    
