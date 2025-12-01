#!/bin/bash
set -Eeuo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPT_NAME=${0##*/}
PROJECT_ROOT="$SCRIPT_DIR/../.."
MGBUILD_HOME_DIR="/home/mg"
MGBUILD_ROOT_DIR="$MGBUILD_HOME_DIR/memgraph"

DEFAULT_TOOLCHAIN="v7"
SUPPORTED_TOOLCHAINS=(
    v4 v5 v6 v7
)
DEFAULT_OS="all"

SUPPORTED_OS=(
    all
    centos-9 centos-10
    debian-10 debian-11 debian-11-arm debian-12 debian-12-arm
    fedora-36 fedora-38 fedora-39 fedora-41
    rocky-9.3
    ubuntu-18.04 ubuntu-20.04 ubuntu-22.04 ubuntu-22.04-arm ubuntu-24.04 ubuntu-24.04-arm
)
SUPPORTED_OS_V4=(
    centos-9
    debian-10 debian-11 debian-11-arm
    fedora-36
    ubuntu-18.04 ubuntu-20.04 ubuntu-22.04 ubuntu-22.04-arm
)
SUPPORTED_OS_V5=(
    centos-9
    debian-11 debian-11-arm debian-12 debian-12-arm
    fedora-38 fedora-39
    rocky-9.3
    ubuntu-20.04 ubuntu-22.04 ubuntu-22.04-arm ubuntu-24.04 ubuntu-24.04-arm
)

SUPPORTED_OS_V6=(
    centos-9 centos-10
    debian-11 debian-11-arm debian-12 debian-12-arm
    fedora-41
    ubuntu-22.04 ubuntu-24.04 ubuntu-24.04-arm
)

SUPPORTED_OS_V7=(
    centos-9 centos-10
    debian-12 debian-12-arm debian-13 debian-13-arm
    fedora-42 fedora-42-arm
    rocky-10
    ubuntu-22.04 ubuntu-24.04 ubuntu-24.04-arm
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
    code-coverage drivers drivers-high-availability durability e2e gql-behave
    integration leftover-CTest macro-benchmark
    mgbench stress-plain stress-ssl
    unit unit-coverage upload-to-bench-graph
)
DEFAULT_THREADS=0
DEFAULT_ENTERPRISE_LICENSE=""
DEFAULT_ORGANIZATION_NAME="memgraph"
DEFAULT_BENCH_GRAPH_HOST="bench-graph-api"
DEFAULT_BENCH_GRAPH_PORT="9001"
DEFAULT_MGDEPS_CACHE_HOST="mgdeps-cache"
DEFAULT_MGDEPS_CACHE_PORT="8000"
DEFAULT_CCACHE_ENABLED="true"
DEFAULT_CONAN_CACHE_ENABLED="true"
DISABLE_NODE=false  # use this to disable tests which use node.js when there's a hack

print_help () {
  echo -e "\nUsage:  $SCRIPT_NAME [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]"
  echo -e "\nInteract with mgbuild containers"

  echo -e "\nCommands:"
  echo -e "  build [OPTIONS]               Build mgbuild image"
  echo -e "  build-memgraph [OPTIONS]      Build memgraph binary inside mgbuild container"
  echo -e "  init-tests                    Initialize tests inside mgbuild container"
  echo -e "  copy [OPTIONS]                Copy an artifact from mgbuild container to host"
  echo -e "  package-memgraph              Create memgraph package from built binary inside mgbuild container"
  echo -e "  package-docker [OPTIONS]      Create memgraph docker image and pack it as .tar.gz"
  echo -e "  pull                          Pull mgbuild image from dockerhub"
  echo -e "  push [OPTIONS]                Push mgbuild image to dockerhub"
  echo -e "  run [OPTIONS]                 Run mgbuild container"
  echo -e "  stop [OPTIONS]                Stop mgbuild container"
  echo -e "  test-memgraph TEST            Run a selected test TEST (see supported tests below) inside mgbuild container"

  echo -e "\nSupported tests:"
  echo -e "  \"${SUPPORTED_TESTS[*]}\""

  echo -e "\nGlobal options:"
  echo -e "  --arch string                 Specify target architecture (\"${SUPPORTED_ARCHS[*]}\") (default \"$DEFAULT_ARCH\")"
  echo -e "  --bench-graph-host string     Specify ip address for bench graph server endpoint (default \"$DEFAULT_BENCH_GRAPH_HOST\")"
  echo -e "  --bench-graph-port string     Specify port for bench graph server endpoint (default \"$DEFAULT_BENCH_GRAPH_PORT\")"
  echo -e "  --build-type string           Specify build type (\"${SUPPORTED_BUILD_TYPES[*]}\") (default \"$DEFAULT_BUILD_TYPE\")"
  echo -e "  --enterprise-license string   Specify the enterprise license (default \"\")"
  echo -e "  --mgdeps-cache-host string    Specify ip address for mgdeps cache server endpoint (default \"$DEFAULT_MGDEPS_CACHE_HOST\")"
  echo -e "  --mgdeps-cache-port string    Specify port for mgdeps cache server endpoint (default \"$DEFAULT_MGDEPS_CACHE_PORT\")"
  echo -e "  --organization-name string    Specify the organization name (default \"memgraph\")"
  echo -e "  --os string                   Specify operating system (\"${SUPPORTED_OS[*]}\") (default \"$DEFAULT_OS\")"
  echo -e "  --threads int                 Specify the number of threads a command will use (default \"\$(nproc)\" for container)"
  echo -e "  --toolchain string            Specify toolchain version (\"${SUPPORTED_TOOLCHAINS[*]}\") (default \"$DEFAULT_TOOLCHAIN\")"
  echo -e "  --no-ccache                   Disable ccache volume mounting (default \"$DEFAULT_CCACHE_ENABLED\") -> this is required for run, stop and build-memgraph commands on the coverage build"
  echo -e "  --no-conan-cache              Disable conan cache volume mounting (default \"$DEFAULT_CONAN_CACHE_ENABLED\") -> this allows sharing conan cache between containers"

  echo -e "\nbuild options:"
  echo -e "  --git-ref string              Specify git ref from which the environment deps will be installed (default \"master\")"
  echo -e "  --rust-version number         Specify rustc and cargo version which be installed (default \"1.80\")"
  echo -e "  --node-version number         Specify nodejs version which be installed (default \"20\")"

  echo -e "\nbuild-memgraph options:"
  echo -e "  --asan                        Build with ASAN"
  echo -e "  --cmake-only                  Only run cmake configure command"
  echo -e "  --community                   Build community version"
  echo -e "  --coverage                    Build with code coverage"
  echo -e "  --for-docker                  Add flag -DMG_TELEMETRY_ID_OVERRIDE=DOCKER to cmake"
  echo -e "  --init-only                   Only run init script"
  echo -e "  --no-copy                     Don't copy the memgraph repo from host."
  echo -e "                                Use this option with caution, be sure that memgraph source code is in correct location inside mgbuild container"
  echo -e "  --ubsan                       Build with UBSAN"
  echo -e "  --disable-jemalloc            Build without jemalloc"
  echo -e "  --disable-testing             Build without tests (faster build for packaging)"
  echo -e "  --conan-remote string         Specify conan remote (default \"\")"
  echo -e "  --conan-username string       Specify conan username (default \"\")"
  echo -e "  --conan-password string       Specify conan password (default \"\")"
  echo -e "  --build-dependency string     Specify build dependency (default \"\"). Set to \"all\" to install all dependencies, or a specific dependency name to install only that dependency. Dependencies are specified in the format of \"<package>/<version>\"."

  echo -e "\ncopy options (default \"--binary\"):"
  echo -e "  --artifact-name string        Specify a custom name for the copied artifact"
  echo -e "  --binary                      Copy memgraph binary from mgbuild container to host (default)"
  echo -e "  --build-logs                  Copy build logs from mgbuild container to host"
  echo -e "  --dest-dir string             Specify a custom path for destination directory on host"
  echo -e "  --package                     Copy memgraph package from mgbuild container to host"
  echo -e "  --use-make-install            Use 'ninja install' with DESTDIR instead of copying individual files"

  echo -e "\npackage-docker options:"
  echo -e "  --dest-dir string             Specify a custom path for destination directory on host. Provide relative path inside memgraph directory."
  echo -e "  --src-dir string              Specify a custom path for the source directory on host. Provide relative path inside memgraph directory."
  echo -e "                                This directory should contain the memgraph package."

  echo -e "\npush options:"
  echo -e "  -p, --password string         Specify password for docker login (default empty)"
  echo -e "  -u, --username string         Specify username for docker login (default empty)"

  echo -e "\nrun options:"
  echo -e "  --pull                        Pull the mgbuild image before running"

  echo -e "\nstop options:"
  echo -e "  --remove                      Remove the stopped mgbuild container"

  echo -e "\nmgbench options:"
  echo -e "  --dataset string              Specify dataset to benchmark (default \"pokec\")"
  echo -e "  --size string                 Specify dataset size: (for pokec: small, medium, large) (default \"medium\")"
  echo -e "  --export-results-file string  Specify output file for benchmark results (default \"benchmark_result.json\")"

  echo -e "\nToolchain v4 supported OSs:"
  echo -e "  \"${SUPPORTED_OS_V4[*]}\""

  echo -e "\nToolchain v5 supported OSs:"
  echo -e "  \"${SUPPORTED_OS_V5[*]}\""

  echo -e "\nToolchain v6 supported OSs:"
  echo -e "  \"${SUPPORTED_OS_V6[*]}\""

    echo -e "\nToolchain v7 supported OSs:"
  echo -e "  \"${SUPPORTED_OS_V7[*]}\""

  echo -e "\nExample usage:"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd build --git-ref my-special-branch"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd run"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd --build-type RelWithDebInfo build-memgraph --community"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd --build-type RelWithDebInfo build-memgraph --disable-testing"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd --build-type RelWithDebInfo test-memgraph unit"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd test-memgraph mgbench --dataset pokec --size large"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd test-memgraph mgbench --dataset ldbc_bi --size medium --export-results-file my_results.json"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd package"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd copy --package"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd copy --use-make-install --dest-dir build/install"
  echo -e "  $SCRIPT_NAME --os debian-12 --toolchain v7 --arch amd stop --remove"
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
        echo -e "Error: Architecture $2 isn't supported!\nChoose from  ${SUPPORTED_ARCHS[*]}"
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
        echo -e "Error: Build type $2 isn't supported!\nChoose from  ${SUPPORTED_BUILD_TYPES[*]}"
        exit 1
      fi
    ;;
    os)
      for e in "${SUPPORTED_OS_V7[@]}"; do
        if [[ "$e" == "$2" ]]; then
          is_supported=true
          break
        fi
      done
      if [[ "$is_supported" == false ]]; then
        echo -e "Error: OS $2 isn't supported!\nChoose from  ${SUPPORTED_OS_V7[*]}"
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
        echo -e "Error: Toolchain version $2 isn't supported!\nChoose from  ${SUPPORTED_TOOLCHAINS[*]}"
        exit 1
      fi
    ;;
    os_toolchain_combo)
      if [[ "$3" == "v4" ]]; then
        local SUPPORTED_OS_TOOLCHAIN=("${SUPPORTED_OS_V4[@]}")
      elif [[ "$3" == "v5" ]]; then
        local SUPPORTED_OS_TOOLCHAIN=("${SUPPORTED_OS_V5[@]}")
      elif [[ "$3" == "v6" ]]; then
        local SUPPORTED_OS_TOOLCHAIN=("${SUPPORTED_OS_V6[@]}")
      elif [[ "$3" == "v7" ]]; then
        local SUPPORTED_OS_TOOLCHAIN=("${SUPPORTED_OS_V7[@]}")
      else
        echo -e "Error: $3 isn't a supported toolchain_version!\nChoose from ${SUPPORTED_TOOLCHAINS[*]}"
        exit 1
      fi
      for e in "${SUPPORTED_OS_TOOLCHAIN[@]}"; do
        if [[ "$e" == "$2" ]]; then
          is_supported=true
          break
        fi
      done
      if [[ "$is_supported" == false ]]; then
        echo -e "Error: Toolchain version $3 doesn't support OS $2!\nChoose from ${SUPPORTED_OS_TOOLCHAIN[*]}"
        exit 1
      fi
    ;;
    pokec_size)
      if [[ "$2" == "small" || "$2" == "medium" || "$2" == "large" ]]; then
        is_supported=true
      fi
      if [[ "$is_supported" == false ]]; then
        echo -e "Error: Pokec size $2 isn't supported!\nChoose from small, medium, large"
        exit 1
      fi
    ;;
    *)
      echo -e "Error: This function can only check arch, build_type, os, toolchain version and os toolchain combination"
      exit 1
    ;;
  esac
}

# Returns 0 (true) if $1 <= $2
version_lte() {
  # sort -V sorts them in ascending order, so the first in the sorted list is the smaller.
  # If $1 equals the first in the list, $1 <= $2
  [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]
}
# Returns 0 (true) if $1 < $2
version_lt() {
  [ "$1" = "$2" ] && return 1
  version_lte "$1" "$2"
}

##################################################
######## BUILD, COPY AND PACKAGE MEMGRAPH ########
##################################################

# Function to handle cache override file creation and cleanup
setup_cache_override() {
  local compose_files="-f ${arch}-builders-${toolchain_version}.yml"

  if [[ "$ccache_enabled" == "true" ]] || [[ "$conan_cache_enabled" == "true" ]]; then
    cat > cache-override.yml << EOF
services:
EOF
    # Add cache volumes for all services in the compose file
    if [[ "$os" == "all" ]]; then
      # For all OS, we need to add volumes to all services
      grep "^  mgbuild_" ${arch}-builders-${toolchain_version}.yml | while read -r line; do
        service_name=$(echo "$line" | sed 's/://')
        echo "  $service_name:" >> cache-override.yml
        echo "    volumes:" >> cache-override.yml
        if [[ "$ccache_enabled" == "true" ]]; then
          echo "      - ~/.cache/ccache:/home/mg/.cache/ccache" >> cache-override.yml
        fi
        if [[ "$conan_cache_enabled" == "true" ]]; then
          echo "      - $conan_cache_dir:/home/mg/.conan2" >> cache-override.yml
        fi
      done
    else
      # For specific OS, only add volume to the target service
      echo "  $build_container:" >> cache-override.yml
      echo "    volumes:" >> cache-override.yml
      if [[ "$ccache_enabled" == "true" ]]; then
        echo "      - ~/.cache/ccache:/home/mg/.cache/ccache" >> cache-override.yml
      fi
      if [[ "$conan_cache_enabled" == "true" ]]; then
        echo "      - $conan_cache_dir:/home/mg/.conan2" >> cache-override.yml
      fi
    fi
    compose_files="$compose_files -f cache-override.yml"
  fi

  echo "$compose_files"
}

cleanup_cache_override() {
  if [[ "$ccache_enabled" == "true" ]] || [[ "$conan_cache_enabled" == "true" ]]; then
    rm -f cache-override.yml
  fi
}

setup_host_cache_permissions() {
  # Set up ccache permissions if enabled
  if [[ "$ccache_enabled" == "true" ]]; then
    echo "Setting up host ccache directory permissions..."
    mkdir -p ~/.cache/ccache

    # Set open permissions on the parent .cache directory to allow other tools to create subdirectories
    # Suppress both errors and warnings about operations not permitted
    chmod -R a+rwX ~/.cache 2>/dev/null || true

    echo "Host ccache directory permissions set to a+rwX (open access)"
  fi

  if [[ "$conan_cache_enabled" == "true" ]]; then
    echo "Setting up host conan cache directory permissions..."
    mkdir -pv $conan_cache_dir

    # Set open permissions on the conan cache directory to allow cross-container access
    # Suppress both errors and warnings about operations not permitted
    chmod -R a+rwX $conan_cache_dir 2>/dev/null || true

    echo "Host conan cache directory permissions set to a+rwX (open access)"
  fi
}

copy_project_files() {
  echo "Copying project files..."
  project_files=$(ls -A1 "$PROJECT_ROOT")
  while IFS= read -r f; do
    # Skip build directory when copying project files
    if [[ "$f" != "build" ]]; then
      docker cp "$PROJECT_ROOT/$f" "$build_container:$MGBUILD_ROOT_DIR/"
    fi
  done <<< "$project_files"
  # Change ownership of copied files so the mg user inside container can access them
  docker exec -u root $build_container bash -c "chown -R mg:mg $MGBUILD_ROOT_DIR"
}


upload_conan_cache() {
  local conan_username=$1
  local conan_password=$2
  if [[ -z "$conan_username" ]] || [[ -z "$conan_password" ]]; then
    echo "Warning: Conan username and password are required for Conan cache upload"
    return 0
  fi
  docker exec -u mg $build_container bash -c "cd $MGBUILD_ROOT_DIR && source env/bin/activate && conan remote login -p $conan_password artifactory $conan_username"
  docker exec -u mg $build_container bash -c "cd $MGBUILD_ROOT_DIR && source env/bin/activate && conan upload \"*/*\" -r=artifactory --confirm"
  return $?
}


build_memgraph () {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  local ACTIVATE_CARGO="source $MGBUILD_HOME_DIR/.cargo/env"
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
  local disable_jemalloc_flag=""
  local disable_testing_flag=""
  local init_only=false
  local cmake_only=false
  local for_docker=false
  local copy_from_host=true
  local init_flags="--ci"
  local conan_remote=""
  local conan_username=""
  local conan_password=""
  local build_dependency=""
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
      --cmake-only)
        cmake_only=true
        shift 1
      ;;
      --for-docker)
        for_docker=true
        telemetry_id_override_flag=" -DMG_TELEMETRY_ID_OVERRIDE=DOCKER "
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
      --no-copy)
        copy_from_host=false
        shift 1
      ;;
      --disable-jemalloc)
        disable_jemalloc_flag="-DENABLE_JEMALLOC=OFF"
        shift 1
      ;;
      --disable-testing)
        disable_testing_flag="-DMG_ENABLE_TESTING=OFF"
        shift 1
      ;;
      --conan-remote)
        conan_remote=$2
        shift 2
      ;;
      --conan-username)
        conan_username=$2
        shift 2
      ;;
      --conan-password)
        conan_password=$2
        shift 2
      ;;
      --build-dependency)
        build_dependency=$2
        shift 2
      ;;
      *)
        echo "Error: Unknown flag '$1'"
        print_help
        exit 1
      ;;
    esac
  done

  echo "Initializing deps ..."
  # If master is not the current branch, fetch it, because the get_version
  # script depends on it. If we are on master, the fetch command is going to
  # fail so that's why there is the explicit check.
  # Required here because Docker build container can't access remote.
  cd "$PROJECT_ROOT"
  if [[ "$(git rev-parse --abbrev-ref HEAD)" != "master" ]]; then
      git fetch origin master:master
  fi

  if [[ "$copy_from_host" == "true" ]]; then
    # Ensure we have a clean build directory
    docker exec -u root "$build_container" bash -c "rm -rf $MGBUILD_ROOT_DIR"
    docker exec -u mg "$build_container" bash -c "mkdir -p $MGBUILD_ROOT_DIR"
    copy_project_files
  fi

  # Ubuntu and Debian builds fail because of missing xmlsec since the Python package xmlsec==1.3.15
  if [[ "$os" == debian* || "$os" == ubuntu* ]]; then
    if [[ "$os" == debian-11* ]]; then
      # this should blacklist that version of xmlsec for debian-11
      docker exec -u root "$build_container" bash -c "echo 'xmlsec!=1.3.15' > /etc/pip_constraints.txt"
      docker exec -u root "$build_container" bash -c "echo '[global]' > /etc/pip.conf && echo 'constraint = /etc/pip_constraints.txt' >> /etc/pip.conf"
    else
      docker exec -u root "$build_container" bash -c "apt update && apt install -y libxmlsec1-dev xmlsec1"
    fi
  fi

  echo "Installing dependencies using '/memgraph/environment/os/$os.sh' script..."
  docker exec -u root "$build_container" bash -c "$MGBUILD_ROOT_DIR/environment/os/$os.sh check TOOLCHAIN_RUN_DEPS || $MGBUILD_ROOT_DIR/environment/os/$os.sh install TOOLCHAIN_RUN_DEPS"
  docker exec -u root "$build_container" bash -c "$MGBUILD_ROOT_DIR/environment/os/$os.sh check MEMGRAPH_BUILD_DEPS || $MGBUILD_ROOT_DIR/environment/os/$os.sh install MEMGRAPH_BUILD_DEPS"

  echo "Building targeted package..."
  local SETUP_MGDEPS_CACHE_ENDPOINT="export MGDEPS_CACHE_HOST_PORT=$mgdeps_cache_host:$mgdeps_cache_port"
  # Fix issue with git marking directory as not safe
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && git config --global --add safe.directory '*'"
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && $SETUP_MGDEPS_CACHE_ENDPOINT && ./init $init_flags"
  if [[ "$init_only" == "true" ]]; then
    return
  fi

  echo "Building Memgraph for $os on $build_container using Conan..."
  # Clean build directory
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && rm -rf build/*"
  # Fix cmake failing locally if remote is clone via ssh
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && git remote set-url origin https://github.com/memgraph/memgraph.git"

  # Zero ccache statistics before build if ccache is enabled
  if [[ "$ccache_enabled" == "true" ]]; then
    echo "Zeroing ccache statistics for this build..."
    docker exec -u mg "$build_container" bash -c "ccache -z"
  fi

  # Clean conan cache before build if conan cache is enabled (optional, can be commented out if not needed)
  if [[ "$conan_cache_enabled" == "true" ]]; then
    echo "Conan cache is enabled - packages will be shared between builds"
    # Uncomment the following lines if you want to clean conan cache before each build
    # echo "Cleaning conan cache for this build..."
    # docker exec -u mg "$build_container" bash -c "conan cache clean"
  fi

  # use this because the commands get far too long!
  CMD_START="cd $MGBUILD_ROOT_DIR"

  # Set up Conan environment
  echo "Setting up Conan environment..."
  docker exec -u mg "$build_container" bash -c "$CMD_START && python3 -m venv env"
  CMD_START="$CMD_START && source ./env/bin/activate"
  docker exec -u mg "$build_container" bash -c "$CMD_START && pip install conan"

  # Check if a conan profile exists and create one if needed
  docker exec -u mg "$build_container" bash -c "$CMD_START && if [ ! -f \"\$HOME/.conan2/profiles/default\" ]; then conan profile detect; fi"

  # Install our config
  docker exec -u mg "$build_container" bash -c "$CMD_START && conan config install ./conan_config"

  # Set Conan remote if specified
  if [[ -n "$conan_remote" ]]; then
    echo "Setting Conan remote to $conan_remote"
    docker exec -u mg "$build_container" bash -c "$CMD_START && conan remote add artifactory $conan_remote --force"
  fi

  # Install our config
  docker exec -u mg "$build_container" bash -c "$CMD_START && conan config install ./conan_config"

  # Install Conan dependencies
  echo "Installing Conan dependencies..."
  local EXPORT_MG_TOOLCHAIN="export MG_TOOLCHAIN_ROOT=/opt/toolchain-${toolchain_version}"
  local EXPORT_BUILD_TYPE="export BUILD_TYPE=$build_type"

  # Determine profile template based on sanitizer flags
  local DASAN_ENABLED=false
  local DUBSAN_ENABLED=false

  # Check if ASAN or UBSAN flags are set
  if [[ "$asan_flag" == "-DASAN=ON" ]]; then
    DASAN_ENABLED=true
  fi
  if [[ "$ubsan_flag" == "-DUBSAN=ON" ]]; then
    DUBSAN_ENABLED=true
  fi

  MG_SANITIZERS=""
  if [[ "$DASAN_ENABLED" == true ]]; then
    MG_SANITIZERS="address"
  fi
  if [[ "$DUBSAN_ENABLED" == true ]]; then
    if [[ -n "$MG_SANITIZERS" ]]; then
      # If we already have address sanitizer, add undefined to the list
      MG_SANITIZERS="address,undefined"
    else
      MG_SANITIZERS="undefined"
    fi
  fi

  if [[ -n "$MG_SANITIZERS" ]]; then
    echo "Sanitizers enabled: $MG_SANITIZERS"
    CMD_START="$CMD_START && export MG_SANITIZERS=$MG_SANITIZERS"
  else
    echo "No sanitizers enabled"
  fi

  CMD_START="$CMD_START && $EXPORT_MG_TOOLCHAIN && $EXPORT_BUILD_TYPE"
  if [[ -n "$build_dependency" ]]; then
    echo "Installing build dependency: $build_dependency"
    if [[ "$build_dependency" == "all" ]]; then
      docker exec -u mg "$build_container" bash -c "$CMD_START && conan install . --build=missing -pr:h memgraph_template_profile -pr:b memgraph_build_profile -s build_type=$build_type -s:a os=Linux -s:a os.distro=$os"
    else
      docker exec -u mg "$build_container" bash -c "$CMD_START && conan install --requires $build_dependency --lockfile="" --build=missing -pr:h memgraph_template_profile -pr:b memgraph_build_profile -s build_type=$build_type -s:a os=Linux -s:a os.distro=$os"
    fi

    if [[ -n "$conan_remote" ]]; then
      echo "Uploading Conan cache to $conan_remote"
      upload_conan_cache $conan_username $conan_password
    fi

    exit 0
  else
    docker exec -u mg "$build_container" bash -c "$CMD_START && conan install . --build=missing -pr:h memgraph_template_profile -pr:b memgraph_build_profile -s build_type=$build_type -s:a os=Linux -s:a os.distro=$os"
  fi
  CMD_START="$CMD_START && source build/generators/conanbuild.sh && $ACTIVATE_CARGO"

  # Determine preset name based on build type (Conan generates this automatically)
  local PRESET=""
  if [[ "$build_type" == "Release" ]]; then
    PRESET="conan-release"
  elif [[ "$build_type" == "RelWithDebInfo" ]]; then
    PRESET="conan-relwithdebinfo"
  elif [[ "$build_type" == "Debug" ]]; then
    PRESET="conan-debug"
  else
    echo "Error: Unsupported build type: $build_type"
    exit 1
  fi

  # Configure with CMake using Conan preset and additional options
  echo "Configuring CMake with Conan preset: $PRESET"

  # Add additional CMake options if any are specified
  local additional_options=""
  local flags=("$arm_flag" "$community_flag" "$telemetry_id_override_flag" "$coverage_flag" "$asan_flag" "$ubsan_flag" "$disable_jemalloc_flag" "$disable_testing_flag")

  for flag in "${flags[@]}"; do
    if [[ -n "$flag" ]]; then
      additional_options="$additional_options $flag"
    fi
  done

  if [[ -n "$additional_options" ]]; then
    echo "Adding additional CMake options: $additional_options"
  fi

  echo "Running CMake with preset: $PRESET $additional_options"
  docker exec -u mg "$build_container" bash -c "$CMD_START && cmake --preset $PRESET $additional_options"

  if [[ "$cmake_only" == "true" ]]; then
    build_target(){
      target=$1
      docker exec -u mg "$build_container" bash -c "$CMD_START && cmake --build --preset $PRESET --target $target -- -j"'$(nproc)'
    }
    # Force build that generate the header files needed by analysis (ie. clang-tidy)
    build_target generated_code
    return
  fi

  # Build using Conan preset
  echo "Building with Conan preset: $PRESET"
  if [[ "$threads" == "$DEFAULT_THREADS" ]]; then
    docker exec -u mg "$build_container" bash -c "$CMD_START && cmake --build --preset $PRESET -- -j"'$(nproc)'
  else
    local EXPORT_THREADS="export THREADS=$threads"
    docker exec -u mg "$build_container" bash -c "$CMD_START && $EXPORT_THREADS && cmake --build --preset $PRESET -- -j\$THREADS"
  fi

  # upload conan cache if remote is set
  if [[ -n "$conan_remote" ]]; then
    echo "Uploading Conan cache to $conan_remote"
    upload_conan_cache $conan_username $conan_password
  fi

  # Show ccache statistics if ccache is enabled
  if [[ "$ccache_enabled" == "true" ]]; then
    echo ""
    echo "=== Ccache Statistics ==="
    docker exec -u mg "$build_container" bash -c "ccache -s"
    echo "========================="
    echo ""
  fi

  # Clean up virtual environment
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && source ./env/bin/activate && deactivate"
}

init_tests() {
  echo "Initializing tests..."
  # we need to add the ~/.local/bin to the path
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && export PATH=\$PATH:\$HOME/.local/bin && export DISABLE_NODE=$DISABLE_NODE && ./init-test --ci"
  echo "...Done"
}

init_tests() {
  echo "Initializing tests..."
  docker exec -u root "$build_container" bash -c "apt update && apt install -y python3-venv"
  docker exec -u mg "$build_container" bash -c "cd $MGBUILD_ROOT_DIR && ./init-test --ci"
  echo "...Done"
}

package_memgraph() {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  local container_output_dir="$MGBUILD_ROOT_DIR/build/output"
  local package_command=""

  if [[ "$os" == "centos-10" ]]; then
      # install much newer rpmlint than what ships with centos-10
      docker exec -u root "$build_container" bash -c "dnf remove -y rpmlint --noautoremove"
      docker exec -u root "$build_container" bash -c "pip install rpmlint==2.8.0 --user"
      package_command=" cpack -G RPM --config ../CPackConfig.cmake"
  elif [[ "$os" =~ ^"fedora".* ]]; then
      package_command=" cpack -G RPM --config ../CPackConfig.cmake && rpmlint --file='../../release/rpm/rpmlintrc_fedora' memgraph*.rpm "
  elif [[ "$os" == "rocky-10" ]]; then
      package_command=" cpack -G RPM --config ../CPackConfig.cmake && rpmlint --file='../../release/rpm/rpmlintrc_rocky' memgraph*.rpm "
  elif [[ "$os" =~ ^"centos".* ]] || [[ "$os" =~ ^"amzn".* ]] || [[ "$os" =~ ^"rocky".* ]]; then
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
  docker exec -u root "$build_container" bash -c "mkdir -p $container_output_dir && cd $container_output_dir && $ACTIVATE_TOOLCHAIN && $package_command"
  if [[ "$os" == "centos-10" ]]; then
    docker exec -u root "$build_container" bash -c "cd $container_output_dir && /root/.local/bin/rpmlint --file='../../release/rpm/rpmlintrc_centos10' memgraph*.rpm || echo 'Warning: rpmlint failed, but package was created successfully'"
  fi
}

package_docker() {
  # TODO(gitbuda): Write the below ifs in a better way (make it automatic with new toolchain versions).
  if [[ "$toolchain_version" == "v4" ]]; then
    if [[ "$os" != "debian-11" && "$os" != "debian-11-arm" ]]; then
      echo -e "Error: When passing '--toolchain v4' the 'docker' command accepts only '--os debian-11' and '--os debian-11-arm'"
      exit 1
    fi
  elif [[ "$toolchain_version" == "v5" ]]; then
    if [[ "$os" != "debian-12" && "$os" != "debian-12-arm" ]]; then
      echo -e "Error: When passing '--toolchain v5' the 'docker' command accepts only '--os debian-12' and '--os debian-12-arm'"
      exit 1
    fi
  else
    if [[ "$os" != "ubuntu-24.04" && "$os" != "ubuntu-24.04-arm" ]]; then
      echo -e "Error: When passing '--toolchain v6' the 'docker' command accepts only '--os ubuntu-24.04' and '--os ubuntu-24.04-arm'"
      exit 1
    fi
  fi
  local package_dir="$PROJECT_ROOT/build/output/$os"
  local docker_host_folder="$PROJECT_ROOT/build/output/docker/${arch}/${toolchain_version}"
  local generate_sbom=false
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --dest-dir)
        docker_host_folder="$PROJECT_ROOT/$2"
        shift 2
      ;;
      --src-dir)
        package_dir="$PROJECT_ROOT/$2"
        shift 2
      ;;
      --generate-sbom)
        generate_sbom=$2
        shift 2
      ;;
      *)
        echo "Error: Unknown flag '$1'"
        print_help
        exit 1
      ;;
    esac
  done
  # shellcheck disable=SC2012
  local last_package_name=$(cd $package_dir && ls -t memgraph* | head -1)
  local docker_build_folder="$PROJECT_ROOT/release/docker"
  cd "$docker_build_folder"
  if [[ "$os" == "ubuntu-24.04" && "$arch" == "amd" ]]; then
    echo "Finding best mirror"
    mirror="$(${PROJECT_ROOT}/tools/test-mirrors.sh)"
  else
    echo "Using default mirror"
    mirror=""
  fi
  echo "Using mirror: $mirror"

  if [[ "$build_type" == "Release" ]]; then
    echo "Package release"
    ./package_docker --latest --package-path "$package_dir/$last_package_name" --toolchain $toolchain_version --arch "${arch}64" --custom-mirror "$mirror" --generate-sbom $generate_sbom
  else
    echo "Package other"
    ./package_docker --package-path "$package_dir/$last_package_name" --toolchain $toolchain_version --arch "${arch}64" --src-path "$PROJECT_ROOT/src" --custom-mirror "$mirror" --generate-sbom $generate_sbom
  fi
  # shellcheck disable=SC2012
  local docker_image_name=$(cd "$docker_build_folder" && ls -t memgraph* | head -1)
  local docker_host_image_path="$docker_host_folder/$docker_image_name"
  mkdir -p "$docker_host_folder"
  cp "$docker_build_folder/$docker_image_name" "$docker_host_folder"
  echo "Docker images saved to $docker_host_image_path."
}

copy_memgraph() {
  local MGBUILD_BUILD_DIR="$MGBUILD_ROOT_DIR/build"
  local PROJECT_BUILD_DIR="$PROJECT_ROOT/build"
  local artifact="binary"
  local artifact_name="memgraph"
  local container_artifact_path="$MGBUILD_BUILD_DIR/$artifact_name"
  local host_dir="$PROJECT_BUILD_DIR"
  local host_dir_override=""
  local artifact_name_override=""
  local use_cmake_install=false

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --binary)
        if [[ "$artifact" == "build logs" ]] || [[ "$artifact" == "package" ]] || [[ "$artifact" == "libs" ]]; then
          echo -e "Error: When executing 'copy' command, choose only one of --binary, --build-logs, --libs, --package or --memgraph-logs"
          exit 1
        fi
        artifact="binary"
        artifact_name="memgraph"
        container_artifact_path="$MGBUILD_BUILD_DIR/$artifact_name"
        host_dir="$PROJECT_BUILD_DIR"
        shift 1
      ;;
      --build-logs)
        if [[ "$artifact" == "package" ]] || [[ "$artifact" == "libs" ]]; then
          echo -e "Error: When executing 'copy' command, choose only one of --binary, --build-logs, --libs, --package or --memgraph-logs"
          exit 1
        fi
        artifact="build logs"
        artifact_name="logs"
        container_artifact_path="$MGBUILD_BUILD_DIR/e2e/logs"
        host_dir="$PROJECT_BUILD_DIR"
        shift 1
      ;;
      --memgraph-logs)
        if [[ "$artifact" == "package" ]] || [[ "$artifact" == "libs" ]]; then
          echo -e "Error: When executing 'copy' command, choose only one of --binary, --build-logs, --libs, --package or --memgraph-logs"
          exit 1
        fi
        artifact="memgraph logs"
        artifact_name="memgraph-logs"
        container_artifact_path="$MGBUILD_BUILD_DIR/memgraph-logs"
        host_dir="$PROJECT_BUILD_DIR"
        shift 1
      ;;
      --package)
        if [[ "$artifact" == "build logs" ]] || [[ "$artifact" == "libs" ]]; then
          echo -e "Error: When executing 'copy' command, choose only one of --binary, --build-logs, --libs, --package or --memgraph-logs"
          exit 1
        fi
        artifact="package"
        local container_package_dir="$MGBUILD_BUILD_DIR/output"
        host_dir="$PROJECT_BUILD_DIR/output/$os"
        artifact_name=$(docker exec -u mg "$build_container" bash -c "cd $container_package_dir && ls -t memgraph* | head -1")
        container_artifact_path="$container_package_dir/$artifact_name"
        shift 1
      ;;
      --libs)
        if [[ "$artifact" == "build logs" ]] || [[ "$artifact" == "package" ]]; then
          echo -e "Error: When executing 'copy' command, choose only one of --binary, --build-logs, --libs, --package or --memgraph-logs"
          exit 1
        fi
        artifact="libs"
        artifact_name="libmemgraph_module_support.so"
        container_artifact_path="$MGBUILD_BUILD_DIR/src/query/$artifact_name"
        host_dir="$PROJECT_BUILD_DIR/src/query"
        shift 1
      ;;
      --logs-dir)
        container_artifact_path=$2
        artifact="logs"
        shift 2
      ;;
      --dest-dir)
        host_dir_override=$2
        shift 2
      ;;
      --artifact-name)
        artifact_name_override=$2
        shift 2
      ;;
      --use-make-install)
        if [[ "$artifact" != "binary" ]]; then
          echo -e "Error: Only the --binary artifact can be installed using cmake install"
          exit 1
        fi
        use_cmake_install=true
        shift 1
      ;;
      --sbom)
        artifact="sbom"
        artifact_name="memgraph-sbom.cdx.json"
        container_artifact_path="$MGBUILD_BUILD_DIR/generators/sbom/$artifact_name"
        host_dir="$PROJECT_BUILD_DIR/generators/sbom"
        shift 1
      ;;
      *)
        echo "Error: Unknown flag '$1'"
        print_help
        exit 1
      ;;
    esac
  done

  if [[ "$host_dir_override" != "" ]]; then
    host_dir=$host_dir_override
  fi
  if [[ "$artifact_name_override" != "" ]]; then
    artifact_name=$artifact_name_override
  fi

  # If using cmake install, handle it differently
  if [[ "$use_cmake_install" == "true" ]]; then
    # Initialize variables that conanbuild.sh appends to (required for set -u shells)
    local INIT_CONAN_ENV="export CLASSPATH= LD_LIBRARY_PATH= DYLD_LIBRARY_PATH="
    local ACTIVATE_CONAN_BUILDENV="source $MGBUILD_BUILD_DIR/generators/conanbuild.sh"

    # Create a temporary staging directory in the container
    local staging_dir="/tmp/memgraph-staging"
    docker exec -u mg "$build_container" bash -c "mkdir -p $staging_dir"

    # NOTE: We use DESTDIR instead of --prefix because some install rules use absolute paths
    # which --prefix doesn't redirect. DESTDIR prepends to ALL paths. Absolute path installs:
    #   - /etc/memgraph/memgraph.conf (src/CMakeLists.txt)
    #   - /etc/memgraph/apoc_compatibility_mappings.json (src/CMakeLists.txt)
    #   - /etc/logrotate.d/memgraph (src/CMakeLists.txt)
    #   - /lib/systemd/system (release/CMakeLists.txt)
    #   - /etc/memgraph/auth_module/ldap.example.yaml (src/auth/CMakeLists.txt)
    echo "Installing Memgraph using cmake --install with DESTDIR=$staging_dir..."
    docker exec -u mg "$build_container" bash -c "$INIT_CONAN_ENV && $ACTIVATE_CONAN_BUILDENV && DESTDIR=$staging_dir cmake --install $MGBUILD_BUILD_DIR"

    # Copy the staged installation from container to host
    # DESTDIR prepends to the install prefix (/usr/local), so files are at $staging_dir/usr/local/lib/memgraph/
    echo "Copying installed files from staging directory to $host_dir..."
    mkdir -p "$host_dir"
    docker cp "$build_container:$staging_dir/usr/local/lib/memgraph/." "$host_dir/"

    # Clean up staging directory
    docker exec -u mg "$build_container" bash -c "rm -rf $staging_dir"

    echo "Memgraph installed to $host_dir!"
    return
  fi

  # Original copy logic for individual files
  local host_artifact_path="$host_dir/$artifact_name"
  echo "Host dir: '$host_dir'"
  echo "Artifact name: '$artifact_name'"
  echo "Host artifact path: '$host_artifact_path'"
  echo "Container artifact path: '$container_artifact_path'"
  echo -e "Copying memgraph $artifact from $build_container to host ..."
  mkdir -p "$host_dir"

  if [[ "$artifact" == "logs" ]]; then
    local temp_log_dir="/tmp/mg_logs_$$"
    docker exec -u mg "$build_container" bash -c "mkdir -p $temp_log_dir"
    # Find and copy all .log files to the temporary directory and copy to host
    # Exclude log files that start with "0" (internal database logs like replication and streams)
    docker exec -u mg "$build_container" bash -c "find $container_artifact_path -name '*.log' ! -name '0*' -exec cp {} $temp_log_dir/ \;"
    docker cp "$build_container:$temp_log_dir/." "$host_dir/"
    docker exec -u mg "$build_container" bash -c "rm -rf $temp_log_dir"
    echo -e "Log files copied to $host_dir!"
  elif [[ "$artifact" == "package" ]]; then
    docker cp $build_container:$container_artifact_path $host_artifact_path
  else
    docker cp -L $build_container:$container_artifact_path $host_artifact_path
  fi
  echo -e "Memgraph $artifact saved to $host_artifact_path!"
}


##################################################
##################### TESTS ######################
##################################################
test_memgraph() {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  local ACTIVATE_VENV="source ve3/bin/activate"
  local ACTIVATE_CARGO="source $MGBUILD_HOME_DIR/.cargo/env"
  local EXPORT_LICENSE="export MEMGRAPH_ENTERPRISE_LICENSE=$enterprise_license"
  local EXPORT_ORG_NAME="export MEMGRAPH_ORGANIZATION_NAME=$organization_name"
  local BUILD_DIR="$MGBUILD_ROOT_DIR/build"

  # NOTE: If you need a fresh copy of memgraph files, call copy_project_files funcation on the line below.
  echo "Running $1 test on $build_container..."
  case "$1" in
    unit)
      if [[ "$threads" == "$DEFAULT_THREADS" ]]; then
        docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN "'&& ctest -R memgraph__unit --output-on-failure -j$(nproc)'
      else
        local EXPORT_THREADS="export THREADS=$threads"
        docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && $EXPORT_THREADS && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN "'&& ctest -R memgraph__unit --output-on-failure -j$THREADS'
      fi
    ;;
    unit-coverage)
      local setup_lsan_ubsan="export LSAN_OPTIONS=suppressions=$BUILD_DIR/../tools/lsan.supp && export UBSAN_OPTIONS=halt_on_error=1"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN && $setup_lsan_ubsan "'&& ctest -R memgraph__unit --output-on-failure -j2'
    ;;
    leftover-CTest)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $BUILD_DIR && $ACTIVATE_TOOLCHAIN "'&& ctest -E "(memgraph__unit|memgraph__benchmark)" --output-on-failure'
    ;;
    drivers)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR && export DISABLE_NODE=$DISABLE_NODE "'&& ./tests/drivers/run.sh'
    ;;
    drivers-high-availability)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR && $ACTIVATE_TOOLCHAIN && export DISABLE_NODE=$DISABLE_NODE "'&& ./tests/drivers/run_cluster.sh'
    ;;
    integration)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR && tests/integration/run.sh"
    ;;
    cppcheck-and-clang-format)
      local test_output_path="$MGBUILD_ROOT_DIR/tools/github/cppcheck_and_clang_format.txt"
      local test_output_host_dest="$PROJECT_ROOT/tools/github/cppcheck_and_clang_format.txt"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tools/github && $ACTIVATE_TOOLCHAIN "'&& ./cppcheck_and_clang_format diff'
      docker cp $build_container:$test_output_path $test_output_host_dest
    ;;
    stress-plain)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate "'&& ./continuous_integration'
      # TODO: Add when mgconsole is available on CI
      # docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source ve3/bin/activate "'&& ./continuous_integration --config-file=configurations/templates/config_ha.yaml'
    ;;
    stress-ssl)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate && ./continuous_integration --config-file=configurations/templates/config_ssl.yaml"
    ;;
    stress-large)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate && ./continuous_integration --config-file=configurations/templates/config_large.yaml"
    ;;
    durability)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate && python3 durability --num-steps 5 --log-file=durability_test.log --verbose"
    ;;
    durability-large)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/stress && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate && python3 durability --num-steps 5 --log-file=durability_test_large.log --verbose"
    ;;
    gql-behave)
      local test_output_dir="$MGBUILD_ROOT_DIR/tests/gql_behave"
      local test_output_host_dest="$PROJECT_ROOT/tests/gql_behave"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/gql_behave && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate && ./continuous_integration"
      docker cp $build_container:$test_output_dir/gql_behave_status.csv $test_output_host_dest/gql_behave_status.csv
      docker cp $build_container:$test_output_dir/gql_behave_status.html $test_output_host_dest/gql_behave_status.html
    ;;
    macro-benchmark)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && export USER=mg && export LANG=$(echo $LANG) && cd $MGBUILD_ROOT_DIR/tests/macro_benchmark "'&& ./harness QuerySuite MemgraphRunner --groups aggregation 1000_create unwind_create dense_expand match --no-strict'
    ;;
    macro-benchmark-parallel)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && export USER=mg && export LANG=$(echo $LANG) && cd $MGBUILD_ROOT_DIR/tests/macro_benchmark "'&& ./harness QueryParallelSuite MemgraphRunner --groups aggregation_parallel create_parallel bfs_parallel --num-database-workers 9 --num-clients-workers 30 --no-strict'
    ;;
    micro-benchmark)
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && $ACTIVATE_TOOLCHAIN && $ACTIVATE_CARGO && cd $MGBUILD_ROOT_DIR/build "'&& ulimit -s 262144 && ctest -R memgraph__benchmark -V'
    ;;
    mgbench)
      shift 1
      local DATASET='pokec'
      local DATASET_SIZE='medium'
      local EXPORT_RESULTS_FILE='benchmark_result.json'

      while [[ $# -gt 0 ]]; do
        case "$1" in
          --dataset)
            DATASET="$2"
            shift 2
          ;;
          --size)
            DATASET_SIZE="$2"
            shift 2
          ;;
          --export-results-file)
            EXPORT_RESULTS_FILE="$2"
            shift 2
          ;;
          *)
            echo "Error: Unknown flag '$1' for mgbench"
            echo "Supported flags: --dataset, --size, --export-results-file"
            exit 1
          ;;
        esac
      done

      check_support pokec_size $DATASET_SIZE
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/mgbench && ./benchmark.py --installation-type native --num-workers-for-benchmark 12 --export-results $EXPORT_RESULTS_FILE $DATASET/$DATASET_SIZE/*/*"
    ;;
    mgbench-supernode)
      shift 1
      local EXPORT_RESULTS_FILE='benchmark_result.json'
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --export-results-file)
            EXPORT_RESULTS_FILE="$2"
            shift 2
          ;;
        esac
      done

      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tests/mgbench && ./benchmark.py --installation-type native --num-workers-for-benchmark 1 --export-results $EXPORT_RESULTS_FILE supernode"
    ;;
    upload-to-bench-graph)
      shift 1
      local SETUP_PASSED_ARGS="export PASSED_ARGS=\"$@\""
      local SETUP_VE3_ENV="virtualenv -p python3 ve3 && source ve3/bin/activate && pip install -r requirements.txt"
      local SETUP_BENCH_GRAPH_SERVER_ENDPOINT="export BENCH_GRAPH_SERVER_ENDPOINT=$bench_graph_host:$bench_graph_port"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && cd $MGBUILD_ROOT_DIR/tools/bench-graph-client && $SETUP_VE3_ENV && $SETUP_BENCH_GRAPH_SERVER_ENDPOINT && $SETUP_PASSED_ARGS "'&& ./main.py $PASSED_ARGS'
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
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && $ACTIVATE_TOOLCHAIN && cd $MGBUILD_ROOT_DIR/tests/code_analysis && $SETUP_PASSED_ARGS "'&& ./clang_tidy.sh $PASSED_ARGS'
    ;;
    e2e)
      # NOTE: Python query modules deps have to be installed globally because memgraph expects them to be.
      docker exec -u root $build_container bash -c "apt-get update && apt-get install -y lsof" # TODO(matt): install within mgbuild container
      docker exec -u mg $build_container bash -c "PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --upgrade pip"
      docker exec -u mg $build_container bash -c "pip install --break-system-packages --user networkx==2.5.1"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && $ACTIVATE_CARGO && $ACTIVATE_TOOLCHAIN && cd $MGBUILD_ROOT_DIR/tests && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate && cd $MGBUILD_ROOT_DIR/tests/e2e && export DISABLE_NODE=$DISABLE_NODE && ./run.sh"
    ;;
    query_modules_e2e)
      # NOTE: Python query modules deps have to be installed globally because memgraph expects them to be.
      docker exec -u mg $build_container bash -c "PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --upgrade pip"
      docker exec -u mg $build_container bash -c "pip install --break-system-packages --user -r $MGBUILD_ROOT_DIR/tests/query_modules/requirements.txt"
      docker exec -u mg $build_container bash -c "$EXPORT_LICENSE && $EXPORT_ORG_NAME && $ACTIVATE_CARGO && cd $MGBUILD_ROOT_DIR/tests/query_modules && source $MGBUILD_ROOT_DIR/tests/ve3/bin/activate && python3 -m pytest ."
    ;;
    *)
      echo "Error: Unknown test '$1'"
      print_help
      exit 1
    ;;
  esac
}


build_heaptrack() {
  local ACTIVATE_TOOLCHAIN="source /opt/toolchain-${toolchain_version}/activate"
  docker exec -i -u root $build_container bash -c "apt-get update && apt-get install -y libdw-dev libboost-all-dev"
  docker exec -i -u root $build_container bash -c "mkdir -p /tmp/heaptrack && chown mg:mg /tmp/heaptrack"

  docker cp tools/build-heaptrack.sh $build_container:$MGBUILD_HOME_DIR/build-heaptrack.sh
  docker exec -u mg $build_container bash -c "$ACTIVATE_TOOLCHAIN && cd $MGBUILD_HOME_DIR && ./build-heaptrack.sh"
}

copy_heaptrack() {
  local dest_dir="release/docker"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --dest-dir)
        dest_dir=$2
        shift 2
      ;;
      *)
        echo "Error: Unknown flag '$1'"
        print_help
        exit 1
    esac
  done
  docker cp $build_container:/tmp/heaptrack/ $dest_dir
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
bench_graph_host=$DEFAULT_BENCH_GRAPH_HOST
bench_graph_port=$DEFAULT_BENCH_GRAPH_PORT
mgdeps_cache_host=$DEFAULT_MGDEPS_CACHE_HOST
mgdeps_cache_port=$DEFAULT_MGDEPS_CACHE_PORT
ccache_enabled=$DEFAULT_CCACHE_ENABLED
conan_cache_enabled=$DEFAULT_CONAN_CACHE_ENABLED
conan_cache_dir=""
command=""
build_container=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch)
        arch=$2
        check_support arch $arch
        shift 2
    ;;
    --bench-graph-host)
        bench_graph_host=$2
        shift 2
    ;;
    --bench-graph-port)
        bench_graph_port=$2
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
    --mgdeps-cache-host)
        mgdeps_cache_host=$2
        shift 2
    ;;
    --mgdeps-cache-port)
        mgdeps_cache_port=$2
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
    --no-ccache)
      ccache_enabled="false"
      shift 1
    ;;
    --no-conan-cache)
      conan_cache_enabled="false"
      shift 1
    ;;
    --conan-cache-dir)
      conan_cache_dir=$2
      shift 2
    ;;
    *)
      if [[ "$1" =~ ^--.* ]]; then
        echo -e "Error: Unknown option '$1'"
        print_help
        exit 1
      else
        command=$1
        shift 1
        break
      fi
    ;;
  esac
done

if [[ -z "$conan_cache_dir" ]]; then
  conan_cache_dir="$HOME/.conan2-ci"
fi

if [[ "$os" != "all" ]]; then
  if [[ "$arch" == 'arm' ]] && [[ "$os" != *"-arm" ]]; then
    os="${os}-arm"
  fi
  check_support os $os
  check_support os_toolchain_combo $os $toolchain_version
fi

build_container="mgbuild_${toolchain_version}_${os}"

if [[ "$command" == "" ]]; then
  echo -e "Error: Command not provided, please provide command"
  print_help
  exit 1
fi

if docker compose version > /dev/null 2>&1; then
  docker_compose_cmd="docker compose"
elif which docker-compose > /dev/null 2>&1; then
  docker_compose_cmd="docker-compose"
else
  echo -e "Missing command: There has to be installed either 'docker-compose' or 'docker compose'"
  exit 1
fi
echo "Using $docker_compose_cmd"

##################################################
################# PARSE COMMAND ##################
##################################################
case $command in
    build)
      cd $SCRIPT_DIR
      # Default values for --git-ref, --rust-version and --node-version
      git_ref_flag="--build-arg GIT_REF=master"
      rust_version_flag="--build-arg RUST_VERSION=1.80"
      node_version_flag="--build-arg NODE_VERSION=20"
      while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --git-ref)
              git_ref_flag="--build-arg GIT_REF=$2"
              shift 2
            ;;
            --rust-version)
              rust_version_flag="--build-arg RUST_VERSION=$2"
              shift 2
            ;;
            --node-version)
              node_version_flag="--build-arg NODE_VERSION=$2"
              shift 2
            ;;
            *)
              echo "Error: Unknown flag '$1'"
              print_help
              exit 1
            ;;
        esac
      done
      if [[ "$os" == "all" ]]; then
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml build $git_ref_flag $rust_version_flag $node_version_flag
      else
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml build $git_ref_flag $rust_version_flag $node_version_flag $build_container
      fi
    ;;
    init-tests)
      init_tests
    ;;
    run)
      cd $SCRIPT_DIR
      pull=false
      while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --pull)
              pull=true
              shift 1
            ;;
            *)
              echo "Error: Unknown flag '$1'"
              print_help
              exit 1
            ;;
        esac
      done

      # Create ccache override file if ccache is enabled
      compose_files=$(setup_cache_override)
      if [[ "$conan_cache_enabled" == "true" ]]; then
        echo "Setting conan cache directory: $conan_cache_dir"
      fi

      # Set up host ccache permissions
      setup_host_cache_permissions
      if [[ "$os" == "all" ]]; then
        if [[ "$pull" == "true" ]]; then
          $docker_compose_cmd $compose_files pull --ignore-pull-failures
        elif [[ "$docker_compose_cmd" == "docker compose" ]]; then
            $docker_compose_cmd $compose_files pull --ignore-pull-failures --policy missing
        fi
        $docker_compose_cmd $compose_files up -d
      else
        if [[ "$pull" == "true" ]]; then
          $docker_compose_cmd $compose_files pull $build_container
        elif ! docker image inspect memgraph/mgbuild:${toolchain_version}_${os} > /dev/null 2>&1; then
          $docker_compose_cmd $compose_files pull --ignore-pull-failures $build_container
        fi
        $docker_compose_cmd $compose_files up -d $build_container

        # set local mirror for Ubuntu
        if [[ "$os" =~ ^"ubuntu".* && "$arch" == "amd" ]]; then
          if [[ "$os" == "ubuntu-22.04" ]]; then
            if mirror="$(${PROJECT_ROOT}/tools/test-mirrors.sh 'jammy')"; then
              # set custom mirror within build container
              docker exec -i -u root \
                -e CUSTOM_MIRROR=$mirror \
                $build_container \
              bash -c '
                if [ -n "$CUSTOM_MIRROR" ]; then
                  sed -E -i \
                    -e "s#https?://[^ ]*archive\.ubuntu\.com/ubuntu/#${CUSTOM_MIRROR}/#g" \
                    -e "s#https?://[^ ]*security\.ubuntu\.com/ubuntu/#${CUSTOM_MIRROR}/#g" \
                    /etc/apt/sources.list
                  apt-get update -qq
                fi
              '
            fi
          else
            if mirror="$(${PROJECT_ROOT}/tools/test-mirrors.sh)"; then
              # set custom mirror within build container
              docker exec -i -u root \
                -e CUSTOM_MIRROR=$mirror \
                $build_container \
              bash -c '
                if [ -n "$CUSTOM_MIRROR" ]; then
                  sed -E -i \
                    -e "/^URIs:/ s#https?://[^ ]*archive\.ubuntu\.com#${CUSTOM_MIRROR}#g" \
                    -e "/^URIs:/ s#https?://security\.ubuntu\.com#${CUSTOM_MIRROR}#g" \
                    /etc/apt/sources.list.d/ubuntu.sources
                  apt-get update -qq
                fi
              '
            fi
          fi
        fi
      fi

      # Install ccache if enabled
      if [[ "$ccache_enabled" == "true" ]]; then
        echo "Installing ccache in container..."
        if [[ "$os" =~ ^"debian".* || "$os" =~ ^"ubuntu".* ]]; then
          docker exec -u root $build_container bash -c "apt update && apt install -y ccache"
        elif [[ "$os" =~ ^"centos".* || "$os" =~ ^"rocky".* || "$os" =~ ^"fedora".* ]]; then
          if [[ "$os" =~ ^"centos".* ]]; then
            docker exec -u root $build_container bash -c "dnf config-manager --set-enabled crb"
            docker exec -u root $build_container bash -c "dnf install -y epel-release"
          fi
          docker exec -u root $build_container bash -c "dnf -y install ccache"
        else
          echo "Warning: Unknown OS $os - not installing ccache"
        fi

        # Verify ccache installation and permissions
        echo "Verifying ccache installation..."
        docker exec -u mg $build_container bash -c "
          ccache --version
          ccache -s
          echo 'Ccache is ready for use'
        "

        # Set cache directory permissions for cross-container access
        echo "Setting cache directory permissions for cross-container access..."
        docker exec -u root $build_container bash -c "
          chmod -R a+rwX /home/mg/.cache/ccache
          echo 'Cache directory permissions set for cross-container access'
        "
      fi

      # Ensure .cache directory permissions are correct for all tools (pip, go, etc.)
      echo "Setting up .cache directory permissions for all tools..."
      docker exec -u root $build_container bash -c "
        mkdir -p /home/mg/.cache
        chown -R mg:mg /home/mg/.cache
        chmod -R 755 /home/mg/.cache
        echo '.cache directory permissions set for all tools'
      "

      # Set up conan cache directory permissions if conan cache is enabled
      if [[ "$conan_cache_enabled" == "true" ]]; then
        echo "Setting up conan cache directory permissions for cross-container access..."
        docker exec -u root $build_container bash -c "
          mkdir -p /home/mg/.conan2
          chown -R mg:mg /home/mg/.conan2
          chmod -R a+rwX /home/mg/.conan2
          echo 'Conan cache directory permissions set for cross-container access'
        "
      fi

      # Clean up override files if they were created
      cleanup_cache_override
    ;;
    stop)
      cd $SCRIPT_DIR
      remove=false
      while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --remove)
              remove=true
              shift 1
            ;;
            *)
              echo "Error: Unknown flag '$1'"
              print_help
              exit 1
            ;;
        esac
      done

      # clean up conan cache inside container
      docker exec -u mg $build_container bash -c "cd $MGBUILD_ROOT_DIR && ./tools/clean_conan.sh 1w"

      # Create cache override files (same logic as run command)
      compose_files=$(setup_cache_override)

      if [[ "$os" == "all" ]]; then
        $docker_compose_cmd $compose_files down
      else
        docker stop $build_container
        if [[ "$remove" == "true" ]]; then
          docker rm $build_container
        fi
      fi

      # Clean up override files if they were created
      cleanup_cache_override
    ;;
    pull)
      cd $SCRIPT_DIR

      # Create cache override files (same logic as run command)
      compose_files=$(setup_cache_override)

      if [[ "$os" == "all" ]]; then
        $docker_compose_cmd $compose_files pull --ignore-pull-failures
      else
        $docker_compose_cmd $compose_files pull $build_container
      fi

      # Clean up override files if they were created
      cleanup_cache_override
    ;;
    push)
      docker login $@
      cd $SCRIPT_DIR
      if [[ "$os" == "all" ]]; then
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml push --ignore-push-failures
      else
        $docker_compose_cmd -f ${arch}-builders-${toolchain_version}.yml push $build_container
      fi
    ;;
    build-memgraph)
      build_memgraph $@
    ;;
    package-memgraph)
      package_memgraph $@
    ;;
    test-memgraph)
      test_memgraph $@
    ;;
    copy)
      copy_memgraph $@
    ;;
    package-docker)
      package_docker $@
    ;;
    build-heaptrack)
      build_heaptrack $@
    ;;
    copy-heaptrack)
      copy_heaptrack $@
    ;;
    *)
        echo "Error: Unknown command '$command'"
        print_help
        exit 1
    ;;
esac
