#!/bin/bash

VERSION="3.5.4"
CONAN_REMOTE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            VERSION=$2
            shift 2
        ;;
        --conan-remote)
            CONAN_REMOTE=$2
            shift 2
        ;;
        *)
            echo "Error: Unknown option '$1'"
            exit 1
        ;;
    esac
done

# check if python environment exists
create_env=false
if [ ! -f "env/bin/activate" ]; then
    create_env=true
fi

function exit_cleanup() {
    status=$?
    deactivate
    if [ "$create_env" = true ]; then
        rm -rf env
    fi
    exit $status
}

trap exit_cleanup EXIT ERR

if [ "$create_env" = true ]; then
  echo "Creating python environment"
  python3 -m venv env
fi

echo "Activating python environment"
source env/bin/activate

# check if conan is installed
if ! command -v conan &> /dev/null; then
    pip install conan==2.24.0
fi

# check if a conan profile exists
if [ ! -f "$HOME/.conan2/profiles/default" ]; then
    echo "Creating conan profile"
    conan profile detect
fi

if [[ -n "$CONAN_REMOTE" ]]; then
    conan remote add artifactory $CONAN_REMOTE --force
fi

conan install   \
  --lockfile="" \
  --requires=openssl/$VERSION  \
  --requires=zlib/1.3.1 \
  --build=missing \
  -o openssl/*:shared=True \
  --deployer=runtime_deploy \
  --output-folder=build/openssl
