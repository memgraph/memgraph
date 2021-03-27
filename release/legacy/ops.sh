#!/bin/bash

# TODO(gitbuda): Install toolchain deps somehow during image build phase.
# TODO(gitbuda): To build Docker image, just use Debian10 package + the release/docker/package_deb_docker.

make_package () {
    offering="$1"
    os="$2"
    build_container="legacy-mgbuild_$os"
    echo "TODO(gitbuda): Build $offering package for $os inside $build_container."
    # TODO(gitbuda): During runtime install deps from scratch because deps might change.
    exit 1
}

case "$1" in
    init)
        docker-compose build
        docker-compose up -d
    ;;
    package)
        shift 1
        offering="$1" # TODO(gitbuda): Validate.
        shift 1
        os="$1" # TODO(gitbuda): Validate.
        shift
        make_package "$offering" "$os"
    ;;
    *)
        echo "$0 init"
        exit 1
    ;;
esac
