#!/bin/bash
DOCKER_REPO=$1
TAG=$2

# print the image size in both SI and IEC units
size=$(docker image inspect $DOCKER_REPO:$TAG -f '{{.Size}}')
size_si=$(numfmt --to=si --suffix=B --format="%.3f" $size | sed 's/\([KMGT]\)B/\1 B/')
size_iec=$(numfmt --to=iec-i --suffix=B --format="%.3f" $size | sed 's/\([KMGT]\)B/\1 B/')
echo "Image size: ${size_si} / ${size_iec}"
