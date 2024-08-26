#!/usr/bin/env python3
import argparse
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import docker


class AtomicInteger:
    def __init__(self, initial=0):
        self.value = initial
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.value += 1
            return self.value

    def decrement(self):
        with self._lock:
            self.value -= 1
            return self.value

    def get(self):
        with self._lock:
            return self.value

    def set(self, new_value):
        with self._lock:
            self.value = new_value


# Example usage
atomic_int = AtomicInteger(0)


def start_containers_from_image(image_tag_searched, num_containers):
    # Create a Docker client
    client = docker.from_env()

    # Find the image with the specified tag

    images = client.images.list()
    target_image = None
    for image in images:
        print(image.tags)
        for image_tag in image.tags:
            if image_tag_searched in image_tag:
                target_image = image
                break

    if not target_image:
        print(f"Image with tag '{image_tag_searched}' not found.")
        return []
    print("here > ", target_image.tags[0])
    # Start the specified number of containers from the found image
    containers = []
    for i in range(num_containers):
        print("Starting container: ", i)
        container = client.containers.run(image=target_image.tags[0], detach=True, name=f"container-{i}", remove=True)
        containers.append(container)
        print(f"Started container: ID: {container.id}")

    return containers


def stop_containers(containers):
    for container in containers:
        container.stop()
        print(f"Stopped container: ID: {container.id}")


def find_folders_with_workloads_yaml(root_directory):
    folders_with_workloads_yaml = []
    for file in Path(root_directory).rglob("workloads.yaml"):
        if str(file).endswith("/streams/workloads.yaml"):
            continue
        folders_with_workloads_yaml.append(file.parent.name)
    return folders_with_workloads_yaml


def split_folders(folders, num_parts):
    avg = len(folders) / float(num_parts)
    out = []
    last = 0.0

    while last < len(folders):
        out.append(folders[int(last) : int(last + avg)])
        last += avg

    return out


parser = argparse.ArgumentParser(description="Parse image name and thread arguments.")
parser.add_argument("--image", type=str, required=True, help="Image name")
parser.add_argument("--threads", type=int, required=True, help="Number of threads")
parser.add_argument("--folder", type=str, required=True, help="Folder")
parser.add_argument("--command", type=str, required=True, help="Command to run in the container")

args = parser.parse_args()

assert args.image is not None, "Image name is required"
assert args.folder is not None, "Folder is required"
assert args.threads is not None, "Number of threads is required"
assert args.command is not None, "Command is required"


print(f">>>>>>>>>>>>> COMMAND: {args.command} ")
print(f">>>>>>>>>>>>>>> FOLDER: {args.folder}")
# Create a Docker client
client = docker.from_env()

# Find the image with the tag 'mgbuild_v5_debian-11-all:latest'
images = client.images.list()
target_image = None
for image in images:
    print(image.tags)
    for image_tag in image.tags:
        if args.image in image_tag:
            target_image = image
            break


# Print the results
if target_image:
    print(f"Found Image: ID: {target_image.id}, Tags: {target_image.tags}")
else:
    print(f"Image with tag {args.image} not found.")
    exit(1)

containers = start_containers_from_image(target_image.tags[0], args.threads)

print(">>>>Containers started!")

for container in containers:
    print(container)


folders = find_folders_with_workloads_yaml(args.folder)
print(f"Folders len {len(folders)}")

for folder in folders:
    print("Folder: ", folder)


def process_folders(container, folders):
    print(">>>>>>>>Executing process_folders!")
    for folder in folders:
        if atomic_int.get() > 0:
            print("Encountered errors on other thread, skipping this execution")
            return
        try:
            docker_command = (
                f"docker exec -u mg {container.id} bash -c "
                f'"{args.command} && '
                f'python3 runner.py --workloads-root-directory {folder}"'
            )

            print(f"Processing folder: {folder}, using docker command {docker_command}")
            res = subprocess.run(docker_command, shell=True, check=True)
            if res.returncode != 0:
                print(f"Failed to execute command: {docker_command}")
                atomic_int.increment()
        except Exception as e:
            print(f"Exception: {e}")
            atomic_int.increment()
            return


folder_parts = split_folders(folders, args.threads)

try:
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        for i, part in enumerate(folder_parts):
            print("doing submit")
            executor.submit(process_folders, containers[i], part)
except Exception as e:
    print(f"Exception: {e}")
finally:
    stop_containers(containers)
