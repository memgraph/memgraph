#!/usr/bin/env python3

import argparse
import subprocess
import threading
import time
from pathlib import Path
from queue import Queue

import docker
import yaml


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


class SynchronizedQueue:
    def __init__(self, values):
        self.queue = Queue()
        for value in values:
            self.queue.put(value)
        self._lock = threading.Lock()

    def has_next(self):
        with self._lock:
            return not self.queue.empty()

    def get_next(self):
        self._lock.acquire()
        folder = None
        if not self.queue.empty():
            folder = self.queue.get()
        else:
            folder = None
        self._lock.release()
        return folder


class ThreadSafePrint:
    def __init__(self):
        self._lock = threading.Lock()

    def print(self, *args, **kwargs):
        with self._lock:
            print(*args, **kwargs)


atomic_int = AtomicInteger(0)

thread_safe_print = ThreadSafePrint()


def start_container_from_image(image_tag, id: int):
    client = docker.from_env()

    return client.containers.run(image=image_tag, detach=True, name=f"container-{id}", remove=True)


def stop_container(container):
    thread_safe_print.print(f"Stopping container with ID: {container.id}")
    container.stop()
    thread_safe_print.print(f"Stopped container with ID: {container.id}")


def find_workloads_for_testing(container, container_root_directory, project_root_directory):
    # everything inside bash -c command should have ' as quotes
    docker_command = (
        f"docker exec -u mg {container.id} bash -c "
        f"\" find {container_root_directory}/build/tests/e2e/ -mindepth 1 -maxdepth 1 -type d -printf '%f\\n' \""
    )

    result = subprocess.run(docker_command, shell=True, capture_output=True, text=True)

    folders = set(result.stdout.split("\n"))
    folders_with_workloads_yaml = []
    for file in Path(f"{project_root_directory}/tests/e2e/").rglob("workloads.yaml"):
        if file.parent.name not in folders:
            print("Continuing on folder: ", file.parent.name)
            continue  # continue on folders which are not in build folders

        if str(file).endswith("/streams/workloads.yaml"):
            continue

        with open(file, "r") as f:
            all_workloads_per_file = yaml.load(f, Loader=yaml.FullLoader)["workloads"]
            all_workloads_per_file = [(file.parent.name, workload["name"]) for workload in all_workloads_per_file]
            folders_with_workloads_yaml.extend(all_workloads_per_file)

    return folders_with_workloads_yaml


def process_workloads(id, image_name, setup_command, synchronized_queue):
    thread_id = threading.get_ident()
    thread_safe_print.print(f">>>>Starting container in thread {thread_id}")
    container = start_container_from_image(image_name, id)
    if container is None:
        atomic_int.increment()
        print(f"Failed to start container in thread {thread_id}")
        return

    thread_safe_print.print(f">>>>Started container with id: {container.id} in thread {thread_id}")

    tasks_executed = []
    total_execution = 0
    while True:
        workload_pair = synchronized_queue.get_next()
        if workload_pair is None:
            break

        workload_folder, workload_name = workload_pair
        start_time = time.time()
        if atomic_int.get() > 0:
            thread_safe_print.print(f"Encountered errors on other thread, skipping this execution {thread_id}")
            break
        try:
            docker_command = (
                f"docker exec -u mg {container.id} bash -c "
                f'"{setup_command} && '
                f"python3 runner.py --workloads-root-directory {workload_folder} --workload-name '{workload_name}' \""
            )

            thread_safe_print.print(
                f"Processing workload: {workload_name} in thread {thread_id} using docker command {docker_command}"
            )
            res = subprocess.run(docker_command, shell=True, check=True)
            if res.returncode != 0:
                thread_safe_print.print(f"Failed to execute command: {docker_command}")
                atomic_int.increment()

            tasks_executed.append((workload_folder, workload_name))
            total_execution += time.time() - start_time
        except Exception as e:
            thread_safe_print.print(f"Exception: {e}")
            atomic_int.increment()
            break

    thread_safe_print.print(f">>>>Stopping container with id: {container.id}")
    stop_container(container)
    thread_safe_print.print(f"Thread {thread_id} executed {len(tasks_executed)} tasks in {total_execution} seconds.")


parser = argparse.ArgumentParser(description="Parse image name and thread arguments.")
parser.add_argument("--image", type=str, required=True, help="Image name")
parser.add_argument("--threads", type=int, required=True, help="Number of threads")
parser.add_argument("--container-root-dir", type=str, required=True, help="Memgraph folder root dir in container")
parser.add_argument("--setup-command", type=str, required=True, help="Command to run in the container")
parser.add_argument("--project-root-dir", type=str, required=True, help="Project root directory")
args = parser.parse_args()


client = docker.from_env()

# Find the image with the tag
images = client.images.list()
target_image = None
for image in images:
    for image_tag in image.tags:
        if args.image in image_tag:
            target_image = image
            break


if target_image:
    print(f"Found Image: ID: {target_image.id}, Tags: {target_image.tags}")
else:
    print(f"Image with tag {args.image} not found.")
    exit(1)


container_0 = start_container_from_image(target_image.tags[0], 0)
assert container_0 is not None, "Container not started!"

workloads = find_workloads_for_testing(container_0, args.container_root_dir, args.project_root_dir)
# TODO: this can be incorporated as 0th task with different logic, as we lose here 10 seconds
stop_container(container_0)

print(f">>>>Workloads {len(workloads)} found!")

for workload_folder, workload_name in workloads:
    print(f"Workload: {workload_name} in folder: {workload_folder}")

synchronized_queue = SynchronizedQueue(workloads)
threads = []
error = False
try:
    for i in range(1, args.threads + 1):
        thread = threading.Thread(
            target=process_workloads,
            args=(
                i,
                args.image,
                args.setup_command,
                synchronized_queue,
            ),
        )
        threads.append(thread)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

except Exception as e:
    print(f"Exception occurred: {e}")
    error = True
finally:
    for thread in threads:
        if thread.is_alive():
            thread.join()

if atomic_int.get() > 0 or error:
    exit(1)
