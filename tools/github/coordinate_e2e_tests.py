#!/usr/bin/env python3

import argparse
import os
import subprocess
import tempfile
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from typing import List, Tuple

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


class SynchronizedMap:
    def __init__(self):
        self._map = defaultdict()
        self._lock = threading.Lock()

    def insert_elem(self, key, value):
        with self._lock:
            self._map[key] = value

    def remove_key(self, key):
        with self._lock:
            self._map.pop(key)

    def get_shallow_copy(self):
        copy_map = defaultdict()
        with self._lock:
            for key, value in self._map.items():
                copy_map[key] = value
        return copy_map


class SynchronnizedPrint:
    def __init__(self):
        self._lock = threading.Lock()

    def print(self, *args, **kwargs):
        with self._lock:
            print(*args, **kwargs)


def clear_directory(path):
    if os.path.exists(path):
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(path)


def create_directory(path):
    os.makedirs(path, exist_ok=True)


class SynchronizedContainerCopy:
    def __init__(self):
        self._lock = threading.Lock()

    def copy_logs(self, src_container_name, dest_container_name, container_path, temp_dir):
        with self._lock:
            try:
                clear_directory(temp_dir)
            except Exception as e:
                print(f"Exception: {e}")

            create_directory(f"{temp_dir}/logs")

            # Copy logs from source container to temporary directory
            copy_from_src_command = f"docker cp {src_container_name}:{container_path} {temp_dir}"
            subprocess.run(copy_from_src_command, shell=True, check=True)

            # Copy logs from temporary directory to destination container
            copy_to_dest_command = f"docker cp {temp_dir}/logs {dest_container_name}:{container_path}"
            subprocess.run(copy_to_dest_command, shell=True, check=True)


@dataclass
class ContainerInfo:
    container_name: str
    tasks_executed: List[Tuple[str, str, float]]
    exception: str
    output: List[Tuple[str, str]]


atomic_int = AtomicInteger(0)
synchronized_print = SynchronnizedPrint()
synchronized_map = SynchronizedMap()
temp_dir = tempfile.TemporaryDirectory().name
synchronized_container_copy = SynchronizedContainerCopy()


def start_container_from_image(image_tag, id: int):
    client = docker.from_env()

    return client.containers.run(image=image_tag, detach=True, name=f"container-{id}", remove=True)


def stop_container(container):
    synchronized_print.print(f"Stopping container with ID: {container.id}")
    container.stop()
    synchronized_print.print(f"Stopped container with ID: {container.id}")


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


def copy_output_to_container(
    container_to_copy, container_name, container_root_dir, formated_stdout_output, formated_stderr_output
):
    stdout_file_path = os.path.join(temp_dir, f"{container_name}_stdout.log")
    stderr_file_path = os.path.join(temp_dir, f"{container_name}_stderr.log")

    print(stderr_file_path)
    print(stdout_file_path)
    with open(stdout_file_path, "w") as stdout_file:
        stdout_file.write(formated_stdout_output)

    with open(stderr_file_path, "w") as stderr_file:
        stderr_file.write(formated_stderr_output)

    # Copy files to Docker container
    copy_stdout_command = f"docker cp {stdout_file_path} {container_to_copy}:{container_root_dir}/build/logs/"
    copy_stderr_command = f"docker cp {stderr_file_path} {container_to_copy}:{container_root_dir}/build/logs/"

    subprocess.run(copy_stdout_command, shell=True, check=True)
    subprocess.run(copy_stderr_command, shell=True, check=True)

    os.remove(stdout_file_path)
    os.remove(stderr_file_path)


def process_workloads(
    id: int,
    image_name,
    setup_command: str,
    synchronized_queue: SynchronizedQueue,
    synchronized_map: SynchronizedMap,
    container_root_dir: str,
    thread_safe_container_copy: SynchronizedContainerCopy,
    original_container_id: str,
):
    synchronized_print.print(f">>>>Starting container-{id}")
    container = start_container_from_image(image_name, id)
    if container is None:
        atomic_int.increment()
        synchronized_print.print(f"Failed to start container {id}")
        return

    synchronized_print.print(f">>>>Started container-{id} with id {container.id}")

    tasks_executed = []
    total_execution = 0
    exception = None
    output = []
    while True:
        if atomic_int.get() > 0:
            synchronized_print.print(f"Encountered errors on other thread, stopping this execution {id}")
            break

        workload_pair = synchronized_queue.get_next()
        if workload_pair is None:
            break

        workload_folder, workload_name = workload_pair
        try:
            docker_command = (
                f"docker exec -u mg {container.id} bash -c "
                f'"{setup_command} && '
                f"python3 runner.py --workloads-root-directory {workload_folder} --workload-name '{workload_name}' \""
            )

            start_time = time.time()
            res = subprocess.run(docker_command, shell=True, capture_output=True)

            tasks_executed.append((workload_folder, workload_name, time.time() - start_time))
            total_execution += time.time() - start_time
            res_stdout_formatted = "\n".join(res.stdout.decode("utf-8").split("\n"))
            res_stderr_formatted = "\n".join(res.stderr.decode("utf-8").split("\n"))
            output.append((res_stdout_formatted, res_stderr_formatted))

            if res.returncode != 0:
                synchronized_print.print(
                    f"Fail! Container id: container-{id} failed workload: {workload_name} using docker command {docker_command}"
                )
                atomic_int.increment()
                break
            else:
                synchronized_print.print(
                    f"\nSuccess! Container id: container-{id} executed workload: {workload_name} using docker command {docker_command}\n"
                )
        except Exception as e:
            exception = str(e)
            synchronized_print.print(f"Exception: {e}")
            atomic_int.increment()
            break

    try:
        synchronized_print.print(f">>>>Copying logs from container-{id}")
        thread_safe_container_copy.copy_logs(
            container.id, original_container_id, f"{container_root_dir}/build/logs/", temp_dir
        )
        synchronized_print.print(f">>>>Copied logs from container-{id}")
    except Exception as e:
        synchronized_print.print(f"Exception occurred while copying logs from container-{id}: {e}")

    synchronized_print.print(f">>>>Stopping container-{id}")
    stop_container(container)
    synchronized_print.print(f">>>>Stopped container-{id}")

    tasks_executed.sort(key=lambda x: x[2], reverse=True)
    synchronized_map.insert_elem(
        id,
        ContainerInfo(
            container_name=f"container-{id}", tasks_executed=tasks_executed, exception=exception, output=output
        ),
    )


parser = argparse.ArgumentParser(description="Parse image name and thread arguments.")
parser.add_argument("--image", type=str, required=True, help="Image name")
parser.add_argument("--threads", type=int, required=True, help="Number of threads")
parser.add_argument("--container-root-dir", type=str, required=True, help="Memgraph folder root dir in container")
parser.add_argument("--setup-command", type=str, required=True, help="Command to run in the container")
parser.add_argument("--project-root-dir", type=str, required=True, help="Project root directory")
parser.add_argument(
    "--original-container-id", type=str, required=True, help="Original container ID to which logs will be copied"
)

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
                synchronized_map,
                args.container_root_dir,
                synchronized_container_copy,
                args.original_container_id,
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

print("SUMMARY:")
for id, container_info in synchronized_map.get_shallow_copy().items():
    print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print(f"\t\tContainer id: {id} \n ")

    stdout, stderr = [out[0] for out in container_info.output], [out[1] for out in container_info.output]

    formated_stdout_output = "\n".join(stdout)
    formated_stderr_output = "\n".join(stderr)

    copy_output_to_container(
        args.original_container_id,
        f"container-{id}",
        args.container_root_dir,
        formated_stdout_output,
        formated_stderr_output,
    )

    total_time = sum([x[2] for x in container_info.tasks_executed])
    print(f"\t\tContainer-{id} executed {len(container_info.tasks_executed)} tasks in {total_time} seconds.")
    for task in container_info.tasks_executed:
        print(f"\t\t\t {task}")
    print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


# Step 1: Collect all tasks from all containers
all_tasks = []
for id, container_info in synchronized_map.get_shallow_copy().items():
    for task in container_info.tasks_executed:
        all_tasks.append(task)

# Step 2: Calculate the average execution time of all tasks
total_time = sum(task[2] for task in all_tasks)
average_time = total_time / len(all_tasks)

# Step 3: Calculate the deviation of each task from the average time
tasks_with_deviation = [(task, abs(task[2] - average_time)) for task in all_tasks]

# Step 4: Sort tasks by their deviation in descending order
tasks_with_deviation.sort(key=lambda x: x[1], reverse=True)

# Step 5: Select the top 10 tasks with the highest deviation
top_10_tasks = tasks_with_deviation[:10]

# Step 6: Print the final list of these tasks
print("Top 10 tasks with the highest deviation from the average time:")
for task, deviation in top_10_tasks:
    print(f"Task: {task}, Deviation: {deviation}")

print(f"Errors occurred {atomic_int.get()}")

if atomic_int.get() > 0 or error:
    exit(1)
