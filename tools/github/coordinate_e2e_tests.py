#!/usr/bin/env python3

"""
This script is used to coordinate the execution of e2e tests in parallel.
It starts multiple containers, each container executes a workload from the list of workloads.
The script is used to find workloads in the build directory of the container and then execute them.
The script takes care of creating docker image from running container, starting multiple containers in parallel,
executing workloads in parallel and then stopping the containers.


"""
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


def get_container_name(id: int):
    return f"container-{id}"


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


class SynchronizedQueue:
    def __init__(self, values):
        self.queue = Queue()
        for value in values:
            self.queue.put(value)
        self._lock = threading.Lock()

    def get_next(self):
        elem = None
        with self._lock:
            if not self.queue.empty():
                elem = self.queue.get()
        return elem


class SynchronizedMap:
    def __init__(self):
        self._map = defaultdict()
        self._lock = threading.Lock()

    def insert_elem(self, key, value):
        with self._lock:
            self._map[key] = value

    def reset_and_get(self):
        copy_map = defaultdict()
        with self._lock:
            self_map, copy_map = copy_map, self._map
        return copy_map


class SynchronizedPrint:
    def __init__(self):
        self._lock = threading.Lock()

    def print(self, *args, **kwargs):
        with self._lock:
            print(*args, **kwargs)


class DockerHandler:
    def __init__(self):
        self._client = docker.from_env()

    def commit_image(self, build_container, new_image_name):
        try:
            self._client.containers.get(build_container).commit(repository=new_image_name)
        except Exception as e:
            print(f"Exception: {e}")
            return False
        return True

    def exists_image(self, image_name):
        images = self._client.images.list()
        target_image = None
        for image in images:
            for image_tag in image.tags:
                if image_name in image_tag:
                    target_image = image
                    break
        return target_image is not None

    def remove_image(self, image_name):
        self._client.images.remove(image_name)

    def start_container_from_image(self, image_tag, id: int):
        return self._client.containers.run(image=image_tag, detach=True, name=f"container-{id}", remove=True)

    def stop_container(self, container):
        container.stop()


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


error_counter = AtomicInteger(0)
synchronized_print = SynchronizedPrint()
synchronized_map = SynchronizedMap()
temp_dir = tempfile.TemporaryDirectory().name
synchronized_container_copy = SynchronizedContainerCopy()


def find_workloads_for_testing(container, container_root_directory, project_root_directory) -> List[Tuple[str, str]]:
    """
    This function gets all folders in the build directory of the container.
    E2E tests work by testing only folders which are in build directory, as all the folders are not copied there.
    Once we found all the folders in the build directory of the container, we can get all the workloads.yaml files
    from project_root_directory and check then if given workloads.yaml is actually in the folder inside build directory.

    :param container: ID of container which has executed build command for e2e tests
    :param container_root_directory: path to the root directory to memgraph folder inside container, i.e. /home/mg/memgraph/
    :param project_root_directory: path to the root directory of memgraph folder in current structure in the github actions
    :return: List of tuples
        Each tuple contains folder name and workload name from workloads.yaml file
        i.e. [('high_availability', 'Distributed coords part 2'), ...]
    """
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
    docker_handler = DockerHandler()
    synchronized_print.print(f">>>>Starting container-{id}")
    container = docker_handler.start_container_from_image(image_name, id)
    if container is None:
        error_counter.increment()
        synchronized_print.print(f"Failed to start container {id}")
        return

    synchronized_print.print(f">>>>Started container-{id} with id {container.id}")

    tasks_executed = []
    total_execution = 0
    exception = None
    output = []
    while True:
        if error_counter.get() > 0:
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
                error_counter.increment()
                break
            else:
                synchronized_print.print(
                    f"\nSuccess! Container id: container-{id} executed workload: {workload_name} using docker command {docker_command}\n"
                )
        except Exception as e:
            exception = str(e)
            synchronized_print.print(f"Exception: {e}")
            error_counter.increment()
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
    docker_handler.stop_container(container)
    synchronized_print.print(f">>>>Stopped container-{id}")

    tasks_executed.sort(key=lambda x: x[2], reverse=True)
    synchronized_map.insert_elem(
        id,
        ContainerInfo(
            container_name=f"container-{id}", tasks_executed=tasks_executed, exception=exception, output=output
        ),
    )


parser = argparse.ArgumentParser(description="Parse image name and thread arguments.")
parser.add_argument("--threads", type=int, required=True, help="Number of threads")
parser.add_argument("--container-root-dir", type=str, required=True, help="Memgraph folder root dir in container")
parser.add_argument("--setup-command", type=str, required=True, help="Command to run in the container")
parser.add_argument("--project-root-dir", type=str, required=True, help="Project root directory")
parser.add_argument(
    "--original-container-id", type=str, required=True, help="Original container ID to which logs will be copied"
)

args = parser.parse_args()

print(f"Threads: {args.threads}")
print(f"Container Root Dir: {args.container_root_dir}")
print(f"Setup Command: {args.setup_command}")
print(f"Project Root Dir: {args.project_root_dir}")
print(f"Original Container ID: {args.original_container_id}")

image_name = f"{args.original_container_id}-e2e"

docker_handler = DockerHandler()

if docker_handler.exists_image(image_name):
    print(f"Image {image_name} already exists, removing it!")
    docker_handler.remove_image(image_name)
    print(f"Removed image {image_name}!")
else:
    print(f"Image {image_name} does not exist!")

print(f"Committing container {args.original_container_id} to {image_name}")
ok = docker_handler.commit_image(args.original_container_id, image_name)

if not ok:
    print(f"Failed to commit image {image_name}")
    exit(1)
print(f"Committed image {image_name}")

print(f"Starting container from image {image_name}")
container_0 = docker_handler.start_container_from_image(image_name, 0)
assert container_0 is not None, "Container not started!"

workloads = find_workloads_for_testing(container_0, args.container_root_dir, args.project_root_dir)
# TODO: this can be incorporated as 0th task with different logic, as we lose here 10 seconds
print(f"Stopping container {container_0}")
docker_handler.stop_container(container_0)

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
                image_name,
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

# TODO remove shallow copy and use instead reset method
print("SUMMARY:")
for id, container_info in synchronized_map.reset_and_get().items():
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


print(f"Errors occurred {error_counter.get()}")

docker_handler.remove_image(image_name)
if docker_handler.exists_image(image_name):
    print(f"Failed to remove image {image_name}")
    exit(1)

if error_counter.get() > 0 or error:
    exit(1)
