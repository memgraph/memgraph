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
from typing import List, Optional, Tuple

import docker
import yaml

from query_modules.nxalg import center

CONTAINER_NAME_PREFIX = "container-"
LOGS_PATH = "/build/logs/"


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
    global CONTAINER_NAME_PREFIX
    return f"{CONTAINER_NAME_PREFIX}-{id}"


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
    """
    Class used to synchronize print statements.
    Print is not thread safe in python, so we need to synchronize it.
    """

    def __init__(self):
        self._lock = threading.Lock()

    def print(self, *args, **kwargs):
        with self._lock:
            print(*args, **kwargs)


class DockerHandler:
    """
    Class used to handle docker operations.
    """

    def __init__(self):
        self._client = docker.from_env(timeout=600)

    def commit_image(self, build_container, new_image_name):
        try:
            self._client.containers.get(build_container).commit(repository=new_image_name)
        except Exception as e:
            print(f"Exception: {e}")
            return False
        return True

    def image_exists(self, image_name):
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
        return self._client.containers.run(image=image_tag, detach=True, name=f"{get_container_name(id)}", remove=True)

    def stop_container(self, container):
        container.stop()

    def get_client(self):
        return self._client


class SynchronizedContainerCopy:
    """
    This class is used to copy logs from one container to another in synchronized manner.
    To copy logs from container to container we first copy logs to temporary directory on host machine.
    After that we copy logs from temporary directory to destination container.
    """

    def __init__(self):
        self._lock = threading.Lock()

    def copy_logs(self, src_container_name, dest_container_name, container_path, temp_dir):
        with self._lock:
            try:
                clear_directory(temp_dir)
            except Exception as e:
                pass  # ignore

            folder_name = f"{temp_dir}/logs"
            create_directory(f"{folder_name}")

            subprocess.run(
                f"docker exec {src_container_name} bash -c 'if [ ! -d '{folder_name}' ]; then mkdir -p '{folder_name}'; fi'",
                shell=True,
                check=True,
            )
            # Copy logs from source container to temporary directory
            copy_from_src_command = f"docker cp {src_container_name}:{container_path} {temp_dir}/logs"
            subprocess.run(copy_from_src_command, shell=True, check=True)

            # Copy logs from temporary directory to destination container
            copy_to_dest_command = f"docker cp {temp_dir}/logs {dest_container_name}:{container_path}"
            subprocess.run(copy_to_dest_command, shell=True, check=True)

            try:
                clear_directory(temp_dir)
            except Exception as _:
                pass  # ignore


@dataclass
class ContainerInfo:
    container_name: str
    tasks_executed: List[Tuple[str, str, float]]
    exceptions: List[str]
    output: List[Tuple[str, str]]
    failure: bool = False


error_counter = AtomicInteger(0)
synchronized_print = SynchronizedPrint()
synchronized_map = SynchronizedMap()
temp_dir = tempfile.TemporaryDirectory().name
synchronized_container_copy = SynchronizedContainerCopy()


def find_workloads_for_testing(container, container_root_directory, project_root_directory) -> List[Tuple[str, str]]:
    """
    E2E tests work by testing only folders which are in build directory, as all the folders are not copied there.
    Once we found all the folders in the build directory of the container (we need therefore running container as we can't get build folders from root dir),
    we can make filter to get us only workloads.yaml files whose parent directory is inside build directory (filter out the ones which are not).
    The ones which are in the build directory are the ones we want to test.

    1. This function gets all folders in the build directory of the container.
    2. Once you have all folders


    Getting workloads.yaml content with docker cp is a bit more difficult, that is why two folder paths are combined.

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
    container_to_copy, file_prefix_name, container_root_dir, formated_stdout_output, formated_stderr_output
):
    """
    This function writes formatted stdout and stderr output to temporary files.
    Afterwards it copies those files to the container_to_copy in the container_root_dir/LOGS_PATH directory.

    :param container_to_copy: Container ID to which output will be copied
    :param file_prefix_name: Name of container where output was generated
    :param container_root_dir: Path to memgraph folder in container from root
    :param formated_stdout_output: stdout output from the container
    :param formated_stderr_output:
    :return:
    """
    stdout_file_path = os.path.join(temp_dir, f"{file_prefix_name}_stdout.log")
    stderr_file_path = os.path.join(temp_dir, f"{file_prefix_name}_stderr.log")

    print(stderr_file_path)
    print(stdout_file_path)
    with open(stdout_file_path, "w") as stdout_file:
        stdout_file.write(formated_stdout_output)

    with open(stderr_file_path, "w") as stderr_file:
        stderr_file.write(formated_stderr_output)

    # Copy files to Docker container
    copy_stdout_command = f"docker cp {stdout_file_path} {container_to_copy}:{container_root_dir}{LOGS_PATH}"
    copy_stderr_command = f"docker cp {stderr_file_path} {container_to_copy}:{container_root_dir}{LOGS_PATH}"

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
    synchronized_print.print(f">>>>Starting {get_container_name(id)}")
    container = docker_handler.start_container_from_image(image_name, id)
    if container is None:
        error_counter.increment()
        synchronized_print.print(f"Failed to start container {id}")
        return

    synchronized_print.print(f">>>>Started {get_container_name(id)} with id {container.id}")

    total_execution = 0

    tasks_executed = []
    output = []
    failure = False
    exceptions = []
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
                    f"Fail! Container id: {get_container_name(id)} failed workload: {workload_name} using docker command {docker_command}"
                )
                error_counter.increment()
                failure = True
                exceptions.append(res_stdout_formatted)
                break
            else:
                synchronized_print.print(
                    f"\nSuccess! Container id: {get_container_name(id)} executed workload: {workload_name} using docker command {docker_command}\n"
                )
        except Exception as e:
            failure = True
            exceptions.append(str(e))
            synchronized_print.print(f"Exception: {e}")
            error_counter.increment()
            break

    try:
        synchronized_print.print(f">>>>Copying logs from {get_container_name(id)}")
        thread_safe_container_copy.copy_logs(
            container.id, original_container_id, f"{container_root_dir}{LOGS_PATH}", temp_dir
        )
        synchronized_print.print(f">>>>Copied logs from {get_container_name(id)}")
    except Exception as e:
        failure = True
        exceptions.append(str(e))
        error_counter.increment()
        synchronized_print.print(f"Exception occurred while copying logs from {get_container_name(id)}: {e}")

    synchronized_print.print(f">>>>Stopping {get_container_name(id)}")
    docker_handler.stop_container(container)
    synchronized_print.print(f">>>>Stopped {get_container_name(id)}")

    tasks_executed.sort(key=lambda x: x[2], reverse=True)
    synchronized_map.insert_elem(
        id,
        ContainerInfo(
            container_name=get_container_name(id),
            tasks_executed=tasks_executed,
            exceptions=exceptions,
            output=output,
            failure=failure,
        ),
    )


def parse_arguments():
    parser = argparse.ArgumentParser(description="Parse arguments for e2e tests")
    parser.add_argument(
        "--threads", type=int, required=True, help="Number of threads available to start containers and run workloads"
    )
    parser.add_argument(
        "--container-root-dir", type=str, required=True, help="Path to Memgraph folder in the container"
    )
    parser.add_argument(
        "--setup-command", type=str, required=True, help="Command to execute before running workload in container"
    )
    parser.add_argument(
        "--project-root-dir", type=str, required=True, help="Path to Memgraph folder in project in GitHub"
    )
    parser.add_argument(
        "--original-container-id",
        type=str,
        required=True,
        help="Container from which we generate image to start containers, and container to which we copy logs to once process is done",
    )

    args = parser.parse_args()
    return args


def remove_image(docker_handler, image_name, max_tries=10):
    for _ in range(max_tries):
        if not docker_handler.image_exists(image_name):
            return True
        time.sleep(1)
        docker_handler.remove_image(image_name)
    return False


def get_and_delete_containers(docker_handler):
    global CONTAINER_NAME_PREFIX

    all_containers = docker_handler.get_client().containers.list(all=True)
    deleted_containers = []

    for container in all_containers:
        if any(name.startswith(CONTAINER_NAME_PREFIX) for name in container.name.split(",")):
            try:
                container.kill()
            except Exception as _:
                pass

            try:
                container.remove_container()
            except Exception as _:
                pass
            deleted_containers.append(container.name)

    return deleted_containers


def cleanup_state(docker_handler, image_name):
    try:
        ok = remove_image(docker_handler, image_name)
        assert (ok, "Failed to remove image")
    except Exception as _:
        assert (False, "Failed to remove image")

    try:
        get_and_delete_containers(docker_handler)
    except Exception as _:
        assert (False, "Failed to remove containers")


def get_workloads_from_container(docker_handler, image_name, container_root_dir, project_root_dir):
    print(f"Starting container from image {image_name}")
    container_0 = docker_handler.start_container_from_image(image_name, 0)
    assert container_0 is not None, "Container not started!"

    # Start one container needed for finding workloads
    workloads = find_workloads_for_testing(container_0, container_root_dir, project_root_dir)
    print(f"Stopping container {container_0}")
    docker_handler.stop_container(container_0)
    print(f">>>>Workloads {len(workloads)} found!")
    return workloads


def create_image_from_container(docker_handler, original_container_id, image_name):
    print(f"Committing container {original_container_id} to {image_name}")
    ok = docker_handler.commit_image(original_container_id, image_name)

    if not ok:
        print(f"Failed to commit image {image_name}")
        exit(1)
    print(f"Committed image {image_name}")


def run_workloads_in_parallel(workloads, args, image_name) -> Tuple[dict, bool]:
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
    result = synchronized_map.reset_and_get()

    return result, error


def print_diff_summary(result, args):
    print("DIFF SUMMARY:")
    for id, container_info in result.items():
        print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print(f"\t\tContainer id: {id} \n ")

        stdout, stderr = [out[0] for out in container_info.output], [out[1] for out in container_info.output]

        formated_stdout_output = "\n".join(stdout)
        formated_stderr_output = "\n".join(stderr)

        copy_output_to_container(
            args.original_container_id,
            f"{get_container_name(id)}",
            args.container_root_dir,
            formated_stdout_output,
            formated_stderr_output,
        )

        total_time = sum([x[2] for x in container_info.tasks_executed])
        print(
            f"\t\t{get_container_name(id)} executed {len(container_info.tasks_executed)} tasks in {total_time} seconds."
        )
        for task in container_info.tasks_executed:
            print(f"\t\t\t {task}")

        if container_info.exception:
            print(f"\t\t{get_container_name(id)} failed with exception: {container_info.exception}")

        print(f">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print("END OF DIFF SUMMARY")


def print_errors_summary(result):
    print("ERRORS SUMMARY:")
    if error_counter.get() > 0:
        print(f"Errors occurred {error_counter.get()}")
        for id, container_info in result.items():
            if container_info.failure:
                print(f"{get_container_name(id)} failed.")

        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        for id, container_info in result.items():
            if container_info.failure:
                exceptions = "\n\t >>>".join(container_info.exceptions)
                print(f"{get_container_name(id)} exception:\n {exceptions}.")
                print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    print("END OF ERRORS SUMMARY")


def main():
    error = False
    docker_handler = DockerHandler()
    args = parse_arguments()
    image_name = f"{args.original_container_id}-e2e"

    cleanup_state(docker_handler, image_name)

    try:
        create_image_from_container(docker_handler, args.original_container_id, image_name)
    except Exception as e:
        print(f"Exception occurred: {e}")
        error = True

    if error:
        cleanup_state(docker_handler, image_name)
        exit(1)

    workloads = []
    try:
        workloads = get_workloads_from_container(
            docker_handler, image_name, args.container_root_dir, args.project_root_dir
        )
    except Exception as e:
        print(f"Exception occurred: {e}")
        error = True

    if error:
        cleanup_state(docker_handler, image_name)
        exit(1)

    result = None
    try:
        result, error = run_workloads_in_parallel(workloads, args, image_name)
    except Exception as e:
        print(f"Exception occurred: {e}")
        error = True

    if result is not None:
        try:
            print_diff_summary(result, args)
            print_errors_summary(result)
        except Exception as e:
            print(f"Exception occurred: {e}")
            error = True

    cleanup_state(docker_handler, image_name)

    if error_counter.get() > 0 or error:
        exit(1)


main()
