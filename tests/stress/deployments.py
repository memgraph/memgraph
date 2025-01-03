import os
import subprocess
import time
from abc import ABC
from typing import List

from gqlalchemy import Memgraph


class Deployment(ABC):
    def start_memgraph(self, additional_flags: List[str] = None):
        pass

    def stop_memgraph(self) -> None:
        pass

    def cleanup(self) -> None:
        pass

    def wait_for_server(self, port) -> None:
        pass

    def execute_query(self, query: str) -> None:
        memgraph = Memgraph()
        memgraph.execute(query)


class DefaultStandaloneDeployment(Deployment):
    def __init__(self, memgraph: str, flags: List[str]):
        super().__init__()
        self._memgraph = memgraph
        self._flags = flags

    def start_memgraph(self, additional_flags: List[str] = None):
        """Starts Memgraph and return the process"""
        cwd = os.path.dirname(self._memgraph)

        flags = [] if additional_flags is None else additional_flags

        cmd = [self._memgraph] + self._flags + flags
        proc = subprocess.Popen(cmd, cwd=cwd)
        self.wait_for_server(7687)

        assert proc.poll() is None, "The database binary died prematurely!"

        self._memgraph_proc = proc

    def stop_memgraph(self) -> None:
        self._memgraph_proc.terminate()
        ret_mg = self._memgraph_proc.wait()
        if ret_mg != 0:
            raise Exception("Memgraph binary returned non-zero ({})!".format(ret_mg))

    def cleanup(self):
        if self._memgraph_proc.poll() != None:
            return
        self._memgraph_proc.kill()
        self._memgraph_proc.wait()

    def wait_for_server(self, port) -> None:
        cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
        while subprocess.call(cmd) != 0:
            time.sleep(0.5)
        time.sleep(2)


class DefaultHADeployment(Deployment):
    def __init__(self, memgraph: str, flags: List[str]):
        super().__init__()
        self._memgraph = memgraph
        self._flags = flags
        self._data_proc = []
        self._coord_proc = []

    def start_memgraph(self, additional_flags: List[str] = None):
        """Starts Memgraph and return the process"""
        cwd = os.path.dirname(self._memgraph)

        flags = [] if additional_flags is None else additional_flags

        for data_id in [0, 1, 2]:
            cmd = [self._memgraph] + self._flags + flags + self._get_data_instance_ha_flags(data_id)
            proc = subprocess.Popen(cmd, cwd=cwd, env=os.environ.copy())
            self.wait_for_server(7687 + data_id)

            assert proc.poll() is None, "The database binary died prematurely!"
            self._data_proc.append(proc)

        for coordinator_id in [1, 2, 3]:
            cmd = [self._memgraph] + self._get_coordinator_instance_ha_flags(coordinator_id)
            proc = subprocess.Popen(cmd, cwd=cwd, env=os.environ.copy())
            self.wait_for_server(7690 + coordinator_id)

            assert proc.poll() is None, "The database binary died prematurely!"
            self._coord_proc.append(proc)

        memgraph = Memgraph(port=7691)
        memgraph.execute(
            'ADD COORDINATOR 2 WITH CONFIG {"bolt_server": "localhost:7692", "coordinator_server": "localhost:10112", "management_server": "localhost:12122"};'
        )
        memgraph.execute(
            'ADD COORDINATOR 3 WITH CONFIG {"bolt_server": "localhost:7693", "coordinator_server": "localhost:10113", "management_server": "localhost:12123"};'
        )
        memgraph.execute(
            'REGISTER INSTANCE instance_1 WITH CONFIG {"bolt_server": "localhost:7687", "management_server": "localhost:13011", "replication_server": "localhost:10001"};'
        )
        memgraph.execute(
            'REGISTER INSTANCE instance_2 WITH CONFIG {"bolt_server": "localhost:7688", "management_server": "localhost:13012", "replication_server": "localhost:10002"};'
        )
        memgraph.execute(
            'REGISTER INSTANCE instance_3 WITH CONFIG {"bolt_server": "localhost:7689", "management_server": "localhost:13013", "replication_server": "localhost:10003"};'
        )
        memgraph.execute("SET INSTANCE instance_1 TO MAIN;")

    def stop_memgraph(self) -> None:
        for proc in self._data_proc + self._coord_proc:
            proc.terminate()
            ret_mg = proc.wait()
            if ret_mg != 0:
                raise Exception("Memgraph binary returned non-zero ({})!".format(ret_mg))

    def cleanup(self):
        for proc in self._data_proc + self._coord_proc:
            if proc.poll() != None:
                return
            proc.kill()
            proc.wait()

    def wait_for_server(self, port) -> None:
        cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
        while subprocess.call(cmd) != 0:
            time.sleep(0.5)
        time.sleep(2)

    def _get_data_instance_ha_flags(flags, data_id: int):
        return [
            f"--management_port={13011 + data_id}",
            f"--bolt-port={7687 + data_id}",
            f"--monitoring-port={7444 + data_id}",
            f"--metrics-port={9091 + data_id}",
            f"--data-directory=mg_data_{data_id}",
            f"--log-file=stress_test_data_{data_id}.log",
            f"--replication-restore-state-on-startup=false",
            f"--data-recovery-on-startup=false",
        ]

    def _get_coordinator_instance_ha_flags(flags, coord_id: int):
        return [
            f"--coordinator-hostname=127.0.0.1",
            f"--coordinator-id={coord_id}",
            f"--bolt-port={7690 + coord_id}",
            f"--management-port={12120 + coord_id}",
            f"--coordinator-port={10110 + coord_id}",
            f"--data-directory=mg_coord_{coord_id}",
            f"--log-file=stress_test_coord_{coord_id}.log",
            f"--log-level=DEBUG",
            f"--also-log-to-stderr=true",
            f"--replication-restore-state-on-startup=false",
            f"--data-recovery-on-startup=false",
        ]


class DockerStandaloneDeployment(Deployment):
    def __init__(self, image: str, tag: str, flags: List[str]):
        super().__init__()
        self._image = image
        self._tag = tag
        self._container_name = "mg_stress"
        self._flags = flags

    def start_memgraph(self, additional_flags: List[str] = None) -> None:
        """Starts Memgraph in a Docker container and returns the process."""
        # Construct the docker run command

        flags = [] if additional_flags is None else additional_flags
        cmd = (
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                self._container_name,
                "-p",
                "7687:7687",
            ]
            + [f"{self._image}:{self._tag}"]
            + self._flags
            + flags
        )

        # Start the Docker container
        subprocess.run(cmd, check=True)
        self.wait_for_server(7687)

        # Check if the container is running
        status_cmd = ["docker", "ps", "--filter", f"name={self._container_name}", "--format", "{{.ID}}"]
        container_id = subprocess.check_output(status_cmd).decode("utf-8").strip()

        assert container_id, "The Memgraph Docker container failed to start!"

        self._container_id = container_id

    def stop_memgraph(self) -> None:
        """Stops and removes the Memgraph Docker container."""
        # Stop the Docker container
        cmd = ["docker", "stop", self._container_id]
        ret = subprocess.run(cmd)
        if ret.returncode != 0:
            raise Exception(f"Failed to stop Docker container ({self._container_id})!")

    def cleanup(self) -> None:
        """Cleans up the Docker container if it is still running."""
        try:
            # Check if the container is running
            status_cmd = ["docker", "ps", "--filter", f"name={self._container_name}", "--format", "{{.ID}}"]
            container_id = subprocess.check_output(status_cmd).decode("utf-8").strip()

            if container_id:
                # Stop and remove the container
                self.stop_memgraph(container_id)
        except subprocess.CalledProcessError:
            # Ignore errors if the container is already stopped or does not exist
            pass

    def wait_for_server(self, port) -> None:
        cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
        while subprocess.call(cmd) != 0:
            print(f"Waiting for server on host port {port}...")
            time.sleep(0.5)
        time.sleep(2)
        print(f"Server is now accessible on host port {port}.")
