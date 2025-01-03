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

        self._data_config = [
            {
                "id": 1,
                "bolt-port": 7687,
                "management-port": 13011,
                "monitoring-port": 7444,
                "metrics-port": 9091,
            },
            {
                "id": 2,
                "bolt-port": 7688,
                "management-port": 13012,
                "monitoring-port": 7445,
                "metrics-port": 9092,
            },
            {
                "id": 3,
                "bolt-port": 7689,
                "management-port": 13013,
                "monitoring-port": 7446,
                "metrics-port": 9093,
            },
        ]

        self._coord_config = [
            {
                "id": 1,
                "bolt-port": 7691,
                "management-port": 12121,
                "coordinator-port": 10111,
                "monitoring-port": 7447,
                "metrics-port": 9094,
            },
            {
                "id": 2,
                "bolt-port": 7692,
                "management-port": 12122,
                "coordinator-port": 10112,
                "monitoring-port": 7448,
                "metrics-port": 9095,
            },
            {
                "id": 3,
                "bolt-port": 7693,
                "management-port": 12123,
                "coordinator-port": 10113,
                "monitoring-port": 7449,
                "metrics-port": 9096,
            },
        ]

    def start_memgraph(self, additional_flags: List[str] = None):
        """Starts Memgraph and return the process"""
        cwd = os.path.dirname(self._memgraph)

        flags = [] if additional_flags is None else additional_flags

        for data_config in self._data_config:
            cmd = [self._memgraph] + self._flags + flags + self._get_data_instance_ha_flags(data_config)
            proc = subprocess.Popen(cmd, cwd=cwd, env=os.environ.copy())
            self.wait_for_server(data_config["bolt-port"])

            assert proc.poll() is None, "The database binary died prematurely!"
            self._data_proc.append(proc)

        for coord_config in self._coord_config:
            cmd = [self._memgraph] + self._get_coordinator_instance_ha_flags(coord_config)
            proc = subprocess.Popen(cmd, cwd=cwd, env=os.environ.copy())
            self.wait_for_server(coord_config["bolt-port"])

            assert proc.poll() is None, "The database binary died prematurely!"
            self._coord_proc.append(proc)

        memgraph = Memgraph(port=self._coord_config[0]["bolt-port"])
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

    def _get_data_instance_ha_flags(self, data_config):
        return [
            f"--management_port={data_config['management-port']}",
            f"--bolt-port={data_config['bolt-port']}",
            f"--monitoring-port={data_config['monitoring-port']}",
            f"--metrics-port={data_config['metrics-port']}",
            f"--data-directory=mg_data_{data_config['id']}",
            f"--log-file=mg_data_{data_config['id']}.log",
            f"--replication-restore-state-on-startup=false",
            f"--data-recovery-on-startup=false",
        ]

    def _get_coordinator_instance_ha_flags(self, coord_config):
        return [
            f"--coordinator-hostname=127.0.0.1",
            f"--coordinator-id={coord_config['id']}",
            f"--bolt-port={coord_config['bolt-port']}",
            f"--management-port={coord_config['management-port']}",
            f"--coordinator-port={coord_config['coordinator-port']}",
            f"--monitoring-port={coord_config['monitoring-port']}",
            f"--metrics-port={coord_config['metrics-port']}",
            f"--data-directory=mg_coord_{coord_config['id']}",
            f"--log-file=mg_coord_{coord_config['id']}.log",
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
