import os
import subprocess
import time
from abc import ABC
from typing import List


class Deployment(ABC):
    def start_memgraph(self, flags: List[str]):
        pass

    def stop_memgraph(self) -> None:
        pass

    def cleanup(self) -> None:
        pass

    def wait_for_server(self, port) -> None:
        pass


class DefaultDeployment(Deployment):
    def __init__(self, memgraph: str):
        super().__init__()
        self.memgraph = memgraph

    def start_memgraph(self, flags: List[str]):
        """Starts Memgraph and return the process"""
        cwd = os.path.dirname(self.memgraph)

        cmd = [self.memgraph] + flags
        memgraph_proc = subprocess.Popen(cmd, cwd=cwd)
        self.wait_for_server(7687)

        assert memgraph_proc.poll() is None, "The database binary died prematurely!"

        self.memgraph_proc = memgraph_proc

    def stop_memgraph(self) -> None:
        self.memgraph_proc.terminate()
        ret_mg = self.memgraph_proc.wait()
        if ret_mg != 0:
            raise Exception("Memgraph binary returned non-zero ({})!".format(ret_mg))

    def cleanup(self):
        if self.memgraph_proc.poll() != None:
            return
        self.memgraph_proc.kill()
        self.memgraph_proc.wait()

    def wait_for_server(self, port) -> None:
        cmd = ["nc", "-z", "-w", "1", "127.0.0.1", str(port)]
        while subprocess.call(cmd) != 0:
            time.sleep(0.5)
        time.sleep(2)


class DockerDeployment(Deployment):
    def __init__(self, image: str, tag: str):
        super().__init__()
        self._image = image
        self._tag = tag
        self._container_name = "mg_stress"

    def start_memgraph(self, flags: List[str]) -> None:
        """Starts Memgraph in a Docker container and returns the process."""
        # Construct the docker run command
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
