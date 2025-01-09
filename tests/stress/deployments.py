import os
import shutil
import subprocess
import time
from abc import ABC
from typing import List

import yaml
from gqlalchemy import Memgraph

# paths
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(BASE_DIR, "build")


class Deployment(ABC):
    def start_memgraph(self, additional_flags: List[str] = None):
        pass

    def stop_memgraph(self) -> None:
        pass

    def cleanup(self) -> None:
        pass

    def wait_for_server(self, port=None) -> None:
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
        """Terminate all processes and clean up data directories."""
        # Stop running processes
        for proc in self._data_proc + self._coord_proc:
            if proc.poll() is None:
                proc.terminate()
                proc.wait()

        # Remove data directories and log files from the build directory
        for data_config in self._data_config:
            data_dir = os.path.join(BUILD_DIR, f"mg_data_{data_config['id']}")
            log_file = os.path.join(BUILD_DIR, f"mg_data_{data_config['id']}.log")
            if os.path.exists(data_dir):
                shutil.rmtree(data_dir)
                print(f"Removed data directory: {data_dir}")
            if os.path.exists(log_file):
                os.remove(log_file)
                print(f"Removed log file: {log_file}")

        for coord_config in self._coord_config:
            coord_dir = os.path.join(BUILD_DIR, f"mg_coord_{coord_config['id']}")
            log_file = os.path.join(BUILD_DIR, f"mg_coord_{coord_config['id']}.log")
            if os.path.exists(coord_dir):
                shutil.rmtree(coord_dir)
                print(f"Removed coordinator directory: {coord_dir}")
            if os.path.exists(log_file):
                os.remove(log_file)
                print(f"Removed coordinator log file: {log_file}")

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


class MinikubeHelmStandaloneDeployment(Deployment):
    def __init__(self, release_name: str, chart_name: str, values_path: str):
        super().__init__()
        self.release_name = release_name
        self.chart_name = chart_name
        self.values_path = values_path
        self._memgraph_pod = None

    def start_minikube(self) -> None:
        """Start Minikube if it is not already running"""
        print("Checking Minikube status...")
        status_cmd = ["minikube", "status", "--format", "{{.Host}}"]
        try:
            minikube_status = subprocess.check_output(status_cmd).decode("utf-8").strip()
            if minikube_status == "Running":
                print("Minikube is already running. Cleaning up for a fresh minikube start.")
                self.delete_minikube()

            print("Starting Minikube...")
            subprocess.run(["minikube", "start"], check=True)
        except subprocess.CalledProcessError:
            print("Minikube is not running. Starting Minikube...")
            subprocess.run(["minikube", "start"], check=True)

    def delete_minikube(self) -> None:
        """Deletes the Minikube cluster"""
        print("Deleting Minikube cluster...")
        subprocess.run(["minikube", "delete"], check=True)

    def start_memgraph(self, additional_flags: List[str] = None):
        """Deploys Memgraph using Helm on Minikube"""
        # Start Minikube if it's not running
        self.start_minikube()
        print("Started minikube!")

        # Set the appropriate namespace for Helm release (default)
        self._set_k8s_context()
        print("Set K8s context!")

        # Prepare the Helm command
        helm_cmd = ["helm", "install", self.release_name, self.chart_name, "-f", self.values_path]

        # Execute the Helm command to install Memgraph
        subprocess.run(helm_cmd, check=True)
        print("Helm charts installed!")

        # Wait for Memgraph to be fully deployed
        self.wait_for_server()
        print("Server ready!")

    def stop_memgraph(self) -> None:
        """Uninstalls Memgraph using Helm"""
        helm_cmd = ["helm", "uninstall", self.release_name]
        subprocess.run(helm_cmd, check=True)

    def cleanup(self):
        # Delete Minikube cluster after cleanup
        self.delete_minikube()

    def wait_for_server(self) -> None:
        """Wait for Memgraph pod to be running and ready"""
        print("Waiting for Memgraph pod to be running and ready...")
        while True:
            try:
                # Get the pod name
                pod_name_cmd = [
                    "kubectl",
                    "get",
                    "pods",
                    "-l",
                    f"app.kubernetes.io/instance={self.release_name}",
                    "-o",
                    "jsonpath='{.items[0].metadata.name}'",
                ]
                pod_name = subprocess.check_output(pod_name_cmd).decode("utf-8").strip().strip("'")

                # Get the pod status
                pod_status_cmd = ["kubectl", "get", "pod", pod_name, "-o", "jsonpath='{.status.phase}'"]
                pod_status = subprocess.check_output(pod_status_cmd).decode("utf-8").strip().strip("'")

                # Get the container readiness (1/1)
                container_status_cmd = [
                    "kubectl",
                    "get",
                    "pod",
                    pod_name,
                    "-o",
                    "jsonpath='{.status.containerStatuses[0].ready}'",
                ]
                container_status = subprocess.check_output(container_status_cmd).decode("utf-8").strip().strip("'")

                if pod_status == "Running" and container_status == "true":
                    print(f"Pod {pod_name} is running and ready!")
                    break
                else:
                    print(f"Pod {pod_name} not ready yet, status: {pod_status}, container ready: {container_status}")
                    time.sleep(5)
            except Exception as e:
                print(f"Pod not ready yet!")
                time.sleep(1)

    def execute_query(self, query: str) -> None:
        """Execute a Cypher query on Memgraph"""
        # Get Minikube IP and NodePort
        minikube_ip = self.get_minikube_ip()
        if not minikube_ip:
            print("Failed to get Minikube IP.")
            return

        nodeport = self.get_service_nodeport(self.release_name)
        if not nodeport:
            print("Failed to get NodePort for Memgraph service.")
            return

        # Connect to Memgraph using the Minikube IP and NodePort
        memgraph = Memgraph(host=minikube_ip, port=nodeport)

        # Execute the Cypher query
        memgraph.execute(query)

    def get_minikube_ip(self) -> str:
        """Get Minikube external IP."""
        try:
            # Run minikube ip command to get the Minikube external IP
            minikube_ip = subprocess.check_output(["minikube", "ip"]).decode("utf-8").strip()
            return minikube_ip
        except subprocess.CalledProcessError as e:
            print(f"Error getting Minikube IP: {e}")
            return None

    def get_service_nodeport(self, service_name: str) -> int:
        """Get the NodePort for the Memgraph service."""
        try:
            # Run kubectl get svc to get the service details and extract the NodePort
            command = ["kubectl", "get", "svc", service_name, "-o", "jsonpath='{.spec.ports[0].nodePort}'"]
            nodeport = subprocess.check_output(command).decode("utf-8").strip().strip("'")
            return int(nodeport)
        except subprocess.CalledProcessError as e:
            print(f"Error getting NodePort for service {service_name}: {e}")
            return None

    def _set_k8s_context(self):
        """Set Kubernetes context to Minikube's current environment"""
        subprocess.run(["kubectl", "config", "use-context", "minikube"], check=True)


class MinikubeHelmHADeployment(Deployment):
    def __init__(self, release_name: str, chart_name: str, values_path: str):
        super().__init__()
        self.release_name = release_name
        self.chart_name = chart_name
        self.values_path = values_path
        self._memgraph_pod = None

        ha_config = yaml.safe_load(open(values_path))
        self._data_ids = [x["id"] for x in ha_config.get("data", [])]
        self._coordinator_ids = [x["id"] for x in ha_config.get("coordinators", [])]
        self._data_service_names = [f"memgraph-data-{x}" for x in self._data_ids]
        self._coord_service_names = [f"memgraph-coordinator-{x}" for x in self._coordinator_ids]
        self._data_ext_service_names = [f"memgraph-data-{x}-external" for x in self._data_ids]
        self._coord_ext_service_names = [f"memgraph-coordinator-{x}-external" for x in self._coordinator_ids]

    def start_minikube(self) -> None:
        """Start Minikube if it is not already running"""
        print("Checking Minikube status...")
        status_cmd = ["minikube", "status", "--format", "{{.Host}}"]
        try:
            minikube_status = subprocess.check_output(status_cmd).decode("utf-8").strip()
            if minikube_status == "Running":
                print("Minikube is already running. Cleaning up for a fresh minikube start.")
                self.delete_minikube()

            print("Starting Minikube...")
            subprocess.run(["minikube", "start"], check=True)
        except subprocess.CalledProcessError:
            print("Minikube is not running. Starting Minikube...")
            subprocess.run(["minikube", "start"], check=True)

    def delete_minikube(self) -> None:
        """Deletes the Minikube cluster"""
        print("Deleting Minikube cluster...")
        subprocess.run(["minikube", "delete"], check=True)

    def start_memgraph(self, additional_flags: List[str] = None):
        """Deploys Memgraph using Helm on Minikube"""
        # Start Minikube if it's not running
        self.start_minikube()
        print("Started minikube!")

        # Set the appropriate namespace for Helm release (default)
        self._set_k8s_context()
        print("Set K8s context!")

        # Prepare the Helm command
        helm_cmd = ["helm", "install", self.release_name, self.chart_name, "-f", self.values_path]

        # Execute the Helm command to install Memgraph
        subprocess.run(helm_cmd, check=True)
        print("Helm charts installed!")

        # Wait for Memgraph to be fully deployed
        self.wait_for_server()
        print("Server ready!")

        self.setup_ha()

    def setup_ha(self) -> None:
        coordinator_service_name = self._coord_ext_service_names[0]
        minikube_ip = self.get_minikube_ip()

        coordinator_2_nodeport = self.get_service_nodeport(self._coord_ext_service_names[1])
        self._execute_query_on_service(
            coordinator_service_name,
            f'ADD COORDINATOR 2 WITH CONFIG {{"bolt_server": "{minikube_ip}:{coordinator_2_nodeport}", "coordinator_server": "{self._coord_service_names[1]}.default.svc.cluster.local:12000", "management_server": "{self._coord_service_names[1]}.default.svc.cluster.local:10000"}};',
        )

        coordinator_3_nodeport = self.get_service_nodeport(self._coord_ext_service_names[2])
        self._execute_query_on_service(
            coordinator_service_name,
            f'ADD COORDINATOR 3 WITH CONFIG {{"bolt_server": "{minikube_ip}:{coordinator_3_nodeport}", "coordinator_server": "{self._coord_service_names[2]}.default.svc.cluster.local:12000", "management_server": "{self._coord_service_names[2]}.default.svc.cluster.local:10000"}};',
        )

        data_0_nodeport = self.get_service_nodeport(self._data_ext_service_names[0])
        self._execute_query_on_service(
            coordinator_service_name,
            f'REGISTER INSTANCE instance_1 WITH CONFIG {{"bolt_server": "{minikube_ip}:{data_0_nodeport}", "management_server": "{self._data_service_names[0]}.default.svc.cluster.local:10000", "replication_server": "{self._data_service_names[0]}.default.svc.cluster.local:20000"}};',
        )

        data_1_nodeport = self.get_service_nodeport(self._data_ext_service_names[1])
        self._execute_query_on_service(
            coordinator_service_name,
            f'REGISTER INSTANCE instance_2 WITH CONFIG {{"bolt_server": "{minikube_ip}:{data_1_nodeport}", "management_server": "{self._data_service_names[1]}.default.svc.cluster.local:10000", "replication_server": "{self._data_service_names[1]}.default.svc.cluster.local:20000"}};',
        )

        data_2_nodeport = self.get_service_nodeport(self._data_ext_service_names[2])
        self._execute_query_on_service(
            coordinator_service_name,
            f'REGISTER INSTANCE instance_3 WITH CONFIG {{"bolt_server": "{minikube_ip}:{data_2_nodeport}", "management_server": "{self._data_service_names[2]}.default.svc.cluster.local:10000", "replication_server": "{self._data_service_names[2]}.default.svc.cluster.local:20000"}};',
        )
        self._execute_query_on_service(coordinator_service_name, "SET INSTANCE instance_1 TO MAIN;")

    def stop_memgraph(self) -> None:
        """Uninstalls Memgraph using Helm"""
        helm_cmd = ["helm", "uninstall", self.release_name]
        subprocess.run(helm_cmd, check=True)

    def cleanup(self):
        # Delete Minikube cluster after cleanup
        self.delete_minikube()

    def wait_for_server(self) -> None:
        """Wait for Memgraph pod to be running and ready"""
        print("Waiting for Memgraph pods to be running and ready...")
        instances = [f"memgraph-data-{x}" for x in self._data_ids] + [
            f"memgraph-coordinator-{x}" for x in self._coordinator_ids
        ]

        for instance in instances:
            while True:
                try:
                    # Get the pod name
                    pod_name_cmd = [
                        "kubectl",
                        "get",
                        "pods",
                        "-l",
                        f"app={instance}",
                        "-o",
                        "jsonpath='{.items[0].metadata.name}'",
                    ]
                    pod_name = subprocess.check_output(pod_name_cmd).decode("utf-8").strip().strip("'")

                    # Get the pod status
                    pod_status_cmd = ["kubectl", "get", "pod", pod_name, "-o", "jsonpath='{.status.phase}'"]
                    pod_status = subprocess.check_output(pod_status_cmd).decode("utf-8").strip().strip("'")

                    # Get the container readiness (1/1)
                    container_status_cmd = [
                        "kubectl",
                        "get",
                        "pod",
                        pod_name,
                        "-o",
                        "jsonpath='{.status.containerStatuses[0].ready}'",
                    ]
                    container_status = subprocess.check_output(container_status_cmd).decode("utf-8").strip().strip("'")

                    if pod_status == "Running" and container_status == "true":
                        print(f"Pod {pod_name} is running and ready!")
                        break
                    else:
                        print(
                            f"Pod {pod_name} not ready yet, status: {pod_status}, container ready: {container_status}"
                        )
                        time.sleep(5)

                except Exception as e:
                    print(f"Pod not ready yet!")
                    time.sleep(1)

        print("All pods ready!")

    def execute_query(self, query: str) -> None:
        """Execute a Cypher query on Memgraph"""
        # Get Minikube IP and NodePort
        minikube_ip = self.get_minikube_ip()
        if not minikube_ip:
            print("Failed to get Minikube IP.")
            return

        nodeport = self.get_service_nodeport(self._data_ext_service_names[0])
        if not nodeport:
            print("Failed to get NodePort for Memgraph service.")
            return

        # Connect to Memgraph using the Minikube IP and NodePort
        memgraph = Memgraph(host=minikube_ip, port=nodeport)

        # Execute the Cypher query
        memgraph.execute(query)

    def _execute_query_on_service(self, service_name: str, query: str):
        minikube_ip = self.get_minikube_ip()
        if not minikube_ip:
            print("Failed to get Minikube IP.")
            return

        nodeport = self.get_service_nodeport(service_name)
        if not nodeport:
            print("Failed to get NodePort for Memgraph service.")
            return

        # Connect to Memgraph using the Minikube IP and NodePort
        memgraph = Memgraph(host=minikube_ip, port=nodeport)

        # Execute the Cypher query
        memgraph.execute(query)

    def get_minikube_ip(self) -> str:
        """Get Minikube external IP."""
        try:
            # Run minikube ip command to get the Minikube external IP
            minikube_ip = subprocess.check_output(["minikube", "ip"]).decode("utf-8").strip()
            return minikube_ip
        except subprocess.CalledProcessError as e:
            print(f"Error getting Minikube IP: {e}")
            return None

    def get_service_nodeport(self, service_name: str) -> int:
        """Get the NodePort for the Memgraph service."""
        try:
            # Run kubectl get svc to get the service details and extract the NodePort
            command = ["kubectl", "get", "svc", service_name, "-o", "jsonpath='{.spec.ports[0].nodePort}'"]
            nodeport = subprocess.check_output(command).decode("utf-8").strip().strip("'")
            return int(nodeport)
        except subprocess.CalledProcessError as e:
            print(f"Error getting NodePort for service {service_name}: {e}")
            return None

    def _set_k8s_context(self):
        """Set Kubernetes context to Minikube's current environment"""
        subprocess.run(["kubectl", "config", "use-context", "minikube"], check=True)
