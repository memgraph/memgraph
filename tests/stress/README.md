# Memgraph Stress Test Suite

## Overview
Stress test suite for evaluating Memgraph performance and stability under various workloads.

## Quick Start

### Run All Workloads for a Deployment Type
```sh
./continuous_integration --deployment ha/docker
./continuous_integration --deployment standalone/native
./continuous_integration --deployment ha/native
./continuous_integration --deployment ha/eks
```

### Run a Specific Workload
```sh
./continuous_integration --workload ha/native/show_replicas_deadlock.yaml
```

## Workload Registry

Each deployment folder has a `workloads.yaml` file that registers which workloads to run:

```yaml
# ha/docker/workloads.yaml
workloads:
  - config: workloads/rag/workload.yaml
    enabled: true

  - config: workloads/other/workload.yaml
    enabled: false  # Skipped
```

### Adding a New Workload

1. Create folder with workload config: `ha/workloads/my_workload.yaml`
2. Register in `workloads.yaml`: `- config: ../workloads/my_workload.yaml`
3. Run: `./continuous_integration --deployment ha/docker`

To skip a workload, set `enabled: false` in the registry.

## Configuration
The test suite is configured using YAML files, which define Memgraph deployment script, general options, dataset specifications, and custom workloads. Below is a breakdown of the configuration parameters:

### 1. Memgraph Configuration
```yaml
memgraph:
  deployment:
    # If true, cluster is managed externally (e.g., EKS) - skip start/stop.
    # If false, CI will run <deployment_type>/deployment/deployment.sh start/stop.
    externally_managed: false
  args:
    # Additional memgraph arguments that are passed. Overrides the arguments from the
    # deployment script.
    - "--telemetry-enabled=false"  # Disables telemetry.
    - "--bolt-server-name-for-init=Neo4j/"  # Specifies the Bolt server name.
    - "--log-file=stress_test.log"  # Log file location.
    - "--data-directory=stress_data"  # Directory for storing data.
  env:
    # (Optional) Environment variables passed to the deployment script.
    # Required for HA deployments which need an enterprise license.
    # If value is empty, the variable will be inherited from the shell environment.
    MEMGRAPH_ENTERPRISE_LICENSE: "<license-key>"
    MEMGRAPH_ORGANIZATION_NAME: "<org-name>"
    # Enable Prometheus monitoring (for ha/docker deployments)
    ENABLE_MONITORING: "true"  # Starts Prometheus exporter on port 9100
```

### 2. Cluster Configuration (HA Deployments)
```yaml
# Optional: Required for HA deployments to define all cluster instances
cluster:
  coordinators:
    - name: <string>  # Unique coordinator name (e.g., "coord-1")
      host: <string>  # Coordinator host/IP
      bolt_port: <int>  # Coordinator Bolt port
    # ... additional coordinators
  data_instances:
    - name: <string>  # Unique data instance name (e.g., "data-1")
      host: <string>  # Data instance host/IP
      bolt_port: <int>  # Data instance Bolt port
    # ... additional data instances
```

### 3. General Configuration
```yaml
general:
  verbose: <true|false>  # Enables verbose logging.
  use_ssl: <true|false>  # Enables SSL.
```

### 4. Custom Workloads
Custom workloads can be defined using either **inline workers** or a **custom Python script**.

#### Option A: Inline Workers
```yaml
customWorkloads:
  tests:
    - name: <string>  # Unique workload name.
      memgraph_args: []
      # Additional Memgraph arguments specific to the workload.
      # Doesn't apply for K8s as they use values file.
      import:
        queries: ["<Cypher Query>"]  # Queries to execute for data import.
      querying:
        host: "localhost"  # Connection host, default: "localhost"
        port: 7687  # Connection port, default: 7687
      workers:
        - name: <string>  # Unique worker name.
          type: <string>
          # Worker types: reader, writer, lab-simulator
          query: "<Cypher Query>"  # Cypher query executed by the worker.
          querying:  # Optional: override workload-level querying
            host: "localhost"
            port: 7687
          num_repetitions: <int>  # Number of times the query should be executed.
          sleep_millis: <int>  # Sleep duration in milliseconds between executions.
          step: <int>  # (Optional) Step number for phased executions.
      timeout_min: <int>  # Maximum execution time in minutes.
```

#### Option B: Custom Python Script
Instead of defining workers inline, you can specify a Python script that handles the workload logic.
```yaml
customWorkloads:
  tests:
    - name: <string>  # Unique workload name.
      memgraph_args: []  # Additional Memgraph arguments.
      script: "<path>"  # Path to Python script (relative to stress/ directory or absolute).
      timeout_min: <int>  # Maximum execution time in minutes.
```

**Script requirements:**
- The script should exit with code 0 on success, non-zero on failure.

## Running the Test Suite

**Note:** HA deployments require an enterprise license:
```sh
export MEMGRAPH_ENTERPRISE_LICENSE="your-license-key"
export MEMGRAPH_ORGANIZATION_NAME="your-org-name"
```

Examples:
```sh
# Run by deployment type
./continuous_integration --deployment ha/docker
./continuous_integration --deployment ha/native

# Run specific workload
./continuous_integration --workload standalone/native/workloads/config_small.yaml
```

## Monitoring with Prometheus & Grafana

For Docker HA deployments, enable monitoring with `ENABLE_MONITORING=true`:

```sh
ENABLE_MONITORING=true ./ha/docker/deployment/deployment.sh start
```

This starts:
- **Prometheus exporter:** `http://localhost:9100`
- **Grafana:** `http://localhost:3000` (login: `admin`/`admin`)

## EKS Deployment

Use `ha/eks/deployment/deployment.sh` to deploy Memgraph HA on AWS EKS.

### Prerequisites

- AWS CLI, eksctl, kubectl, Helm

### Quick Start

```sh
cd ha/eks/deployment

# Configure license in values.yaml, then:
./deployment.sh start    # Create cluster and deploy
./deployment.sh status   # Check status
./deployment.sh stop     # Stop Memgraph (keeps cluster)
./deployment.sh destroy  # Destroy entire cluster
```

### Commands

| Command | Description |
|---------|-------------|
| `start` | Create EKS cluster and deploy Memgraph HA |
| `stop` | Uninstall Memgraph (keeps cluster) |
| `destroy` | Delete entire EKS cluster |
| `status` | Show cluster and pod status |
| `port-forward <type> <port>` | Forward port to coordinator/data pod |
| `logs <pod>` | View pod logs |
| `export-metrics [file]` | Export Prometheus metrics to JSON |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLUSTER_NAME` | `test-cluster-ha` | EKS cluster name |
| `CLUSTER_REGION` | `eu-west-1` | AWS region |
| `HELM_RELEASE_NAME` | `mem-ha-test` | Helm release name |
