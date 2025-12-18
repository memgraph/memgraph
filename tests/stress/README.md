# Memgraph Stress Test Suite

## Overview
This stress test suite is designed to evaluate the performance, stability, and reliability of Memgraph deployments under various workloads. The configuration allows testing different deployments, and query workloads to ensure Memgraph can handle real-world production scenarios.

## Configuration
The test suite is configured using YAML files, which define Memgraph deployment script, general options, dataset specifications, and custom workloads. Below is a breakdown of the configuration parameters:

### 1. Memgraph Configuration
```yaml
memgraph:
  deployment:
    # Specifies the path to the deployment script
    # Deployment script needs to setup and cleanup the Memgraph ecosystem it is run on.
    # Script needs to have methods for start, stop and status, since continuous integration
    # calls the script with these arguments. Optionally, the script can have an additional
    # argument to pass memgraph flags to the cluster, which will override the existing cluster
    # flags. Stopping of the cluster needs to guarantee the cleanup of the resources so that
    # no files or directories are left behind. Check binary_standalone.sh for more info.
    #
    # If script is empty or not specified, the test suite assumes the cluster is managed
    # externally (e.g., K8s, cloud deployment) and will skip start/stop operations.
    script: <path to script>  # Leave empty for externally managed clusters
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
    # Enable Prometheus monitoring (for docker_ha.sh deployments)
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

### 4. Dataset Configuration
```yaml
# The dataset configuration is a legacy way to provide stress tests. For adding your own stress test
# Please check section 4. Custom Workloads
dataset:
  tests:
    - name: <script_name>  # Name of the test script (e.g., bipartite.py, detach_delete.py).
      test_args:  # Arguments passed to the legacy test scripts.
        - "<argument>"
      timeout_min: <int>  # Maximum execution time for the test in minutes.
      memgraph_args:
      # (Optional) Additional Memgraph-specific arguments.
      # Arguments are merged with the default arguments from above Memgraph configuration
        - "--flag-name=flag-value"
```

### 5. Custom Workloads
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
To run the stress test suite, ensure you have the necessary dependencies installed and execute the following command:
```sh
./continuous_integration --config-file <path-to-config.yaml>
```

### Example: Standalone Memgraph
Run the stress test suite against a standalone Memgraph instance:
```sh
./continuous_integration --config-file configurations/templates/config_small.yaml
```

### Example: High Availability Memgraph
Run the stress test suite against a High Availability (HA) Memgraph cluster.

**Note:** HA requires an enterprise license. Set the license either in the config YAML under `memgraph.env` or via shell environment variables:
```sh
export MEMGRAPH_ENTERPRISE_LICENSE="your-license-key"
export MEMGRAPH_ORGANIZATION_NAME="your-org-name"
./continuous_integration --config-file configurations/templates/config_ha.yaml
```

### Example: Externally Managed Cluster
Run workloads against an existing cluster (e.g., K8s, cloud deployment) without managing the cluster lifecycle:
```yaml
memgraph:
  deployment:
    script: ""  # Empty = cluster managed externally
```

```sh
./continuous_integration --config-file configurations/workloads/rag/vector_workload.yaml
```

The test suite will skip start/stop operations and run workloads directly against the existing cluster.

## Monitoring with Prometheus & Grafana
For Docker HA deployments (`docker_ha.sh`), you can enable the full monitoring stack by setting `ENABLE_MONITORING`:

```yaml
memgraph:
  deployment:
    script: "docker_ha.sh"
  env:
    MEMGRAPH_ENTERPRISE_LICENSE: "<license-key>"
    MEMGRAPH_ORGANIZATION_NAME: "<org-name>"
    ENABLE_MONITORING: "true"
```

When enabled, the deployment script starts:
- **[Prometheus exporter](https://github.com/memgraph/prometheus-exporter):** `http://localhost:9100`
- **Grafana dashboard:** `http://localhost:3000` (login: `admin`/`admin`)

Grafana comes pre-configured with:
- **Data source:** Memgraph Prometheus (already connected)
- **Dashboard:** "Memgraph HA Overview" with panels for:
  - Vertex/Edge counts
  - Memory usage
  - Active transactions
  - Query rate

Just open http://localhost:3000, login with `admin`/`admin`, and go to **Dashboards → Memgraph → Memgraph HA Overview**.

The exporter collects metrics from all Memgraph instances (data nodes and coordinators) including HA-specific metrics.

## EKS Deployment

For deploying Memgraph HA on AWS EKS, use the `eks_ha.sh` script located in `configurations/deployments/`.

### Prerequisites

- **AWS CLI** configured with appropriate credentials
- **eksctl** for EKS cluster management
- **kubectl** for Kubernetes operations
- **Helm** for deploying Memgraph

### Configuration Files

The EKS deployment uses configuration files in `configurations/deployments/eks/`:

| File | Description |
|------|-------------|
| `cluster.yaml` | EKS cluster configuration (node groups, instance types) |
| `values.yaml` | Helm values for Memgraph HA (license, storage, affinity) |
| `gp3-sc.yaml` | GP3 storage class for EBS volumes |

### Quick Start

1. **Configure your license** in `eks/values.yaml`:
   ```yaml
   env:
     MEMGRAPH_ENTERPRISE_LICENSE: "<your-license>"
     MEMGRAPH_ORGANIZATION_NAME: "<your-organization>"
   ```

2. **Create cluster and deploy Memgraph:**
   ```sh
   cd tests/stress/configurations/deployments
   ./eks_ha.sh start
   ```

   This will:
   - Create an EKS cluster with 3 coordinator nodes (t3.medium) and 2 data nodes (r5.large)
   - Apply GP3 storage class
   - Attach necessary IAM policies for EBS
   - Install Memgraph HA via Helm
   - Wait for all pods to be ready

3. **Check deployment status:**
   ```sh
   ./eks_ha.sh status
   ```

4. **Connect to Memgraph:**
   ```sh
   # Port forward to coordinator
   ./eks_ha.sh port-forward coordinator 7687

   # Then connect with mgconsole
   mgconsole --host 127.0.0.1 --port 7687
   ```

5. **View logs:**
   ```sh
   ./eks_ha.sh logs <pod-name>
   ```

6. **Stop Memgraph (keeps cluster):**
   ```sh
   ./eks_ha.sh stop
   ```

7. **Destroy entire cluster:**
   ```sh
   ./eks_ha.sh destroy
   ```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLUSTER_NAME` | `test-cluster-ha` | EKS cluster name |
| `CLUSTER_REGION` | `eu-west-1` | AWS region |
| `HELM_RELEASE_NAME` | `mem-ha-test` | Helm release name |
| `POD_READY_TIMEOUT` | `600` | Timeout in seconds for pods to be ready |
