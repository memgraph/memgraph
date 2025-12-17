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
    script: <path to script>
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
