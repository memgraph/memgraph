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
```

### 2. General Configuration
```yaml
general:
  verbose: <true|false>  # Enables verbose logging.
  use_ssl: <true|false>  # Enables SSL.
```

### 3. Dataset Configuration
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

### 4. Custom Workloads
```yaml
# This is the current way of stress testing Memgraph and should be enforced for all adding additional
# stress tests.
customWorkloads:
  tests:
    - name: <string>  # Unique workload name.
      memgraph_args: []
      # Additional Memgraph arguments specific to the workload.
      # Doesn't apply for K8s as they use values file.
      import:
        queries: ["<Cypher Query>"]  # Queries to execute for data import. Used to setup your dataset or workload.
      # Type of querying needed for workers to connect to the instance
      querying:
        # Connection host, default: "localhost"
        host: "localhost"
        # Connection port, default: 7687
        port: 7687
      workers:
        - name: <string>  # Unique worker name.
          type: <string>
          # Specifies the worker type. Workers are defined in stress/workers.py and are matched against this string
          # in the get_worker_object(worker) function
          # Worker types:
          # 1. reader -> executes read queries
          # 2. writer -> executes ingestion queries (is not different by nature from a reader, but used as a semantic distinction)
          # 3. lab-simulator -> executes a set of queries performed usually by Memgraph Lab to monitor the instance. Used for
          # users which frequently use Memgraph Lab to see if Lab is doing any instability in the database workload
          # 4. metrics -> collects metrics data via HTTP endpoint using curl. Fetches metrics every 5 seconds (configurable)
          # and stores them in an array for later analysis. Useful for monitoring system performance during stress tests.
          query: "<Cypher Query>"  # Cypher query executed by the worker.
          # Optional: if the worker is connecting in a different way from the custom workload querying.
          # If nothing is specified, the querying type will be injected from the workload.
          # Additional info: "metrics" worker will query the http endpoint based on these information provided
          querying:
            host: "localhost"
            port: 7687
          num_repetitions: <int>  # Number of times the query should be executed.
          sleep_millis: <int>  # Sleep duration in milliseconds between executions.
          replicas: <int> # Amount of workers spawned with the same configuration
          step: <int>
          # (Optional) Step number for phased executions. If nothing is specified, worker will have step of value (1), which means
          # it will be executed first. For phased execution, you can specify different non-negative integer numbers. Each step
          # will execute the set of workers that apply to the step one after other.
          metrics: <list> # Metrics that will be displayed
          # Values for metrics:
          # "duration" -> will display how long the worker took to finish
          # "throughput" -> will display the throughput in amount of queries per second after the execution is done
          # Any of the "SHOW METRICS" values can be inserted here for the "metrics" worker
      timeout_min: <int>
      # Maximum execution time for the workload in minutes. Failing to execute the workload in this amount of minutes
      # will result in a failure of the stress test
```

## Running the Test Suite
To run the stress test suite, ensure you have the necessary dependencies installed and execute the following command:
```sh
python3 continuous_integration --config-file <path-to-config.yaml>
```

If no config file is provided, the stress tests are going to execute on the default argument provided in the script.
