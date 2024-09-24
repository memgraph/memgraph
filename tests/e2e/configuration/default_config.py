# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

# In order to check the working correctness of the SHOW CONFIG command, a couple of configuration flags has been passed to the testing instance. These are:
# "--log-level=TRACE", "--storage-properties-on-edges=True", "--storage-snapshot-interval-sec", "300", "--storage-wal-enabled=True"
# If you wish to modify these, update the startup_config_dict and workloads.yaml !


startup_config_dict = {
    "auth_module_mappings": (
        "",
        "",
        'Associates auth schemes to external modules. A mapping is structured as follows: "<scheme>:<absolute path>", '
        'and individual entries are separated with ";". If the mapping contains whitespace, enclose all of it inside '
        'quotation marks: " "',
    ),
    "auth_module_timeout_ms": (
        "10000",
        "10000",
        "Timeout (in milliseconds) used when waiting for a response from the auth module.",
    ),
    "auth_password_permit_null": ("true", "true", "Set to false to disable null passwords."),
    "auth_password_strength_regex": (
        ".+",
        ".+",
        "The regular expression that should be used to match the entire entered password to ensure its strength.",
    ),
    "allow_load_csv": ("true", "true", "Controls whether LOAD CSV clause is allowed in queries."),
    "audit_buffer_flush_interval_ms": (
        "200",
        "200",
        "Interval (in milliseconds) used for flushing the audit log buffer.",
    ),
    "audit_buffer_size": ("100000", "100000", "Maximum number of items in the audit log buffer."),
    "audit_enabled": ("false", "false", "Set to true to enable audit logging."),
    "auth_user_or_role_name_regex": (
        "[a-zA-Z0-9_.+-@]+",
        "[a-zA-Z0-9_.+-@]+",
        "Set to the regular expression that each user or role name must fulfill.",
    ),
    "bolt_address": ("0.0.0.0", "0.0.0.0", "IP address on which the Bolt server should listen."),
    "bolt_cert_file": ("", "", "Certificate file which should be used for the Bolt server."),
    "bolt_key_file": ("", "", "Key file which should be used for the Bolt server."),
    "bolt_num_workers": (
        "12",
        "12",
        "Number of workers used by the Bolt server. By default, this will be the number of processing units available on the machine.",
    ),
    "bolt_port": ("7687", "7687", "Port on which the Bolt server should listen."),
    "bolt_server_name_for_init": (
        "Neo4j/v5.11.0 compatible graph database server - Memgraph",
        "Neo4j/v5.11.0 compatible graph database server - Memgraph",
        "Server name which the database should send to the client in the Bolt INIT message.",
    ),
    "bolt_session_inactivity_timeout": (
        "1800",
        "1800",
        "Time in seconds after which inactive Bolt sessions will be closed.",
    ),
    "cartesian_product_enabled": ("true", "true", "Enable cartesian product expansion."),
    "management_port": ("0", "0", "Port on which coordinator servers will be started."),
    "coordinator_port": ("0", "0", "Port on which raft servers will be started."),
    "coordinator_id": ("0", "0", "Unique ID of the raft server."),
    "ha_durability": ("true", "true", "Whether to use durability for coordinator logs and snapshots."),
    "instance_down_timeout_sec": ("5", "5", "Time duration after which an instance is considered down."),
    "instance_health_check_frequency_sec": ("1", "1", "The time duration between two health checks/pings."),
    "instance_get_uuid_frequency_sec": ("10", "10", "The time duration between two instance uuid checks."),
    "coordinator_hostname": ("", "", "Instance's hostname. Used as output of SHOW INSTANCES query."),
    "data_directory": ("mg_data", "mg_data", "Path to directory in which to save all permanent data."),
    "data_recovery_on_startup": (
        "true",
        "true",
        "Controls whether the database recovers persisted data on startup.",
    ),
    "isolation_level": (
        "SNAPSHOT_ISOLATION",
        "SNAPSHOT_ISOLATION",
        "Default isolation level used for the transactions. Allowed values: SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED",
    ),
    "kafka_bootstrap_servers": (
        "",
        "",
        "List of default Kafka brokers as a comma separated list of broker host or host:port.",
    ),
    "log_file": ("", "", "Path to where the log should be stored."),
    "nuraft_log_file": ("", "", "Path to the file where NuRaft logs are saved."),
    "log_level": (
        "WARNING",
        "TRACE",
        "Minimum log level. Allowed values: TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL",
    ),
    "memory_limit": (
        "0",
        "0",
        "Total memory limit in MiB. Set to 0 to use the default values which are 100% of the phyisical memory if the swap is enabled and 90% of the physical memory otherwise.",
    ),
    "memory_warning_threshold": (
        "1024",
        "1024",
        "Memory warning threshold, in MB. If Memgraph detects there is less available RAM it will log a warning. Set to 0 to disable.",
    ),
    "metrics_address": (
        "0.0.0.0",
        "0.0.0.0",
        "IP address on which the Memgraph server for exposing metrics should listen.",
    ),
    "metrics_port": ("9091", "9091", "Port on which the Memgraph server for exposing metrics should listen."),
    "monitoring_address": (
        "0.0.0.0",
        "0.0.0.0",
        "IP address on which the websocket server for Memgraph monitoring should listen.",
    ),
    "monitoring_port": ("7444", "7444", "Port on which the websocket server for Memgraph monitoring should listen."),
    "storage_parallel_index_recovery": (
        "false",
        "false",
        "Controls whether the index creation can be done in a multithreaded fashion.",
    ),
    "storage_parallel_schema_recovery": (
        "false",
        "false",
        "Controls whether the indices and constraints creation can be done in a multithreaded fashion.",
    ),
    "storage_enable_schema_metadata": (
        "false",
        "false",
        "Controls whether metadata should be collected about the resident labels and edge types.",
    ),
    "storage_automatic_label_index_creation_enabled": (
        "false",
        "false",
        "Controls whether label indexes on vertices should be created automatically.",
    ),
    "storage_automatic_edge_type_index_creation_enabled": (
        "false",
        "false",
        "Controls whether edge-type indexes on relationships should be created automatically.",
    ),
    "storage_enable_edges_metadata": (
        "false",
        "false",
        "Controls whether additional metadata should be stored about the edges in order to do faster traversals on certain queries.",
    ),
    "storage_property_store_compression_enabled": (
        "false",
        "false",
        "Controls whether the properties should be compressed in the storage.",
    ),
    "storage_property_store_compression_level": (
        "mid",
        "mid",
        "Compression level for storing properties. Allowed values: low, mid, high.",
    ),
    "password_encryption_algorithm": ("bcrypt", "bcrypt", "The password encryption algorithm used for authentication."),
    "pulsar_service_url": ("", "", "Default URL used while connecting to Pulsar brokers."),
    "query_execution_timeout_sec": (
        "600",
        "600",
        "Maximum allowed query execution time. Queries exceeding this limit will be aborted. Value of 0 means no limit.",
    ),
    "query_modules_directory": (
        "",
        "",
        "Directory where modules with custom query procedures are stored. NOTE: Multiple comma-separated directories can be defined.",
    ),
    "replication_replica_check_frequency_sec": (
        "1",
        "1",
        "The time duration between two replica checks/pings. If < 1, replicas will NOT be checked at all. NOTE: The MAIN instance allocates a new thread for each REPLICA.",
    ),
    "storage_delta_on_identical_property_update": (
        "true",
        "true",
        "Controls whether updating a property with the same value should create a delta object.",
    ),
    "storage_gc_cycle_sec": ("30", "30", "Storage garbage collector interval (in seconds)."),
    "storage_python_gc_cycle_sec": ("180", "180", "Storage python full garbage collection interval (in seconds)."),
    "storage_items_per_batch": (
        "1000000",
        "1000000",
        "The number of edges and vertices stored in a batch in a snapshot file.",
    ),
    "storage_properties_on_edges": ("false", "true", "Controls whether edges have properties."),
    "storage_recovery_thread_count": ("12", "12", "The number of threads used to recover persisted data from disk."),
    "storage_snapshot_interval_sec": (
        "0",
        "300",
        "Storage snapshot creation interval (in seconds). Set to 0 to disable periodic snapshot creation.",
    ),
    "storage_snapshot_on_exit": ("false", "false", "Controls whether the storage creates another snapshot on exit."),
    "storage_snapshot_retention_count": ("3", "3", "The number of snapshots that should always be kept."),
    "storage_wal_enabled": (
        "false",
        "true",
        "Controls whether the storage uses write-ahead-logging. To enable WAL periodic snapshots must be enabled.",
    ),
    "storage_wal_file_flush_every_n_tx": (
        "100000",
        "100000",
        "Issue a 'fsync' call after this amount of transactions are written to the WAL file. Set to 1 for fully synchronous operation.",
    ),
    "storage_mode": (
        "IN_MEMORY_TRANSACTIONAL",
        "IN_MEMORY_TRANSACTIONAL",
        "Default storage mode Memgraph uses. Allowed values: IN_MEMORY_TRANSACTIONAL, IN_MEMORY_ANALYTICAL, ON_DISK_TRANSACTIONAL",
    ),
    "storage_wal_file_size_kib": ("20480", "20480", "Minimum file size of each WAL file."),
    "stream_transaction_conflict_retries": (
        "30",
        "30",
        "Number of times to retry when a stream transformation fails to commit because of conflicting transactions",
    ),
    "stream_transaction_retry_interval": (
        "500",
        "500",
        "Retry interval in milliseconds when a stream transformation fails to commit because of conflicting transactions",
    ),
    "telemetry_enabled": (
        "false",
        "false",
        "Set to true to enable telemetry. We collect information about the running system (CPU and memory information) and information about the database runtime (vertex and edge counts and resource usage) to allow for easier improvement of the product.",
    ),
    "timezone": (
        "UTC",
        "UTC",
        "Define instance's timezone (IANA format).",
    ),
    "query_cost_planner": ("true", "true", "Use the cost-estimating query planner."),
    "query_plan_cache_max_size": ("1000", "1000", "Maximum number of query plans to cache."),
    "query_vertex_count_to_expand_existing": (
        "10",
        "10",
        "Maximum count of indexed vertices which provoke indexed lookup and then expand to existing, instead of a regular expand. Default is 10, to turn off use -1.",
    ),
    "query_max_plans": ("1000", "1000", "Maximum number of generated plans for a query."),
    "flag_file": ("", "", "load flags from file"),
    "hops_limit_partial_results": (
        "true",
        "true",
        "If set to true, the query will return partial results if the hops limit is reached.",
    ),
    "init_file": (
        "",
        "",
        "Path to cypherl file that is used for configuring users and database schema before server starts.",
    ),
    "init_data_file": ("", "", "Path to cypherl file that is used for creating data after server starts."),
    "replication_restore_state_on_startup": (
        "true",
        "true",
        "Restore replication state on startup, e.g. recover replica",
    ),
    "query_callable_mappings_path": (
        "",
        "",
        "The path to mappings that describes aliases to callables in cypher queries in the form of key-value pairs in a json file. With this option query module procedures that do not exist in memgraph can be mapped to ones that exist.",
    ),
    "delta_chain_cache_threshold": (
        "128",
        "128",
        "The threshold for when to cache long delta chains. This is used for heavy read + write workloads where repeated processing of delta chains can become costly.",
    ),
    "experimental_enabled": (
        "",
        "",
        "Experimental features to be used, comma-separated. Options [text-search, high-availability]",
    ),
    "query_log_directory": ("", "", "Path to directory where the query logs should be stored."),
    "schema_info_enabled": ("false", "false", "Set to true to enable run-time schema info tracking."),
}
