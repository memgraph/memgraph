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
    "auth_module_create_missing_role": ("true", "true", "Set to false to disable creation of missing roles."),
    "auth_module_create_missing_user": ("true", "true", "Set to false to disable creation of missing users."),
    "auth_module_executable": ("", "", "Absolute path to the auth module executable that should be used."),
    "auth_module_manage_roles": (
        "true",
        "true",
        "Set to false to disable management of roles through the auth module.",
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
        "",
        "",
        "Server name which the database should send to the client in the Bolt INIT message.",
    ),
    "bolt_session_inactivity_timeout": (
        "1800",
        "1800",
        "Time in seconds after which inactive Bolt sessions will be closed.",
    ),
    "data_directory": ("mg_data", "mg_data", "Path to directory in which to save all permanent data."),
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
    "log_level": (
        "WARNING",
        "TRACE",
        "Minimum log level. Allowed values: TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL",
    ),
    "memory_limit": (
        "0",
        "0",
        "Total memory limit in MiB. Set to 0 to use the default values which are 100% of the physical memory if the swap is enabled and 90% of the physical memory otherwise.",
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
    "storage_gc_cycle_sec": ("30", "30", "Storage garbage collector interval (in seconds)."),
    "storage_items_per_batch": (
        "1000000",
        "1000000",
        "The number of edges and vertices stored in a batch in a snapshot file.",
    ),
    "storage_properties_on_edges": ("false", "true", "Controls whether edges have properties."),
    "storage_recover_on_startup": (
        "false",
        "false",
        "Controls whether the storage recovers persisted data on startup.",
    ),
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
    "query_cost_planner": ("true", "true", "Use the cost-estimating query planner."),
    "query_plan_cache_ttl": ("60", "60", "Time to live for cached query plans, in seconds."),
    "query_vertex_count_to_expand_existing": (
        "10",
        "10",
        "Maximum count of indexed vertices which provoke indexed lookup and then expand to existing, instead of a regular expand. Default is 10, to turn off use -1.",
    ),
    "query_max_plans": ("1000", "1000", "Maximum number of generated plans for a query."),
    "flag_file": ("", "", "load flags from file"),
    "init_file": (
        "",
        "",
        "Path to cypherl file that is used for configuring users and database schema before server starts.",
    ),
    "init_data_file": ("", "", "Path to cypherl file that is used for creating data after server starts."),
}
