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
    "Set to false to disable creation of missing roles.": ("true", "true"),
    "Set to false to disable creation of missing users.": ("true", "true"),
    "Absolute path to the auth module executable that should be used.": ("", ""),
    "Set to false to disable management of roles through the auth module.": ("true", "true"),
    "Timeout (in milliseconds) used when waiting for a response from the auth module.": ("10000", "10000"),
    "Set to false to disable null passwords.": ("true", "true"),
    "The regular expression that should be used to match the entire entered password to ensure its strength.": (
        ".+",
        ".+",
    ),
    "Controls whether LOAD CSV clause is allowed in queries.": ("true", "true"),
    "Interval (in milliseconds) used for flushing the audit log buffer.": ("200", "200"),
    "Maximum number of items in the audit log buffer.": ("100000", "100000"),
    "Set to true to enable audit logging.": ("false", "false"),
    "Set to the regular expression that each user or role name must fulfill.": (
        "[a-zA-Z0-9_.+-@]+",
        "[a-zA-Z0-9_.+-@]+",
    ),
    "IP address on which the Bolt server should listen.": ("0.0.0.0", "0.0.0.0"),
    "Certificate file which should be used for the Bolt server.": ("", ""),
    "Key file which should be used for the Bolt server.": ("", ""),
    "Number of workers used by the Bolt server. By default, this will be the number of processing units available on the machine.": (
        "12",
        "12",
    ),
    "Port on which the Bolt server should listen.": ("7687", "7687"),
    "Server name which the database should send to the client in the Bolt INIT message.": ("", ""),
    "Time in seconds after which inactive Bolt sessions will be closed.": ("1800", "1800"),
    "Path to directory in which to save all permanent data.": ("mg_data", "mg_data"),
    "Default isolation level used for the transactions. Allowed values: SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED": (
        "SNAPSHOT_ISOLATION",
        "SNAPSHOT_ISOLATION",
    ),
    "List of default Kafka brokers as a comma separated list of broker host or host:port.": ("", ""),
    "Path to where the log should be stored.": ("", ""),
    "Minimum log level. Allowed values: TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL": ("WARNING", "TRACE"),
    "Total memory limit in MiB. Set to 0 to use the default values which are 100% of the phyisical memory if the swap is enabled and 90% of the physical memory otherwise.": (
        "0",
        "0",
    ),
    "Memory warning threshold, in MB. If Memgraph detects there is less available RAM it will log a warning. Set to 0 to disable.": (
        "1024",
        "1024",
    ),
    "IP address on which the websocket server for Memgraph monitoring should listen.": ("0.0.0.0", "0.0.0.0"),
    "Port on which the websocket server for Memgraph monitoring should listen.": ("7444", "7444"),
    "Default URL used while connecting to Pulsar brokers.": ("", ""),
    "Maximum allowed query execution time. Queries exceeding this limit will be aborted. Value of 0 means no limit.": (
        "600",
        "600",
    ),
    "Directory where modules with custom query procedures are stored. NOTE: Multiple comma-separated directories can be defined.": (
        "",
        "",
    ),
    "The time duration between two replica checks/pings. If < 1, replicas will NOT be checked at all. NOTE: The MAIN instance allocates a new thread for each REPLICA.": (
        "1",
        "1",
    ),
    "Storage garbage collector interval (in seconds).": ("30", "30"),
    "Controls whether edges have properties.": ("false", "true"),
    "Controls whether the storage recovers persisted data on startup.": ("false", "false"),
    "Controls replicas should be restored automatically.": ("true", "true"),
    "Storage snapshot creation interval (in seconds). Set to 0 to disable periodic snapshot creation.": ("0", "300"),
    "Controls whether the storage creates another snapshot on exit.": ("false", "false"),
    "The number of snapshots that should always be kept.": ("3", "3"),
    "Controls whether the storage uses write-ahead-logging. To enable WAL periodic snapshots must be enabled.": (
        "false",
        "true",
    ),
    "Issue a 'fsync' call after this amount of transactions are written to the WAL file. Set to 1 for fully synchronous operation.": (
        "100000",
        "100000",
    ),
    "Minimum file size of each WAL file.": ("20480", "20480"),
    "Number of times to retry when a stream transformation fails to commit because of conflicting transactions": (
        "30",
        "30",
    ),
    "Retry interval in milliseconds when a stream transformation fails to commit because of conflicting transactions": (
        "500",
        "500",
    ),
    "Set to true to enable telemetry. We collect information about the running system (CPU and memory information) and information about the database runtime (vertex and edge counts and resource usage) to allow for easier improvement of the product.": (
        "false",
        "false",
    ),
    "Use the cost-estimating query planner.": ("true", "true"),
    "Time to live for cached query plans, in seconds.": ("60", "60"),
    "Maximum count of indexed vertices which provoke indexed lookup and then expand to existing, instead of a regular expand. Default is 10, to turn off use -1.": (
        "10",
        "10",
    ),
    "Maximum number of generated plans for a query.": ("1000", "1000"),
    "load flags from file": ("", ""),
}
