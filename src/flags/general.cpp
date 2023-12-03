// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "general.hpp"

#include "glue/auth_global.hpp"
#include "storage/v2/config.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/string.hpp"

#include <thread>

// Short help flag.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_bool(h, false, "Print usage and exit.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(monitoring_address, "0.0.0.0",
              "IP address on which the websocket server for Memgraph monitoring should listen.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(metrics_address, "0.0.0.0",
              "IP address on which the Memgraph server for exposing metrics should listen.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(monitoring_port, 7444,
                       "Port on which the websocket server for Memgraph monitoring should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(metrics_port, 9091, "Port on which the Memgraph server for exposing metrics should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(init_file, "",
              "Path to cypherl file that is used for configuring users and database schema before server starts.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(init_data_file, "", "Path to cypherl file that is used for creating data after server starts.");

// General purpose flags.
// NOTE: The `data_directory` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(data_directory, "mg_data", "Path to directory in which to save all permanent data.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(data_recovery_on_startup, false, "Controls whether the database recovers persisted data on startup.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(allow_load_csv, true, "Controls whether LOAD CSV clause is allowed in queries.");

// Storage flags.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_gc_cycle_sec, 30, "Storage garbage collector interval (in seconds).",
                        FLAG_IN_RANGE(1, 24UL * 3600));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_python_gc_cycle_sec, 180,
                        "Storage python full garbage collector interval (in seconds).", FLAG_IN_RANGE(1, 24UL * 3600));
// NOTE: The `storage_properties_on_edges` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_properties_on_edges, false, "Controls whether edges have properties.");

// storage_recover_on_startup deprecated; use data_recovery_on_startup instead
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_bool(storage_recover_on_startup, false,
                   "Controls whether the storage recovers persisted data on startup.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_snapshot_interval_sec, 0,
                        "Storage snapshot creation interval (in seconds). Set "
                        "to 0 to disable periodic snapshot creation.",
                        FLAG_IN_RANGE(0, 7 * 24 * 3600));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_wal_enabled, false,
            "Controls whether the storage uses write-ahead-logging. To enable "
            "WAL periodic snapshots must be enabled.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_snapshot_retention_count, 3, "The number of snapshots that should always be kept.",
                        FLAG_IN_RANGE(1, 1000000));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_wal_file_size_kib, memgraph::storage::Config::Durability().wal_file_size_kibibytes,
                        "Minimum file size of each WAL file.",
                        FLAG_IN_RANGE(1, static_cast<unsigned long>(1000) * 1024));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_wal_file_flush_every_n_tx,
                        memgraph::storage::Config::Durability().wal_file_flush_every_n_tx,
                        "Issue a 'fsync' call after this amount of transactions are written to the "
                        "WAL file. Set to 1 for fully synchronous operation.",
                        FLAG_IN_RANGE(1, 1000000));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_snapshot_on_exit, false, "Controls whether the storage creates another snapshot on exit.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(storage_items_per_batch, memgraph::storage::Config::Durability().items_per_batch,
              "The number of edges and vertices stored in a batch in a snapshot file.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_parallel_index_recovery, false,
            "Controls whether the index creation can be done in a multithreaded fashion.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(storage_recovery_thread_count,
              std::max(static_cast<uint64_t>(std::thread::hardware_concurrency()),
                       memgraph::storage::Config::Durability().recovery_thread_count),
              "The number of threads used to recover persisted data from disk.");

#ifdef MG_ENTERPRISE
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_delete_on_drop, true,
            "If set to true the query 'DROP DATABASE x' will delete the underlying storage as well.");
#endif

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");

// Streams flags
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint32(
    stream_transaction_conflict_retries, 30,
    "Number of times to retry when a stream transformation fails to commit because of conflicting transactions");
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint32(
    stream_transaction_retry_interval, 500,
    "Retry interval in milliseconds when a stream transformation fails to commit because of conflicting transactions");
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(kafka_bootstrap_servers, "",
              "List of default Kafka brokers as a comma separated list of broker host or host:port.");

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(pulsar_service_url, "", "Default URL used while connecting to Pulsar brokers.");

// Query flags.

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(replication_replica_check_frequency_sec, 1,
              "The time duration between two replica checks/pings. If < 1, replicas will NOT be checked at all. NOTE: "
              "The MAIN instance allocates a new thread for each REPLICA.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(replication_restore_state_on_startup, false, "Restore replication state on startup, e.g. recover replica");

DEFINE_VALIDATED_string(query_modules_directory, "",
                        "Directory where modules with custom query procedures are stored. "
                        "NOTE: Multiple comma-separated directories can be defined.",
                        {
                          if (value.empty()) return true;
                          const auto directories = memgraph::utils::Split(value, ",");
                          for (const auto &dir : directories) {
                            if (!memgraph::utils::DirExists(dir)) {
                              std::cout << "Expected --" << flagname << " to point to directories." << std::endl;
                              std::cout << dir << " is not a directory." << std::endl;
                              return false;
                            }
                          }
                          return true;
                        });

auto memgraph::flags::ParseQueryModulesDirectory() -> std::vector<std::filesystem::path> {
  const auto directories = memgraph::utils::Split(FLAGS_query_modules_directory, ",");
  std::vector<std::filesystem::path> query_modules_directories;
  query_modules_directories.reserve(directories.size());
  std::transform(directories.begin(), directories.end(), std::back_inserter(query_modules_directories),
                 [](const auto &dir) { return dir; });

  return query_modules_directories;
}

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(query_callable_mappings_path, "",
              "The path to mappings that describes aliases to callables in cypher queries in the form of key-value "
              "pairs in a json file. With this option query module procedures that do not exist in memgraph can be "
              "mapped to ones that exist.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_string(license_key, "", "License key for Memgraph Enterprise.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_string(organization_name, "", "Organization name.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(auth_user_or_role_name_regex, memgraph::glue::kDefaultUserRoleRegex.data(),
              "Set to the regular expression that each user or role name must fulfill.");
