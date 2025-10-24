// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/config.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/string.hpp"
#include "utils/sysinfo/cpuinfo.hpp"

#include <iostream>
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
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(data_directory, "mg_data", "Path to directory in which to save all permanent data.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(data_recovery_on_startup, true, "Controls whether the database recovers persisted data on startup.");

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
                        "Storage python full garbage collection interval (in seconds).", FLAG_IN_RANGE(1, 24UL * 3600));

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_properties_on_edges, false, "Controls whether edges have properties.");

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

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables,misc-unused-parameters)
DEFINE_VALIDATED_bool(
    storage_parallel_index_recovery, false,
    "Controls whether the index creation can be done in a multithreaded fashion.", {
      spdlog::warn(
          "storage_parallel_index_recovery flag is deprecated. Check storage_mode_parallel_schema_recovery for more "
          "details.");
      return true;
    });

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_parallel_schema_recovery, false,
            "Controls whether the indices and constraints creation can be done in a multithreaded fashion.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_parallel_snapshot_creation, false,
            "If true, snapshots will be created using --storage-snapshot-thread-count number of treads.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(storage_snapshot_thread_count,
              std::max(memgraph::utils::sysinfo::LogicalCPUCores().value_or(1),
                       memgraph::storage::Config::Durability().snapshot_thread_count),
              "The number of threads used to create snapshots.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(storage_recovery_thread_count,
              std::max(memgraph::utils::sysinfo::LogicalCPUCores().value_or(1),
                       memgraph::storage::Config::Durability().recovery_thread_count),
              "The number of threads used to recover persisted data from disk.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_enable_schema_metadata, false,
            "Controls whether metadata should be collected about the resident labels and edge types.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_automatic_label_index_creation_enabled, false,
            "Controls whether label indexes on vertices should be created automatically.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_automatic_edge_type_index_creation_enabled, false,
            "Controls whether edge-type indexes on relationships should be created automatically.");
DEFINE_bool(storage_enable_edges_metadata, false,
            "Controls whether additional metadata should be stored about the edges in order to do faster traversals on "
            "certain queries.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_delta_on_identical_property_update, true,
            "Controls whether updating a property with the same value should create a delta object.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(schema_info_enabled, false, "Set to true to enable run-time schema info tracking.");

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
