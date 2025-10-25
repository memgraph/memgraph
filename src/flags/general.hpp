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

#pragma once

#include "gflags/gflags.h"

#include <filesystem>

// Short help flag.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(h);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(monitoring_address);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_int32(monitoring_port);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(metrics_address);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_int32(metrics_port);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(init_file);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(init_data_file);

// General purpose flags.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(data_directory);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(data_recovery_on_startup);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(memory_warning_threshold);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(allow_load_csv);

// Storage flags.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_gc_cycle_sec);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_python_gc_cycle_sec);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_properties_on_edges);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_snapshot_interval_sec);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(storage_snapshot_interval);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_wal_enabled);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_snapshot_retention_count);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_wal_file_size_kib);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_wal_file_flush_every_n_tx);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_snapshot_on_exit);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_items_per_batch);
// storage_parallel_index_recovery deprecated; use storage_parallel_schema_recovery instead
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_parallel_index_recovery);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_parallel_snapshot_creation);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_parallel_schema_recovery);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_snapshot_thread_count);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_recovery_thread_count);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_enable_schema_metadata);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_automatic_label_index_creation_enabled);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_automatic_edge_type_index_creation_enabled);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_enable_edges_metadata);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_delta_on_identical_property_update);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(schema_info_enabled);

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(telemetry_enabled);

// Streams flags
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint32(stream_transaction_conflict_retries);
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint32(stream_transaction_retry_interval);

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(kafka_bootstrap_servers);

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(pulsar_service_url);

// Query flags.

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
// DECLARE_double(query_execution_timeout_sec); Moved to run_time_configurable
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(query_modules_directory);
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(query_callable_mappings_path);
namespace memgraph::flags {
auto ParseQueryModulesDirectory() -> std::vector<std::filesystem::path>;
}  // namespace memgraph::flags

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(license_key);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(organization_name);

// AWS settings
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(aws_region);
DECLARE_string(aws_access_key);
DECLARE_string(aws_secret_key);
