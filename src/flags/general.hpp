// Copyright 2024 Memgraph Ltd.
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
// NOTE: The `data_directory` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
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
// NOTE: The `storage_properties_on_edges` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_properties_on_edges);
// storage_recover_on_startup deprecated; use data_recovery_on_startup instead
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_recover_on_startup);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_snapshot_interval_sec);
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
DECLARE_bool(storage_parallel_schema_recovery);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_recovery_thread_count);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_bool(storage_enable_schema_metadata);

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
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(auth_user_or_role_name_regex);
