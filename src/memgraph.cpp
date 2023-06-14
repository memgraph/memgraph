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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <thread>

#include <fmt/core.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <spdlog/common.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "auth/models.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "communication/http/server.hpp"
#include "communication/websocket/auth.hpp"
#include "communication/websocket/server.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "helpers.hpp"
#include "http_handlers/metrics.hpp"
#include "license/license.hpp"
#include "license/license_sender.hpp"
#include "py/py.hpp"
#include "query/auth_checker.hpp"
#include "query/discard_value_stream.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpreter.hpp"
#include "query/plan/operator.hpp"
#include "query/procedure/module.hpp"
#include "query/procedure/py_module.hpp"
#include "requests/requests.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/enum.hpp"
#include "utils/event_counter.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/readable_size.hpp"
#include "utils/rw_lock.hpp"
#include "utils/settings.hpp"
#include "utils/signals.hpp"
#include "utils/string.hpp"
#include "utils/synchronized.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/system_info.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

// Communication libraries must be included after query libraries are included.
// This is to enable compilation of the binary when linking with old OpenSSL
// libraries (as on CentOS 7).
//
// The OpenSSL library available on CentOS 7 is v1.0.0, that version includes
// `libkrb5` in its public API headers (that we include in our communication
// stack). The `libkrb5` library has `#define`s for `TRUE` and `FALSE`. Those
// defines clash with Antlr's usage of `TRUE` and `FALSE` as enumeration keys.
// Because of that the definitions of `TRUE` and `FALSE` that are inherited
// from `libkrb5` must be included after the Antlr includes. Hence,
// communication headers must be included after query headers.
#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/init.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "glue/communication.hpp"

#include "auth/auth.hpp"
#include "glue/auth.hpp"

#ifdef MG_ENTERPRISE
#include "audit/log.hpp"
#endif

constexpr const char *kMgUser = "MEMGRAPH_USER";
constexpr const char *kMgPassword = "MEMGRAPH_PASSWORD";
constexpr const char *kMgPassfile = "MEMGRAPH_PASSFILE";

// Short help flag.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_bool(h, false, "Print usage and exit.");

// Bolt server flags.
DEFINE_string(bolt_address, "0.0.0.0", "IP address on which the Bolt server should listen.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(monitoring_address, "0.0.0.0",
              "IP address on which the websocket server for Memgraph monitoring should listen.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(metrics_address, "0.0.0.0",
              "IP address on which the Memgraph server for exposing metrics should listen.");
DEFINE_VALIDATED_int32(bolt_port, 7687, "Port on which the Bolt server should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(monitoring_port, 7444,
                       "Port on which the websocket server for Memgraph monitoring should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(metrics_port, 9091, "Port on which the Memgraph server for exposing metrics should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(bolt_num_workers, std::max(std::thread::hardware_concurrency(), 1U),
                       "Number of workers used by the Bolt server. By default, this will be the "
                       "number of processing units available on the machine.",
                       FLAG_IN_RANGE(1, INT32_MAX));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(bolt_session_inactivity_timeout, 1800,
                       "Time in seconds after which inactive Bolt sessions will be "
                       "closed.",
                       FLAG_IN_RANGE(1, INT32_MAX));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(bolt_cert_file, "", "Certificate file which should be used for the Bolt server.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(bolt_key_file, "", "Key file which should be used for the Bolt server.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(bolt_server_name_for_init, "",
              "Server name which the database should send to the client in the "
              "Bolt INIT message.");
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
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(allow_load_csv, true, "Controls whether LOAD CSV clause is allowed in queries.");

// Storage flags.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(storage_gc_cycle_sec, 30, "Storage garbage collector interval (in seconds).",
                        FLAG_IN_RANGE(1, 24 * 3600));
// NOTE: The `storage_properties_on_edges` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_properties_on_edges, false, "Controls whether edges have properties.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_recover_on_startup, false, "Controls whether the storage recovers persisted data on startup.");
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

// Audit logging flags.
#ifdef MG_ENTERPRISE
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(audit_enabled, false, "Set to true to enable audit logging.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(audit_buffer_size, memgraph::audit::kBufferSizeDefault,
                       "Maximum number of items in the audit log buffer.", FLAG_IN_RANGE(1, INT32_MAX));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(audit_buffer_flush_interval_ms, memgraph::audit::kBufferFlushIntervalMillisDefault,
                       "Interval (in milliseconds) used for flushing the audit log buffer.",
                       FLAG_IN_RANGE(10, INT32_MAX));
#endif

// Query flags.

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_double(query_execution_timeout_sec, 600,
              "Maximum allowed query execution time. Queries exceeding this "
              "limit will be aborted. Value of 0 means no limit.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(replication_replica_check_frequency_sec, 1,
              "The time duration between two replica checks/pings. If < 1, replicas will NOT be checked at all. NOTE: "
              "The MAIN instance allocates a new thread for each REPLICA.");

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(
    memory_limit, 0,
    "Total memory limit in MiB. Set to 0 to use the default values which are 100\% of the phyisical memory if the swap "
    "is enabled and 90\% of the physical memory otherwise.");

namespace {
using namespace std::literals;
inline constexpr std::array isolation_level_mappings{
    std::pair{"SNAPSHOT_ISOLATION"sv, memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION},
    std::pair{"READ_COMMITTED"sv, memgraph::storage::IsolationLevel::READ_COMMITTED},
    std::pair{"READ_UNCOMMITTED"sv, memgraph::storage::IsolationLevel::READ_UNCOMMITTED}};

const std::string isolation_level_help_string =
    fmt::format("Default isolation level used for the transactions. Allowed values: {}",
                memgraph::utils::GetAllowedEnumValuesString(isolation_level_mappings));
}  // namespace

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(isolation_level, "SNAPSHOT_ISOLATION", isolation_level_help_string.c_str(), {
  if (const auto result = memgraph::utils::IsValidEnumValueString(value, isolation_level_mappings); result.HasError()) {
    const auto error = result.GetError();
    switch (error) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Isolation level cannot be empty." << std::endl;
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for isolation level. Allowed values: "
                  << memgraph::utils::GetAllowedEnumValuesString(isolation_level_mappings) << std::endl;
        break;
      }
    }
    return false;
  }

  return true;
});

namespace {
memgraph::storage::IsolationLevel ParseIsolationLevel() {
  const auto isolation_level =
      memgraph::utils::StringToEnum<memgraph::storage::IsolationLevel>(FLAGS_isolation_level, isolation_level_mappings);
  MG_ASSERT(isolation_level, "Invalid isolation level");
  return *isolation_level;
}

int64_t GetMemoryLimit() {
  if (FLAGS_memory_limit == 0) {
    auto maybe_total_memory = memgraph::utils::sysinfo::TotalMemory();
    MG_ASSERT(maybe_total_memory, "Failed to fetch the total physical memory");
    const auto maybe_swap_memory = memgraph::utils::sysinfo::SwapTotalMemory();
    MG_ASSERT(maybe_swap_memory, "Failed to fetch the total swap memory");

    if (*maybe_swap_memory == 0) {
      // take only 90% of the total memory
      *maybe_total_memory *= 9;
      *maybe_total_memory /= 10;
    }
    return *maybe_total_memory * 1024;
  }

  // We parse the memory as MiB every time
  return FLAGS_memory_limit * 1024 * 1024;
}
}  // namespace

namespace {
std::vector<std::filesystem::path> query_modules_directories;
}  // namespace
DEFINE_VALIDATED_string(query_modules_directory, "",
                        "Directory where modules with custom query procedures are stored. "
                        "NOTE: Multiple comma-separated directories can be defined.",
                        {
                          query_modules_directories.clear();
                          if (value.empty()) return true;
                          const auto directories = memgraph::utils::Split(value, ",");
                          for (const auto &dir : directories) {
                            if (!memgraph::utils::DirExists(dir)) {
                              std::cout << "Expected --" << flagname << " to point to directories." << std::endl;
                              std::cout << dir << " is not a directory." << std::endl;
                              return false;
                            }
                          }
                          query_modules_directories.reserve(directories.size());
                          std::transform(directories.begin(), directories.end(),
                                         std::back_inserter(query_modules_directories),
                                         [](const auto &dir) { return dir; });
                          return true;
                        });

// Logging flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_bool(also_log_to_stderr, false, "Log messages go to stderr in addition to logfiles");
DEFINE_string(log_file, "", "Path to where the log should be stored.");

namespace {
inline constexpr std::array log_level_mappings{
    std::pair{"TRACE"sv, spdlog::level::trace}, std::pair{"DEBUG"sv, spdlog::level::debug},
    std::pair{"INFO"sv, spdlog::level::info},   std::pair{"WARNING"sv, spdlog::level::warn},
    std::pair{"ERROR"sv, spdlog::level::err},   std::pair{"CRITICAL"sv, spdlog::level::critical}};

const std::string log_level_help_string = fmt::format("Minimum log level. Allowed values: {}",
                                                      memgraph::utils::GetAllowedEnumValuesString(log_level_mappings));
}  // namespace

DEFINE_VALIDATED_string(log_level, "WARNING", log_level_help_string.c_str(), {
  if (const auto result = memgraph::utils::IsValidEnumValueString(value, log_level_mappings); result.HasError()) {
    const auto error = result.GetError();
    switch (error) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Log level cannot be empty." << std::endl;
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for log level. Allowed values: "
                  << memgraph::utils::GetAllowedEnumValuesString(log_level_mappings) << std::endl;
        break;
      }
    }
    return false;
  }

  return true;
});

namespace {
spdlog::level::level_enum ParseLogLevel() {
  const auto log_level = memgraph::utils::StringToEnum<spdlog::level::level_enum>(FLAGS_log_level, log_level_mappings);
  MG_ASSERT(log_level, "Invalid log level");
  return *log_level;
}

// 5 weeks * 7 days
inline constexpr auto log_retention_count = 35;
void CreateLoggerFromSink(const auto &sinks, const auto log_level) {
  auto logger = std::make_shared<spdlog::logger>("memgraph_log", sinks.begin(), sinks.end());
  logger->set_level(log_level);
  logger->flush_on(spdlog::level::trace);
  spdlog::set_default_logger(std::move(logger));
}

void InitializeLogger() {
  std::vector<spdlog::sink_ptr> sinks;

  if (FLAGS_also_log_to_stderr) {
    sinks.emplace_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
  }

  if (!FLAGS_log_file.empty()) {
    // get local time
    time_t current_time{0};
    struct tm *local_time{nullptr};

    time(&current_time);
    local_time = localtime(&current_time);

    sinks.emplace_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>(
        FLAGS_log_file, local_time->tm_hour, local_time->tm_min, false, log_retention_count));
  }
  CreateLoggerFromSink(sinks, ParseLogLevel());
}

void AddLoggerSink(spdlog::sink_ptr new_sink) {
  auto default_logger = spdlog::default_logger();
  auto sinks = default_logger->sinks();
  sinks.push_back(new_sink);
  CreateLoggerFromSink(sinks, default_logger->level());
}

}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_string(license_key, "", "License key for Memgraph Enterprise.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_string(organization_name, "", "Organization name.");

/// Encapsulates Dbms and Interpreter that are passed through the network server
/// and worker to the session.
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
#if MG_ENTERPRISE

  SessionData(memgraph::storage::Storage *db, memgraph::query::InterpreterContext *interpreter_context,
              memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
              memgraph::audit::Log *audit_log)
      : db(db), interpreter_context(interpreter_context), auth(auth), audit_log(audit_log) {}
  memgraph::storage::Storage *db;
  memgraph::query::InterpreterContext *interpreter_context;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth;
  memgraph::audit::Log *audit_log;

#else

  SessionData(memgraph::storage::Storage *db, memgraph::query::InterpreterContext *interpreter_context,
              memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth)
      : db(db), interpreter_context(interpreter_context), auth(auth) {}
  memgraph::storage::Storage *db;
  memgraph::query::InterpreterContext *interpreter_context;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth;

#endif
  // NOTE: run_id should be const but that complicates code a lot.
  std::optional<std::string> run_id;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(auth_user_or_role_name_regex, memgraph::glue::kDefaultUserRoleRegex.data(),
              "Set to the regular expression that each user or role name must fulfill.");

void InitFromCypherlFile(memgraph::query::InterpreterContext &ctx, std::string cypherl_file_path
#ifdef MG_ENTERPRISE
                         ,
                         memgraph::audit::Log *audit_log
#endif
) {
  memgraph::query::Interpreter interpreter(&ctx);
  std::ifstream file(cypherl_file_path);
  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      if (!line.empty()) {
        auto results = interpreter.Prepare(line, {}, {});
        memgraph::query::DiscardValueResultStream stream;
        interpreter.Pull(&stream, {}, results.qid);

#ifdef MG_ENTERPRISE
        if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
          audit_log->Record("", "", line, {});
        }
#endif
      }
    }
    file.close();
  }
}

namespace memgraph::metrics {
extern const Event ActiveBoltSessions;
}  // namespace memgraph::metrics

class BoltSession final : public memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                                                        memgraph::communication::v2::OutputStream> {
 public:
  BoltSession(SessionData *data, const memgraph::communication::v2::ServerEndpoint &endpoint,
              memgraph::communication::v2::InputStream *input_stream,
              memgraph::communication::v2::OutputStream *output_stream)
      : memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>(input_stream, output_stream),
        db_(data->db),
        interpreter_context_(data->interpreter_context),
        interpreter_(data->interpreter_context),
        auth_(data->auth),
#if MG_ENTERPRISE
        audit_log_(data->audit_log),
#endif
        endpoint_(endpoint),
        run_id_(data->run_id) {
    memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveBoltSessions);
    interpreter_context_->interpreters.WithLock([this](auto &interpreters) { interpreters.insert(&interpreter_); });
  }

  ~BoltSession() override {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveBoltSessions);
    interpreter_context_->interpreters.WithLock([this](auto &interpreters) { interpreters.erase(&interpreter_); });
  }

  using memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>::TEncoder;

  void BeginTransaction(const std::map<std::string, memgraph::communication::bolt::Value> &metadata) override {
    std::map<std::string, memgraph::storage::PropertyValue> metadata_pv;
    for (const auto &[key, bolt_value] : metadata) {
      metadata_pv.emplace(key, memgraph::glue::ToPropertyValue(bolt_value));
    }
    interpreter_.BeginTransaction(metadata_pv);
  }

  void CommitTransaction() override { interpreter_.CommitTransaction(); }

  void RollbackTransaction() override { interpreter_.RollbackTransaction(); }

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, memgraph::communication::bolt::Value> &params,
      const std::map<std::string, memgraph::communication::bolt::Value> &metadata) override {
    std::map<std::string, memgraph::storage::PropertyValue> params_pv;
    std::map<std::string, memgraph::storage::PropertyValue> metadata_pv;
    for (const auto &[key, bolt_param] : params) {
      params_pv.emplace(key, memgraph::glue::ToPropertyValue(bolt_param));
    }
    for (const auto &[key, bolt_md] : metadata) {
      metadata_pv.emplace(key, memgraph::glue::ToPropertyValue(bolt_md));
    }
    const std::string *username{nullptr};
    if (user_) {
      username = &user_->username();
    }
#ifdef MG_ENTERPRISE
    if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
      audit_log_->Record(endpoint_.address().to_string(), user_ ? *username : "", query,
                         memgraph::storage::PropertyValue(params_pv));
    }
#endif
    try {
      auto result = interpreter_.Prepare(query, params_pv, username, metadata_pv);
      if (user_ && !memgraph::glue::AuthChecker::IsUserAuthorized(*user_, result.privileges)) {
        interpreter_.Abort();
        throw memgraph::communication::bolt::ClientError(
            "You are not authorized to execute this query! Please contact "
            "your database administrator.");
      }
      return {result.headers, result.qid};

    } catch (const memgraph::query::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw memgraph::communication::bolt::ClientError(e.what());
    } catch (const memgraph::query::ReplicationException &e) {
      throw memgraph::communication::bolt::ClientError(e.what());
    }
  }

  std::map<std::string, memgraph::communication::bolt::Value> Pull(TEncoder *encoder, std::optional<int> n,
                                                                   std::optional<int> qid) override {
    TypedValueResultStream stream(encoder, db_);
    return PullResults(stream, n, qid);
  }

  std::map<std::string, memgraph::communication::bolt::Value> Discard(std::optional<int> n,
                                                                      std::optional<int> qid) override {
    memgraph::query::DiscardValueResultStream stream;
    return PullResults(stream, n, qid);
  }

  void Abort() override { interpreter_.Abort(); }

  bool Authenticate(const std::string &username, const std::string &password) override {
    auto locked_auth = auth_->Lock();
    if (!locked_auth->HasUsers()) {
      return true;
    }
    user_ = locked_auth->Authenticate(username, password);
    return user_.has_value();
  }

  std::optional<std::string> GetServerNameForInit() override {
    if (FLAGS_bolt_server_name_for_init.empty()) return std::nullopt;
    return FLAGS_bolt_server_name_for_init;
  }

 private:
  template <typename TStream>
  std::map<std::string, memgraph::communication::bolt::Value> PullResults(TStream &stream, std::optional<int> n,
                                                                          std::optional<int> qid) {
    try {
      const auto &summary = interpreter_.Pull(&stream, n, qid);
      std::map<std::string, memgraph::communication::bolt::Value> decoded_summary;
      for (const auto &kv : summary) {
        auto maybe_value = memgraph::glue::ToBoltValue(kv.second, *db_, memgraph::storage::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case memgraph::storage::Error::DELETED_OBJECT:
            case memgraph::storage::Error::SERIALIZATION_ERROR:
            case memgraph::storage::Error::VERTEX_HAS_EDGES:
            case memgraph::storage::Error::PROPERTIES_DISABLED:
            case memgraph::storage::Error::NONEXISTENT_OBJECT:
              throw memgraph::communication::bolt::ClientError("Unexpected storage error when streaming summary.");
          }
        }
        decoded_summary.emplace(kv.first, std::move(*maybe_value));
      }
      // Add this memgraph instance run_id, received from telemetry
      // This is sent with every query, instead of only on bolt init inside
      // communication/bolt/v1/states/init.hpp because neo4jdriver does not
      // read the init message.
      if (auto run_id = run_id_; run_id) {
        decoded_summary.emplace("run_id", *run_id);
      }

      return decoded_summary;
    } catch (const memgraph::query::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw memgraph::communication::bolt::ClientError(e.what());
    }
  }

  /// Wrapper around TEncoder which converts TypedValue to Value
  /// before forwarding the calls to original TEncoder.
  class TypedValueResultStream {
   public:
    TypedValueResultStream(TEncoder *encoder, const memgraph::storage::Storage *db) : encoder_(encoder), db_(db) {}

    void Result(const std::vector<memgraph::query::TypedValue> &values) {
      std::vector<memgraph::communication::bolt::Value> decoded_values;
      decoded_values.reserve(values.size());
      for (const auto &v : values) {
        auto maybe_value = memgraph::glue::ToBoltValue(v, *db_, memgraph::storage::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case memgraph::storage::Error::DELETED_OBJECT:
              throw memgraph::communication::bolt::ClientError("Returning a deleted object as a result.");
            case memgraph::storage::Error::NONEXISTENT_OBJECT:
              throw memgraph::communication::bolt::ClientError("Returning a nonexistent object as a result.");
            case memgraph::storage::Error::VERTEX_HAS_EDGES:
            case memgraph::storage::Error::SERIALIZATION_ERROR:
            case memgraph::storage::Error::PROPERTIES_DISABLED:
              throw memgraph::communication::bolt::ClientError("Unexpected storage error when streaming results.");
          }
        }
        decoded_values.emplace_back(std::move(*maybe_value));
      }
      encoder_->MessageRecord(decoded_values);
    }

   private:
    TEncoder *encoder_;
    // NOTE: Needed only for ToBoltValue conversions
    const memgraph::storage::Storage *db_;
  };

  // NOTE: Needed only for ToBoltValue conversions
  const memgraph::storage::Storage *db_;
  memgraph::query::InterpreterContext *interpreter_context_;
  memgraph::query::Interpreter interpreter_;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
  std::optional<memgraph::auth::User> user_;
#ifdef MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
#endif
  memgraph::communication::v2::ServerEndpoint endpoint_;
  // NOTE: run_id should be const but that complicates code a lot.
  std::optional<std::string> run_id_;
};

using ServerT = memgraph::communication::v2::Server<BoltSession, SessionData>;
using MonitoringServerT =
    memgraph::communication::http::Server<memgraph::http::MetricsRequestHandler<SessionData>, SessionData>;
using memgraph::communication::ServerContext;

// Needed to correctly handle memgraph destruction from a signal handler.
// Without having some sort of a flag, it is possible that a signal is handled
// when we are exiting main, inside destructors of database::GraphDb and
// similar. The signal handler may then initiate another shutdown on memgraph
// which is in half destructed state, causing invalid memory access and crash.
volatile sig_atomic_t is_shutting_down = 0;

void InitSignalHandlers(const std::function<void()> &shutdown_fun) {
  // Prevent handling shutdown inside a shutdown. For example, SIGINT handler
  // being interrupted by SIGTERM before is_shutting_down is set, thus causing
  // double shutdown.
  sigset_t block_shutdown_signals;
  sigemptyset(&block_shutdown_signals);
  sigaddset(&block_shutdown_signals, SIGTERM);
  sigaddset(&block_shutdown_signals, SIGINT);

  // Wrap the shutdown function in a safe way to prevent recursive shutdown.
  auto shutdown = [shutdown_fun]() {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    shutdown_fun();
  };

  MG_ASSERT(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::Terminate, shutdown,
                                                            block_shutdown_signals),
            "Unable to register SIGTERM handler!");
  MG_ASSERT(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::Interrupt, shutdown,
                                                            block_shutdown_signals),
            "Unable to register SIGINT handler!");
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph database server");
  gflags::SetVersionString(version_string);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig("memgraph");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_h) {
    gflags::ShowUsageWithFlags(argv[0]);
    exit(1);
  }

  InitializeLogger();

  // Unhandled exception handler init.
  std::set_terminate(&memgraph::utils::TerminateHandler);

  // Initialize Python
  auto *program_name = Py_DecodeLocale(argv[0], nullptr);
  MG_ASSERT(program_name);
  // Set program name, so Python can find its way to runtime libraries relative
  // to executable.
  Py_SetProgramName(program_name);
  PyImport_AppendInittab("_mgp", &memgraph::query::procedure::PyInitMgpModule);
  Py_InitializeEx(0 /* = initsigs */);
  PyEval_InitThreads();
  Py_BEGIN_ALLOW_THREADS;

  // Add our Python modules to sys.path
  try {
    auto exe_path = memgraph::utils::GetExecutablePath();
    auto py_support_dir = exe_path.parent_path() / "python_support";
    if (std::filesystem::is_directory(py_support_dir)) {
      auto gil = memgraph::py::EnsureGIL();
      auto maybe_exc = memgraph::py::AppendToSysPath(py_support_dir.c_str());
      if (maybe_exc) {
        spdlog::error(memgraph::utils::MessageWithLink("Unable to load support for embedded Python: {}.", *maybe_exc,
                                                       "https://memgr.ph/python"));
      } else {
        // Change how we load dynamic libraries on Python by using RTLD_NOW and
        // RTLD_DEEPBIND flags. This solves an issue with using the wrong version of
        // libstd.
        auto gil = memgraph::py::EnsureGIL();
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        auto *flag = PyLong_FromLong(RTLD_NOW | RTLD_DEEPBIND);
        auto *setdl = PySys_GetObject("setdlopenflags");
        MG_ASSERT(setdl);
        auto *arg = PyTuple_New(1);
        MG_ASSERT(arg);
        MG_ASSERT(PyTuple_SetItem(arg, 0, flag) == 0);
        PyObject_CallObject(setdl, arg);
        Py_DECREF(flag);
        Py_DECREF(setdl);
        Py_DECREF(arg);
      }
    } else {
      spdlog::error(
          memgraph::utils::MessageWithLink("Unable to load support for embedded Python: missing directory {}.",
                                           py_support_dir, "https://memgr.ph/python"));
    }
  } catch (const std::filesystem::filesystem_error &e) {
    spdlog::error(memgraph::utils::MessageWithLink("Unable to load support for embedded Python: {}.", e.what(),
                                                   "https://memgr.ph/python"));
  }

  // Initialize the communication library.
  memgraph::communication::SSLInit sslInit;

  // Initialize the requests library.
  memgraph::requests::Init();

  // Start memory warning logger.
  memgraph::utils::Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    auto free_ram = memgraph::utils::sysinfo::AvailableMemory();
    if (free_ram) {
      mem_log_scheduler.Run("Memory warning", std::chrono::seconds(3), [] {
        auto free_ram = memgraph::utils::sysinfo::AvailableMemory();
        if (free_ram && *free_ram / 1024 < FLAGS_memory_warning_threshold)
          spdlog::warn(memgraph::utils::MessageWithLink("Running out of available RAM, only {} MB left.",
                                                        *free_ram / 1024, "https://memgr.ph/ram"));
      });
    } else {
      // Kernel version for the `MemAvailable` value is from: man procfs
      spdlog::warn(
          "You have an older kernel version (<3.14) or the /proc "
          "filesystem isn't available so remaining memory warnings "
          "won't be available.");
    }
  }

  std::cout << "You are running Memgraph v" << gflags::VersionString() << std::endl;
  std::cout << "To get started with Memgraph, visit https://memgr.ph/start" << std::endl;

  auto data_directory = std::filesystem::path(FLAGS_data_directory);

  const auto memory_limit = GetMemoryLimit();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  spdlog::info("Memory limit in config is set to {}", memgraph::utils::GetReadableSize(memory_limit));
  memgraph::utils::total_memory_tracker.SetMaximumHardLimit(memory_limit);
  memgraph::utils::total_memory_tracker.SetHardLimit(memory_limit);

  memgraph::utils::global_settings.Initialize(data_directory / "settings");
  memgraph::utils::OnScopeExit settings_finalizer([&] { memgraph::utils::global_settings.Finalize(); });

  // register all runtime settings
  memgraph::license::RegisterLicenseSettings(memgraph::license::global_license_checker,
                                             memgraph::utils::global_settings);

  memgraph::license::global_license_checker.CheckEnvLicense();
  if (!FLAGS_organization_name.empty() && !FLAGS_license_key.empty()) {
    memgraph::license::global_license_checker.SetLicenseInfoOverride(FLAGS_license_key, FLAGS_organization_name);
  }

  memgraph::license::global_license_checker.StartBackgroundLicenseChecker(memgraph::utils::global_settings);

  // All enterprise features should be constructed before the main database
  // storage. This will cause them to be destructed *after* the main database
  // storage. That way any errors that happen during enterprise features
  // destruction won't have an impact on the storage engine.
  // Example: When the main storage is destructed it makes a snapshot. When
  // audit logging is destructed it syncs all pending data to disk and that can
  // fail. That is why it must be destructed *after* the main database storage
  // to minimise the impact of their failure on the main storage.

  // Begin enterprise features initialization

  // Auth
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> auth{data_directory /
                                                                                                    "auth"};

#ifdef MG_ENTERPRISE
  // Audit log
  memgraph::audit::Log audit_log{data_directory / "audit", FLAGS_audit_buffer_size,
                                 FLAGS_audit_buffer_flush_interval_ms};
  // Start the log if enabled.
  if (FLAGS_audit_enabled) {
    audit_log.Start();
  }
  // Setup SIGUSR2 to be used for reopening audit log files, when e.g. logrotate
  // rotates our audit logs.
  MG_ASSERT(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::User2,
                                                            [&audit_log]() { audit_log.ReopenLog(); }),
            "Unable to register SIGUSR2 handler!");

  // End enterprise features initialization
#endif

  // Main storage and execution engines initialization
  memgraph::storage::Config db_config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC,
             .interval = std::chrono::seconds(FLAGS_storage_gc_cycle_sec)},
      .items = {.properties_on_edges = FLAGS_storage_properties_on_edges},
      .durability = {.storage_directory = FLAGS_data_directory,
                     .recover_on_startup = FLAGS_storage_recover_on_startup,
                     .snapshot_retention_count = FLAGS_storage_snapshot_retention_count,
                     .wal_file_size_kibibytes = FLAGS_storage_wal_file_size_kib,
                     .wal_file_flush_every_n_tx = FLAGS_storage_wal_file_flush_every_n_tx,
                     .snapshot_on_exit = FLAGS_storage_snapshot_on_exit,
                     .restore_replicas_on_startup = true,
                     .items_per_batch = FLAGS_storage_items_per_batch,
                     .recovery_thread_count = FLAGS_storage_recovery_thread_count,
                     .allow_parallel_index_creation = FLAGS_storage_parallel_index_recovery},
      .transaction = {.isolation_level = ParseIsolationLevel()}};
  if (FLAGS_storage_snapshot_interval_sec == 0) {
    if (FLAGS_storage_wal_enabled) {
      LOG_FATAL(
          "In order to use write-ahead-logging you must enable "
          "periodic snapshots by setting the snapshot interval to a "
          "value larger than 0!");
      db_config.durability.snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::DISABLED;
    }
  } else {
    if (FLAGS_storage_wal_enabled) {
      db_config.durability.snapshot_wal_mode =
          memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    } else {
      db_config.durability.snapshot_wal_mode =
          memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    }
    db_config.durability.snapshot_interval = std::chrono::seconds(FLAGS_storage_snapshot_interval_sec);
  }
  memgraph::storage::Storage db(db_config);

  memgraph::query::InterpreterContext interpreter_context{
      &db,
      {.query = {.allow_load_csv = FLAGS_allow_load_csv},
       .execution_timeout_sec = FLAGS_query_execution_timeout_sec,
       .replication_replica_check_frequency = std::chrono::seconds(FLAGS_replication_replica_check_frequency_sec),
       .default_kafka_bootstrap_servers = FLAGS_kafka_bootstrap_servers,
       .default_pulsar_service_url = FLAGS_pulsar_service_url,
       .stream_transaction_conflict_retries = FLAGS_stream_transaction_conflict_retries,
       .stream_transaction_retry_interval = std::chrono::milliseconds(FLAGS_stream_transaction_retry_interval)},
      FLAGS_data_directory};
#ifdef MG_ENTERPRISE
  SessionData session_data{&db, &interpreter_context, &auth, &audit_log};
#else
  SessionData session_data{&db, &interpreter_context, &auth};
#endif

  memgraph::query::procedure::gModuleRegistry.SetModulesDirectory(query_modules_directories, FLAGS_data_directory);
  memgraph::query::procedure::gModuleRegistry.UnloadAndLoadModulesFromDirectories();

  memgraph::glue::AuthQueryHandler auth_handler(&auth, FLAGS_auth_user_or_role_name_regex);
  memgraph::glue::AuthChecker auth_checker{&auth};
  interpreter_context.auth = &auth_handler;
  interpreter_context.auth_checker = &auth_checker;

  if (!FLAGS_init_file.empty()) {
    spdlog::info("Running init file.");
#ifdef MG_ENTERPRISE
    if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
      InitFromCypherlFile(interpreter_context, FLAGS_init_file, &audit_log);
    }
#else
    InitFromCypherlFile(interpreter_context, FLAGS_init_file);
#endif
  }

  auto *maybe_username = std::getenv(kMgUser);
  auto *maybe_password = std::getenv(kMgPassword);
  auto *maybe_pass_file = std::getenv(kMgPassfile);
  if (maybe_username && maybe_password) {
    auth_handler.CreateUser(maybe_username, maybe_password);
  } else if (maybe_pass_file) {
    const auto [username, password] = LoadUsernameAndPassword(maybe_pass_file);
    if (!username.empty() && !password.empty()) {
      auth_handler.CreateUser(username, password);
    }
  }

  {
    // Triggers can execute query procedures, so we need to reload the modules first and then
    // the triggers
    auto storage_accessor = interpreter_context.db->Access();
    auto dba = memgraph::query::DbAccessor{&storage_accessor};
    interpreter_context.trigger_store.RestoreTriggers(
        &interpreter_context.ast_cache, &dba, interpreter_context.config.query, interpreter_context.auth_checker);
  }

  // As the Stream transformations are using modules, they have to be restored after the query modules are loaded.
  interpreter_context.streams.RestoreStreams();

  ServerContext context;
  std::string service_name = "Bolt";
  if (!FLAGS_bolt_key_file.empty() && !FLAGS_bolt_cert_file.empty()) {
    context = ServerContext(FLAGS_bolt_key_file, FLAGS_bolt_cert_file);
    service_name = "BoltS";
    spdlog::info("Using secure Bolt connection (with SSL)");
  } else {
    spdlog::warn(
        memgraph::utils::MessageWithLink("Using non-secure Bolt connection (without SSL).", "https://memgr.ph/ssl"));
  }

  auto server_endpoint = memgraph::communication::v2::ServerEndpoint{
      boost::asio::ip::address::from_string(FLAGS_bolt_address), static_cast<uint16_t>(FLAGS_bolt_port)};
  ServerT server(server_endpoint, &session_data, &context, FLAGS_bolt_session_inactivity_timeout, service_name,
                 FLAGS_bolt_num_workers);

  const auto run_id = memgraph::utils::GenerateUUID();
  const auto machine_id = memgraph::utils::GetMachineId();
  session_data.run_id = run_id;

  // Setup telemetry
  static constexpr auto telemetry_server{"https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/"};
  std::optional<memgraph::telemetry::Telemetry> telemetry;
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace(telemetry_server, data_directory / "telemetry", run_id, machine_id, std::chrono::minutes(10));
    telemetry->AddCollector("storage", [&db]() -> nlohmann::json {
      auto info = db.GetInfo();
      return {{"vertices", info.vertex_count}, {"edges", info.edge_count}};
    });
    telemetry->AddCollector("event_counters", []() -> nlohmann::json {
      nlohmann::json ret;
      for (size_t i = 0; i < memgraph::metrics::CounterEnd(); ++i) {
        ret[memgraph::metrics::GetCounterName(i)] =
            memgraph::metrics::global_counters[i].load(std::memory_order_relaxed);
      }
      return ret;
    });
    telemetry->AddCollector("query_module_counters", []() -> nlohmann::json {
      return memgraph::query::plan::CallProcedure::GetAndResetCounters();
    });
  }
  memgraph::license::LicenseInfoSender license_info_sender(telemetry_server, run_id, machine_id, memory_limit,
                                                           memgraph::license::global_license_checker.GetLicenseInfo());

  memgraph::communication::websocket::SafeAuth websocket_auth{&auth};
  memgraph::communication::websocket::Server websocket_server{
      {FLAGS_monitoring_address, static_cast<uint16_t>(FLAGS_monitoring_port)}, &context, websocket_auth};
  AddLoggerSink(websocket_server.GetLoggingSink());

  MonitoringServerT metrics_server{
      {FLAGS_metrics_address, static_cast<uint16_t>(FLAGS_metrics_port)}, &session_data, &context};

#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    // Handler for regular termination signals
    auto shutdown = [&metrics_server, &websocket_server, &server, &interpreter_context] {
      // Server needs to be shutdown first and then the database. This prevents
      // a race condition when a transaction is accepted during server shutdown.
      server.Shutdown();
      // After the server is notified to stop accepting and processing
      // connections we tell the execution engine to stop processing all pending
      // queries.
      memgraph::query::Shutdown(&interpreter_context);

      websocket_server.Shutdown();
      metrics_server.Shutdown();
    };

    InitSignalHandlers(shutdown);
  } else {
    // Handler for regular termination signals
    auto shutdown = [&websocket_server, &server, &interpreter_context] {
      // Server needs to be shutdown first and then the database. This prevents
      // a race condition when a transaction is accepted during server shutdown.
      server.Shutdown();
      // After the server is notified to stop accepting and processing
      // connections we tell the execution engine to stop processing all pending
      // queries.
      memgraph::query::Shutdown(&interpreter_context);

      websocket_server.Shutdown();
    };

    InitSignalHandlers(shutdown);
  }
#else
  // Handler for regular termination signals
  auto shutdown = [&websocket_server, &server, &interpreter_context] {
    // Server needs to be shutdown first and then the database. This prevents
    // a race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    // After the server is notified to stop accepting and processing
    // connections we tell the execution engine to stop processing all pending
    // queries.
    memgraph::query::Shutdown(&interpreter_context);

    websocket_server.Shutdown();
  };

  InitSignalHandlers(shutdown);
#endif

  MG_ASSERT(server.Start(), "Couldn't start the Bolt server!");
  websocket_server.Start();

#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    metrics_server.Start();
  }
#endif

  if (!FLAGS_init_data_file.empty()) {
    spdlog::info("Running init data file.");
#ifdef MG_ENTERPRISE
    if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
      InitFromCypherlFile(interpreter_context, FLAGS_init_data_file, &audit_log);
    }
#else
    InitFromCypherlFile(interpreter_context, FLAGS_init_data_file);
#endif
  }

  server.AwaitShutdown();
  websocket_server.AwaitShutdown();
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    metrics_server.AwaitShutdown();
  }
#endif

  memgraph::query::procedure::gModuleRegistry.UnloadAllModules();

  Py_END_ALLOW_THREADS;
  // Shutdown Python
  Py_Finalize();
  PyMem_RawFree(program_name);

  memgraph::utils::total_memory_tracker.LogPeakMemoryUsage();
  return 0;
}
