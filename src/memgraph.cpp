// Copyright 2022 Memgraph Ltd.
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
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <spdlog/common.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/dist_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "communication/bolt/v1/constants.hpp"
#include "communication/websocket/auth.hpp"
#include "communication/websocket/server.hpp"
#include "coordinator/shard_map.hpp"
#include "helpers.hpp"
#include "io/address.hpp"
#include "io/local_transport/local_system.hpp"
#include "io/local_transport/local_transport.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "machine_manager/machine_config.hpp"
#include "machine_manager/machine_manager.hpp"
#include "py/py.hpp"
#include "query/v2/discard_value_stream.hpp"
#include "query/v2/exceptions.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/interpreter.hpp"
#include "query/v2/plan/operator.hpp"
#include "requests/requests.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/isolation_level.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/view.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/event_counter.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/license.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/readable_size.hpp"
#include "utils/rw_lock.hpp"
#include "utils/settings.hpp"
#include "utils/signals.hpp"
#include "utils/string.hpp"
#include "utils/sysinfo/memory.hpp"
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
#include "glue/v2/communication.hpp"

#include "auth/auth.hpp"
#include "glue/v2/auth.hpp"

#ifdef MG_ENTERPRISE
#include "audit/log.hpp"
#endif

namespace {
std::string GetAllowedEnumValuesString(const auto &mappings) {
  std::vector<std::string> allowed_values;
  allowed_values.reserve(mappings.size());
  std::transform(mappings.begin(), mappings.end(), std::back_inserter(allowed_values),
                 [](const auto &mapping) { return std::string(mapping.first); });
  return memgraph::utils::Join(allowed_values, ", ");
}

enum class ValidationError : uint8_t { EmptyValue, InvalidValue };

memgraph::utils::BasicResult<ValidationError> IsValidEnumValueString(const auto &value, const auto &mappings) {
  if (value.empty()) {
    return ValidationError::EmptyValue;
  }

  if (std::find_if(mappings.begin(), mappings.end(), [&](const auto &mapping) { return mapping.first == value; }) ==
      mappings.cend()) {
    return ValidationError::InvalidValue;
  }

  return {};
}

template <typename Enum>
std::optional<Enum> StringToEnum(const auto &value, const auto &mappings) {
  const auto mapping_iter =
      std::find_if(mappings.begin(), mappings.end(), [&](const auto &mapping) { return mapping.first == value; });
  if (mapping_iter == mappings.cend()) {
    return std::nullopt;
  }

  return mapping_iter->second;
}
}  // namespace

// Bolt server flags.
DEFINE_string(bolt_address, "0.0.0.0", "IP address on which the Bolt server should listen.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(monitoring_address, "0.0.0.0",
              "IP address on which the websocket server for Memgraph monitoring should listen.");
DEFINE_VALIDATED_int32(bolt_port, 7687, "Port on which the Bolt server should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(monitoring_port, 7444,
                       "Port on which the websocket server for Memgraph monitoring should listen.",
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

// General purpose flags.
// NOTE: The `data_directory` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(data_directory, "mg_data", "Path to directory in which to save all permanent data.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_string(log_link_basename, "", "Basename used for symlink creation to the last log file.");
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
DEFINE_bool(storage_snapshot_on_exit, false, "Controls whether the storage creates another snapshot on exit.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(storage_restore_replicas_on_startup, true,
            "Controls replicas should be restored automatically.");  // TODO(42jeremy) this must be removed once T0835
                                                                     // is implemented.

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

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(split_file, "",
              "Path to the split file which contains the predefined labels, properties, edge types and shard-ranges.");

namespace {
using namespace std::literals;
inline constexpr std::array isolation_level_mappings{
    std::pair{"SNAPSHOT_ISOLATION"sv, memgraph::storage::v3::IsolationLevel::SNAPSHOT_ISOLATION},
    std::pair{"READ_COMMITTED"sv, memgraph::storage::v3::IsolationLevel::READ_COMMITTED},
    std::pair{"READ_UNCOMMITTED"sv, memgraph::storage::v3::IsolationLevel::READ_UNCOMMITTED}};

const std::string isolation_level_help_string =
    fmt::format("Default isolation level used for the transactions. Allowed values: {}",
                GetAllowedEnumValuesString(isolation_level_mappings));
}  // namespace

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(isolation_level, "SNAPSHOT_ISOLATION", isolation_level_help_string.c_str(), {
  if (const auto result = IsValidEnumValueString(value, isolation_level_mappings); result.HasError()) {
    const auto error = result.GetError();
    switch (error) {
      case ValidationError::EmptyValue: {
        std::cout << "Isolation level cannot be empty." << std::endl;
        break;
      }
      case ValidationError::InvalidValue: {
        std::cout << "Invalid value for isolation level. Allowed values: "
                  << GetAllowedEnumValuesString(isolation_level_mappings) << std::endl;
        break;
      }
    }
    return false;
  }

  return true;
});

namespace {
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

// Logging flags
DEFINE_bool(also_log_to_stderr, false, "Log messages go to stderr in addition to logfiles");
DEFINE_string(log_file, "", "Path to where the log should be stored.");

namespace {
inline constexpr std::array log_level_mappings{
    std::pair{"TRACE"sv, spdlog::level::trace}, std::pair{"DEBUG"sv, spdlog::level::debug},
    std::pair{"INFO"sv, spdlog::level::info},   std::pair{"WARNING"sv, spdlog::level::warn},
    std::pair{"ERROR"sv, spdlog::level::err},   std::pair{"CRITICAL"sv, spdlog::level::critical}};

const std::string log_level_help_string =
    fmt::format("Minimum log level. Allowed values: {}", GetAllowedEnumValuesString(log_level_mappings));
}  // namespace

DEFINE_VALIDATED_string(log_level, "WARNING", log_level_help_string.c_str(), {
  if (const auto result = IsValidEnumValueString(value, log_level_mappings); result.HasError()) {
    const auto error = result.GetError();
    switch (error) {
      case ValidationError::EmptyValue: {
        std::cout << "Log level cannot be empty." << std::endl;
        break;
      }
      case ValidationError::InvalidValue: {
        std::cout << "Invalid value for log level. Allowed values: " << GetAllowedEnumValuesString(log_level_mappings)
                  << std::endl;
        break;
      }
    }
    return false;
  }

  return true;
});

namespace {
spdlog::level::level_enum ParseLogLevel() {
  const auto log_level = StringToEnum<spdlog::level::level_enum>(FLAGS_log_level, log_level_mappings);
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

}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(license_key, "", "License key for Memgraph Enterprise.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(organization_name, "", "Organization name.");

/// Encapsulates Dbms and Interpreter that are passed through the network server
/// and worker to the session.
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
  SessionData(memgraph::coordinator::ShardMap &shard_map, memgraph::query::v2::InterpreterContext *interpreter_context)
      : shard_map(&shard_map), interpreter_context(interpreter_context) {}
  memgraph::coordinator::ShardMap *shard_map;
  memgraph::query::v2::InterpreterContext *interpreter_context;
};

inline constexpr std::string_view default_user_role_regex = "[a-zA-Z0-9_.+-@]+";
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)

class BoltSession final : public memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                                                        memgraph::communication::v2::OutputStream> {
 public:
  BoltSession(SessionData &data, const memgraph::communication::v2::ServerEndpoint &endpoint,
              memgraph::communication::v2::InputStream *input_stream,
              memgraph::communication::v2::OutputStream *output_stream)
      : memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>(input_stream, output_stream),
        shard_map_(data.shard_map),
        interpreter_(data.interpreter_context),
        endpoint_(endpoint) {}

  using memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>::TEncoder;

  void BeginTransaction() override { interpreter_.BeginTransaction(); }

  void CommitTransaction() override { interpreter_.CommitTransaction(); }

  void RollbackTransaction() override { interpreter_.RollbackTransaction(); }

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, memgraph::communication::bolt::Value> &params) override {
    std::map<std::string, memgraph::storage::v3::PropertyValue> params_pv;
    for (const auto &kv : params) params_pv.emplace(kv.first, memgraph::glue::v2::ToPropertyValue(kv.second));
    const std::string *username{nullptr};
    try {
      auto result = interpreter_.Prepare(query, params_pv, username);
      return {result.headers, result.qid};

    } catch (const memgraph::query::v2::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw memgraph::communication::bolt::ClientError(e.what());
    }
  }

  std::map<std::string, memgraph::communication::bolt::Value> Pull(TEncoder *encoder, std::optional<int> n,
                                                                   std::optional<int> qid) override {
    TypedValueResultStream stream(encoder, *shard_map_);
    return PullResults(stream, n, qid);
  }

  std::map<std::string, memgraph::communication::bolt::Value> Discard(std::optional<int> n,
                                                                      std::optional<int> qid) override {
    memgraph::query::v2::DiscardValueResultStream stream;
    return PullResults(stream, n, qid);
  }

  void Abort() override { interpreter_.Abort(); }

  bool Authenticate(const std::string & /*username*/, const std::string & /*password*/) override { return true; }

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
        auto maybe_value = memgraph::glue::v2::ToBoltValue(kv.second, *shard_map_, memgraph::storage::v3::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case memgraph::storage::v3::Error::DELETED_OBJECT:
            case memgraph::storage::v3::Error::SERIALIZATION_ERROR:
            case memgraph::storage::v3::Error::VERTEX_HAS_EDGES:
            case memgraph::storage::v3::Error::PROPERTIES_DISABLED:
            case memgraph::storage::v3::Error::NONEXISTENT_OBJECT:
              throw memgraph::communication::bolt::ClientError("Unexpected storage error when streaming summary.");
          }
        }
        decoded_summary.emplace(kv.first, std::move(*maybe_value));
      }
      return decoded_summary;
    } catch (const memgraph::query::v2::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw memgraph::communication::bolt::ClientError(e.what());
    }
  }

  /// Wrapper around TEncoder which converts TypedValue to Value
  /// before forwarding the calls to original TEncoder.
  class TypedValueResultStream {
   public:
    TypedValueResultStream(TEncoder *encoder, const memgraph::coordinator::ShardMap &shard_map)
        : encoder_(encoder), shard_map_(&shard_map) {}

    void Result(const std::vector<memgraph::query::v2::TypedValue> &values) {
      std::vector<memgraph::communication::bolt::Value> decoded_values;
      decoded_values.reserve(values.size());
      for (const auto &v : values) {
        auto maybe_value = memgraph::glue::v2::ToBoltValue(v, *shard_map_, memgraph::storage::v3::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case memgraph::storage::v3::Error::DELETED_OBJECT:
              throw memgraph::communication::bolt::ClientError("Returning a deleted object as a result.");
            case memgraph::storage::v3::Error::NONEXISTENT_OBJECT:
              throw memgraph::communication::bolt::ClientError("Returning a nonexistent object as a result.");
            case memgraph::storage::v3::Error::VERTEX_HAS_EDGES:
            case memgraph::storage::v3::Error::SERIALIZATION_ERROR:
            case memgraph::storage::v3::Error::PROPERTIES_DISABLED:
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
    const memgraph::coordinator::ShardMap *shard_map_;
  };

  // NOTE: Needed only for ToBoltValue conversions
  const memgraph::coordinator::ShardMap *shard_map_;
  memgraph::query::v2::Interpreter interpreter_;
  memgraph::communication::v2::ServerEndpoint endpoint_;
};

using ServerT = memgraph::communication::v2::Server<BoltSession, SessionData>;
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
  MG_ASSERT(memgraph::utils::SignalHandler::RegisterHandler(memgraph::utils::Signal::Interupt, shutdown,
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

  InitializeLogger();

  // Unhandled exception handler init.
  std::set_terminate(&memgraph::utils::TerminateHandler);

  // Initialize the communication library.
  memgraph::communication::SSLInit sslInit;

  // Initialize the requests library.
  memgraph::requests::Init();

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
  memgraph::utils::license::RegisterLicenseSettings(memgraph::utils::license::global_license_checker,
                                                    memgraph::utils::global_settings);

  memgraph::utils::license::global_license_checker.CheckEnvLicense();
  if (!FLAGS_organization_name.empty() && !FLAGS_license_key.empty()) {
    memgraph::utils::license::global_license_checker.SetLicenseInfoOverride(FLAGS_license_key, FLAGS_organization_name);
  }

  memgraph::utils::license::global_license_checker.StartBackgroundLicenseChecker(memgraph::utils::global_settings);

  // All enterprise features should be constructed before the main database
  // storage. This will cause them to be destructed *after* the main database
  // storage. That way any errors that happen during enterprise features
  // destruction won't have an impact on the storage engine.
  // Example: When the main storage is destructed it makes a snapshot. When
  // audit logging is destructed it syncs all pending data to disk and that can
  // fail. That is why it must be destructed *after* the main database storage
  // to minimize the impact of their failure on the main storage.

  memgraph::io::local_transport::LocalSystem ls;
  auto unique_local_addr_query = memgraph::coordinator::Address::UniqueLocalAddress();
  auto io = ls.Register(unique_local_addr_query);

  memgraph::machine_manager::MachineConfig config{
      .coordinator_addresses = std::vector<memgraph::io::Address>{unique_local_addr_query},
      .is_storage = true,
      .is_coordinator = true,
      .listen_ip = unique_local_addr_query.last_known_ip,
      .listen_port = unique_local_addr_query.last_known_port,
  };

  memgraph::coordinator::ShardMap sm;
  if (FLAGS_split_file.empty()) {
    const std::string property{"property"};
    const std::string label{"label"};
    auto prop_map = sm.AllocatePropertyIds(std::vector<std::string>{property});
    auto edge_type_map = sm.AllocateEdgeTypeIds(std::vector<std::string>{"TO"});
    std::vector<memgraph::storage::v3::SchemaProperty> schema{
        {prop_map.at(property), memgraph::common::SchemaType::INT}};
    sm.InitializeNewLabel(label, schema, 1, sm.shard_map_version);
    sm.SplitShard(sm.GetHlc(), *sm.GetLabelId(label),
                  std::vector<memgraph::storage::v3::PropertyValue>{memgraph::storage::v3::PropertyValue{2}});
  } else {
    std::ifstream input{FLAGS_split_file, std::ios::in};
    MG_ASSERT(input.is_open(), "Cannot open split file to read: {}", FLAGS_split_file);
    sm = memgraph::coordinator::ShardMap::Parse(input);
  }

  memgraph::coordinator::Coordinator coordinator{sm};

  memgraph::machine_manager::MachineManager<memgraph::io::local_transport::LocalTransport> mm{io, config, coordinator,
                                                                                              sm};
  std::jthread mm_thread([&mm] { mm.Run(); });

  memgraph::query::v2::InterpreterContext interpreter_context{
      (memgraph::storage::v3::Shard *)(nullptr),
      {.query = {.allow_load_csv = FLAGS_allow_load_csv},
       .execution_timeout_sec = FLAGS_query_execution_timeout_sec,
       .replication_replica_check_frequency = std::chrono::seconds(FLAGS_replication_replica_check_frequency_sec),
       .default_kafka_bootstrap_servers = FLAGS_kafka_bootstrap_servers,
       .default_pulsar_service_url = FLAGS_pulsar_service_url,
       .stream_transaction_conflict_retries = FLAGS_stream_transaction_conflict_retries,
       .stream_transaction_retry_interval = std::chrono::milliseconds(FLAGS_stream_transaction_retry_interval)},
      FLAGS_data_directory,
      std::move(io),
      mm.CoordinatorAddress()};

  SessionData session_data{sm, &interpreter_context};

  interpreter_context.auth = nullptr;
  interpreter_context.auth_checker = nullptr;

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
  ServerT server(server_endpoint, session_data, &context, FLAGS_bolt_session_inactivity_timeout, service_name,
                 FLAGS_bolt_num_workers);

  // Setup telemetry
  std::optional<memgraph::telemetry::Telemetry> telemetry;

  // Handler for regular termination signals
  auto shutdown = [&server, &interpreter_context, &ls] {
    // Server needs to be shutdown first and then the database. This prevents
    // a race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    // After the server is notified to stop accepting and processing
    // connections we tell the execution engine to stop processing all pending
    // queries.
    memgraph::query::v2::Shutdown(&interpreter_context);
    ls.ShutDown();
  };

  InitSignalHandlers(shutdown);

  MG_ASSERT(server.Start(), "Couldn't start the Bolt server!");
  server.AwaitShutdown();

  memgraph::utils::total_memory_tracker.LogPeakMemoryUsage();
  return 0;
}
