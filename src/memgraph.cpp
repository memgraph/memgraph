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
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <spdlog/common.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "communication/bolt/v1/constants.hpp"
#include "helpers.hpp"
#include "py/py.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/plan/operator.hpp"
#include "query/procedure/module.hpp"
#include "query/procedure/py_module.hpp"
#include "requests/requests.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/event_counter.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/readable_size.hpp"
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
#include "communication/server.hpp"
#include "communication/session.hpp"
#include "glue/communication.hpp"

#ifdef MG_ENTERPRISE
#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "glue/auth.hpp"
#endif

namespace {
std::string GetAllowedEnumValuesString(const auto &mappings) {
  std::vector<std::string> allowed_values;
  allowed_values.reserve(mappings.size());
  std::transform(mappings.begin(), mappings.end(), std::back_inserter(allowed_values),
                 [](const auto &mapping) { return std::string(mapping.first); });
  return utils::Join(allowed_values, ", ");
}

enum class ValidationError : uint8_t { EmptyValue, InvalidValue };

utils::BasicResult<ValidationError> IsValidEnumValueString(const auto &value, const auto &mappings) {
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
DEFINE_VALIDATED_int32(bolt_port, 7687, "Port on which the Bolt server should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
DEFINE_VALIDATED_int32(bolt_num_workers, std::max(std::thread::hardware_concurrency(), 1U),
                       "Number of workers used by the Bolt server. By default, this will be the "
                       "number of processing units available on the machine.",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(bolt_session_inactivity_timeout, 1800,
                       "Time in seconds after which inactive Bolt sessions will be "
                       "closed.",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_string(bolt_cert_file, "", "Certificate file which should be used for the Bolt server.");
DEFINE_string(bolt_key_file, "", "Key file which should be used for the Bolt server.");
DEFINE_string(bolt_server_name_for_init, "",
              "Server name which the database should send to the client in the "
              "Bolt INIT message.");

// General purpose flags.
// NOTE: The `data_directory` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
DEFINE_string(data_directory, "mg_data", "Path to directory in which to save all permanent data.");
DEFINE_HIDDEN_string(log_link_basename, "", "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");

// Storage flags.
DEFINE_VALIDATED_uint64(storage_gc_cycle_sec, 30, "Storage garbage collector interval (in seconds).",
                        FLAG_IN_RANGE(1, 24 * 3600));
// NOTE: The `storage_properties_on_edges` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
DEFINE_bool(storage_properties_on_edges, false, "Controls whether edges have properties.");
DEFINE_bool(storage_recover_on_startup, false, "Controls whether the storage recovers persisted data on startup.");
DEFINE_VALIDATED_uint64(storage_snapshot_interval_sec, 0,
                        "Storage snapshot creation interval (in seconds). Set "
                        "to 0 to disable periodic snapshot creation.",
                        FLAG_IN_RANGE(0, 7 * 24 * 3600));
DEFINE_bool(storage_wal_enabled, false,
            "Controls whether the storage uses write-ahead-logging. To enable "
            "WAL periodic snapshots must be enabled.");
DEFINE_VALIDATED_uint64(storage_snapshot_retention_count, 3, "The number of snapshots that should always be kept.",
                        FLAG_IN_RANGE(1, 1000000));
DEFINE_VALIDATED_uint64(storage_wal_file_size_kib, storage::Config::Durability().wal_file_size_kibibytes,
                        "Minimum file size of each WAL file.", FLAG_IN_RANGE(1, 1000 * 1024));
DEFINE_VALIDATED_uint64(storage_wal_file_flush_every_n_tx, storage::Config::Durability().wal_file_flush_every_n_tx,
                        "Issue a 'fsync' call after this amount of transactions are written to the "
                        "WAL file. Set to 1 for fully synchronous operation.",
                        FLAG_IN_RANGE(1, 1000000));
DEFINE_bool(storage_snapshot_on_exit, false, "Controls whether the storage creates another snapshot on exit.");

DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");

// Audit logging flags.
#ifdef MG_ENTERPRISE
DEFINE_bool(audit_enabled, false, "Set to true to enable audit logging.");
DEFINE_VALIDATED_int32(audit_buffer_size, audit::kBufferSizeDefault, "Maximum number of items in the audit log buffer.",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(audit_buffer_flush_interval_ms, audit::kBufferFlushIntervalMillisDefault,
                       "Interval (in milliseconds) used for flushing the audit log buffer.",
                       FLAG_IN_RANGE(10, INT32_MAX));
#endif

// Query flags.
DEFINE_uint64(query_execution_timeout_sec, 180,
              "Maximum allowed query execution time. Queries exceeding this "
              "limit will be aborted. Value of 0 means no limit.");

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint64(
    memory_limit, 0,
    "Total memory limit in MiB. Set to 0 to use the default values which are 100\% of the phyisical memory if the swap "
    "is enabled and 90\% of the physical memory otherwise.");

namespace {
using namespace std::literals;
constexpr std::array isolation_level_mappings{
    std::pair{"SNAPSHOT_ISOLATION"sv, storage::IsolationLevel::SNAPSHOT_ISOLATION},
    std::pair{"READ_COMMITTED"sv, storage::IsolationLevel::READ_COMMITTED},
    std::pair{"READ_UNCOMMITTED"sv, storage::IsolationLevel::READ_UNCOMMITTED}};

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
storage::IsolationLevel ParseIsolationLevel() {
  const auto isolation_level = StringToEnum<storage::IsolationLevel>(FLAGS_isolation_level, isolation_level_mappings);
  MG_ASSERT(isolation_level, "Invalid isolation level");
  return *isolation_level;
}

int64_t GetMemoryLimit() {
  if (FLAGS_memory_limit == 0) {
    auto maybe_total_memory = utils::sysinfo::TotalMemory();
    MG_ASSERT(maybe_total_memory, "Failed to fetch the total physical memory");
    const auto maybe_swap_memory = utils::sysinfo::SwapTotalMemory();
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
                          const auto directories = utils::Split(value, ",");
                          for (const auto &dir : directories) {
                            if (!utils::DirExists(dir)) {
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
DEFINE_bool(also_log_to_stderr, false, "Log messages go to stderr in addition to logfiles");
DEFINE_string(log_file, "", "Path to where the log should be stored.");

namespace {
constexpr std::array log_level_mappings{
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
void ParseLogLevel() {
  const auto log_level = StringToEnum<spdlog::level::level_enum>(FLAGS_log_level, log_level_mappings);
  MG_ASSERT(log_level, "Invalid log level");
  spdlog::set_level(*log_level);
}

// 5 weeks * 7 days
constexpr auto log_retention_count = 35;

void ConfigureLogging() {
  std::vector<spdlog::sink_ptr> loggers;

  if (FLAGS_also_log_to_stderr) {
    loggers.emplace_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
  }

  if (!FLAGS_log_file.empty()) {
    // get local time
    time_t current_time;
    struct tm *local_time{nullptr};

    time(&current_time);
    local_time = localtime(&current_time);

    loggers.emplace_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>(
        FLAGS_log_file, local_time->tm_hour, local_time->tm_min, false, log_retention_count));
  }

  spdlog::set_default_logger(std::make_shared<spdlog::logger>("memgraph_log", loggers.begin(), loggers.end()));

  spdlog::flush_on(spdlog::level::trace);
  ParseLogLevel();
}
}  // namespace

/// Encapsulates Dbms and Interpreter that are passed through the network server
/// and worker to the session.
#ifdef MG_ENTERPRISE
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
  SessionData(storage::Storage *db, query::InterpreterContext *interpreter_context, auth::Auth *auth,
              audit::Log *audit_log)
      : db(db), interpreter_context(interpreter_context), auth(auth), audit_log(audit_log) {}
  storage::Storage *db;
  query::InterpreterContext *interpreter_context;
  auth::Auth *auth;
  audit::Log *audit_log;
};
#else
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
  SessionData(storage::Storage *db, query::InterpreterContext *interpreter_context)
      : db(db), interpreter_context(interpreter_context) {}
  storage::Storage *db;
  query::InterpreterContext *interpreter_context;
};
#endif

class BoltSession final : public communication::bolt::Session<communication::InputStream, communication::OutputStream> {
 public:
  BoltSession(SessionData *data, const io::network::Endpoint &endpoint, communication::InputStream *input_stream,
              communication::OutputStream *output_stream)
      : communication::bolt::Session<communication::InputStream, communication::OutputStream>(input_stream,
                                                                                              output_stream),
        db_(data->db),
        interpreter_(data->interpreter_context),
#ifdef MG_ENTERPRISE
        auth_(data->auth),
        audit_log_(data->audit_log),
#endif
        endpoint_(endpoint) {
  }

  using communication::bolt::Session<communication::InputStream, communication::OutputStream>::TEncoder;

  void BeginTransaction() override { interpreter_.BeginTransaction(); }

  void CommitTransaction() override { interpreter_.CommitTransaction(); }

  void RollbackTransaction() override { interpreter_.RollbackTransaction(); }

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, communication::bolt::Value> &params) override {
    std::map<std::string, storage::PropertyValue> params_pv;
    for (const auto &kv : params) params_pv.emplace(kv.first, glue::ToPropertyValue(kv.second));
#ifdef MG_ENTERPRISE
    audit_log_->Record(endpoint_.address, user_ ? user_->username() : "", query, storage::PropertyValue(params_pv));
#endif
    try {
      auto result = interpreter_.Prepare(query, params_pv);
#ifdef MG_ENTERPRISE
      if (user_) {
        const auto &permissions = user_->GetPermissions();
        for (const auto &privilege : result.privileges) {
          if (permissions.Has(glue::PrivilegeToPermission(privilege)) != auth::PermissionLevel::GRANT) {
            interpreter_.Abort();
            throw communication::bolt::ClientError(
                "You are not authorized to execute this query! Please contact "
                "your database administrator.");
          }
        }
      }
#endif
      return {result.headers, result.qid};

    } catch (const query::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw communication::bolt::ClientError(e.what());
    }
  }

  std::map<std::string, communication::bolt::Value> Pull(TEncoder *encoder, std::optional<int> n,
                                                         std::optional<int> qid) override {
    TypedValueResultStream stream(encoder, db_);
    return PullResults(stream, n, qid);
  }

  std::map<std::string, communication::bolt::Value> Discard(std::optional<int> n, std::optional<int> qid) override {
    DiscardValueResultStream stream;
    return PullResults(stream, n, qid);
  }

  void Abort() override { interpreter_.Abort(); }

  bool Authenticate(const std::string &username, const std::string &password) override {
#ifdef MG_ENTERPRISE
    if (!auth_->HasUsers()) return true;
    user_ = auth_->Authenticate(username, password);
    return !!user_;
#else
    return true;
#endif
  }

  std::optional<std::string> GetServerNameForInit() override {
    if (FLAGS_bolt_server_name_for_init.empty()) return std::nullopt;
    return FLAGS_bolt_server_name_for_init;
  }

 private:
  template <typename TStream>
  std::map<std::string, communication::bolt::Value> PullResults(TStream &stream, std::optional<int> n,
                                                                std::optional<int> qid) {
    try {
      const auto &summary = interpreter_.Pull(&stream, n, qid);
      std::map<std::string, communication::bolt::Value> decoded_summary;
      for (const auto &kv : summary) {
        auto maybe_value = glue::ToBoltValue(kv.second, *db_, storage::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case storage::Error::DELETED_OBJECT:
            case storage::Error::SERIALIZATION_ERROR:
            case storage::Error::VERTEX_HAS_EDGES:
            case storage::Error::PROPERTIES_DISABLED:
            case storage::Error::NONEXISTENT_OBJECT:
              throw communication::bolt::ClientError("Unexpected storage error when streaming summary.");
          }
        }
        decoded_summary.emplace(kv.first, std::move(*maybe_value));
      }
      return decoded_summary;
    } catch (const query::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw communication::bolt::ClientError(e.what());
    }
  }

  /// Wrapper around TEncoder which converts TypedValue to Value
  /// before forwarding the calls to original TEncoder.
  class TypedValueResultStream {
   public:
    TypedValueResultStream(TEncoder *encoder, const storage::Storage *db) : encoder_(encoder), db_(db) {}

    void Result(const std::vector<query::TypedValue> &values) {
      std::vector<communication::bolt::Value> decoded_values;
      decoded_values.reserve(values.size());
      for (const auto &v : values) {
        auto maybe_value = glue::ToBoltValue(v, *db_, storage::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case storage::Error::DELETED_OBJECT:
              throw communication::bolt::ClientError("Returning a deleted object as a result.");
            case storage::Error::NONEXISTENT_OBJECT:
              throw communication::bolt::ClientError("Returning a nonexistent object as a result.");
            case storage::Error::VERTEX_HAS_EDGES:
            case storage::Error::SERIALIZATION_ERROR:
            case storage::Error::PROPERTIES_DISABLED:
              throw communication::bolt::ClientError("Unexpected storage error when streaming results.");
          }
        }
        decoded_values.emplace_back(std::move(*maybe_value));
      }
      encoder_->MessageRecord(decoded_values);
    }

   private:
    TEncoder *encoder_;
    // NOTE: Needed only for ToBoltValue conversions
    const storage::Storage *db_;
  };

  struct DiscardValueResultStream {
    void Result(const std::vector<query::TypedValue> &) {
      // do nothing
    }
  };

  // NOTE: Needed only for ToBoltValue conversions
  const storage::Storage *db_;
  query::Interpreter interpreter_;
#ifdef MG_ENTERPRISE
  auth::Auth *auth_;
  std::optional<auth::User> user_;
  audit::Log *audit_log_;
#endif
  io::network::Endpoint endpoint_;
};

using ServerT = communication::Server<BoltSession, SessionData>;
using communication::ServerContext;

#ifdef MG_ENTERPRISE
DEFINE_string(auth_user_or_role_name_regex, "[a-zA-Z0-9_.+-@]+",
              "Set to the regular expression that each user or role name must fulfill.");

class AuthQueryHandler final : public query::AuthQueryHandler {
  auth::Auth *auth_;
  std::regex name_regex_;

 public:
  AuthQueryHandler(auth::Auth *auth, const std::regex &name_regex) : auth_(auth), name_regex_(name_regex) {}

  bool CreateUser(const std::string &username, const std::optional<std::string> &password) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      return !!auth_->AddUser(username, password);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  bool DropUser(const std::string &username) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto user = auth_->GetUser(username);
      if (!user) return false;
      return auth_->RemoveUser(username);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  void SetPassword(const std::string &username, const std::optional<std::string> &password) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto user = auth_->GetUser(username);
      if (!user) {
        throw query::QueryRuntimeException("User '{}' doesn't exist.", username);
      }
      user->UpdatePassword(password);
      auth_->SaveUser(*user);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  bool CreateRole(const std::string &rolename) override {
    if (!std::regex_match(rolename, name_regex_)) {
      throw query::QueryRuntimeException("Invalid role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      return !!auth_->AddRole(rolename);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  bool DropRole(const std::string &rolename) override {
    if (!std::regex_match(rolename, name_regex_)) {
      throw query::QueryRuntimeException("Invalid role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto role = auth_->GetRole(rolename);
      if (!role) return false;
      return auth_->RemoveRole(rolename);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  std::vector<query::TypedValue> GetUsernames() override {
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      std::vector<query::TypedValue> usernames;
      const auto &users = auth_->AllUsers();
      usernames.reserve(users.size());
      for (const auto &user : users) {
        usernames.emplace_back(user.username());
      }
      return usernames;
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  std::vector<query::TypedValue> GetRolenames() override {
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      std::vector<query::TypedValue> rolenames;
      const auto &roles = auth_->AllRoles();
      rolenames.reserve(roles.size());
      for (const auto &role : roles) {
        rolenames.emplace_back(role.rolename());
      }
      return rolenames;
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  std::optional<std::string> GetRolenameForUser(const std::string &username) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto user = auth_->GetUser(username);
      if (!user) {
        throw query::QueryRuntimeException("User '{}' doesn't exist .", username);
      }
      if (user->role()) return user->role()->rolename();
      return std::nullopt;
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  std::vector<query::TypedValue> GetUsernamesForRole(const std::string &rolename) override {
    if (!std::regex_match(rolename, name_regex_)) {
      throw query::QueryRuntimeException("Invalid role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto role = auth_->GetRole(rolename);
      if (!role) {
        throw query::QueryRuntimeException("Role '{}' doesn't exist.", rolename);
      }
      std::vector<query::TypedValue> usernames;
      const auto &users = auth_->AllUsersForRole(rolename);
      usernames.reserve(users.size());
      for (const auto &user : users) {
        usernames.emplace_back(user.username());
      }
      return usernames;
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  void SetRole(const std::string &username, const std::string &rolename) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    if (!std::regex_match(rolename, name_regex_)) {
      throw query::QueryRuntimeException("Invalid role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto user = auth_->GetUser(username);
      if (!user) {
        throw query::QueryRuntimeException("User '{}' doesn't exist .", username);
      }
      auto role = auth_->GetRole(rolename);
      if (!role) {
        throw query::QueryRuntimeException("Role '{}' doesn't exist .", rolename);
      }
      if (user->role()) {
        throw query::QueryRuntimeException("User '{}' is already a member of role '{}'.", username,
                                           user->role()->rolename());
      }
      user->SetRole(*role);
      auth_->SaveUser(*user);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  void ClearRole(const std::string &username) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto user = auth_->GetUser(username);
      if (!user) {
        throw query::QueryRuntimeException("User '{}' doesn't exist .", username);
      }
      user->ClearRole();
      auth_->SaveUser(*user);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  std::vector<std::vector<query::TypedValue>> GetPrivileges(const std::string &user_or_role) override {
    if (!std::regex_match(user_or_role, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user or role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      std::vector<std::vector<query::TypedValue>> grants;
      auto user = auth_->GetUser(user_or_role);
      auto role = auth_->GetRole(user_or_role);
      if (!user && !role) {
        throw query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
      }
      if (user) {
        const auto &permissions = user->GetPermissions();
        for (const auto &privilege : query::kPrivilegesAll) {
          auto permission = glue::PrivilegeToPermission(privilege);
          auto effective = permissions.Has(permission);
          if (permissions.Has(permission) != auth::PermissionLevel::NEUTRAL) {
            std::vector<std::string> description;
            auto user_level = user->permissions().Has(permission);
            if (user_level == auth::PermissionLevel::GRANT) {
              description.emplace_back("GRANTED TO USER");
            } else if (user_level == auth::PermissionLevel::DENY) {
              description.emplace_back("DENIED TO USER");
            }
            if (user->role()) {
              auto role_level = user->role()->permissions().Has(permission);
              if (role_level == auth::PermissionLevel::GRANT) {
                description.emplace_back("GRANTED TO ROLE");
              } else if (role_level == auth::PermissionLevel::DENY) {
                description.emplace_back("DENIED TO ROLE");
              }
            }
            grants.push_back({query::TypedValue(auth::PermissionToString(permission)),
                              query::TypedValue(auth::PermissionLevelToString(effective)),
                              query::TypedValue(utils::Join(description, ", "))});
          }
        }
      } else {
        const auto &permissions = role->permissions();
        for (const auto &privilege : query::kPrivilegesAll) {
          auto permission = glue::PrivilegeToPermission(privilege);
          auto effective = permissions.Has(permission);
          if (effective != auth::PermissionLevel::NEUTRAL) {
            std::string description;
            if (effective == auth::PermissionLevel::GRANT) {
              description = "GRANTED TO ROLE";
            } else if (effective == auth::PermissionLevel::DENY) {
              description = "DENIED TO ROLE";
            }
            grants.push_back({query::TypedValue(auth::PermissionToString(permission)),
                              query::TypedValue(auth::PermissionLevelToString(effective)),
                              query::TypedValue(description)});
          }
        }
      }
      return grants;
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  void GrantPrivilege(const std::string &user_or_role,
                      const std::vector<query::AuthQuery::Privilege> &privileges) override {
    EditPermissions(user_or_role, privileges, [](auto *permissions, const auto &permission) {
      // TODO (mferencevic): should we first check that the
      // privilege is granted/denied/revoked before
      // unconditionally granting/denying/revoking it?
      permissions->Grant(permission);
    });
  }

  void DenyPrivilege(const std::string &user_or_role,
                     const std::vector<query::AuthQuery::Privilege> &privileges) override {
    EditPermissions(user_or_role, privileges, [](auto *permissions, const auto &permission) {
      // TODO (mferencevic): should we first check that the
      // privilege is granted/denied/revoked before
      // unconditionally granting/denying/revoking it?
      permissions->Deny(permission);
    });
  }

  void RevokePrivilege(const std::string &user_or_role,
                       const std::vector<query::AuthQuery::Privilege> &privileges) override {
    EditPermissions(user_or_role, privileges, [](auto *permissions, const auto &permission) {
      // TODO (mferencevic): should we first check that the
      // privilege is granted/denied/revoked before
      // unconditionally granting/denying/revoking it?
      permissions->Revoke(permission);
    });
  }

 private:
  template <class TEditFun>
  void EditPermissions(const std::string &user_or_role, const std::vector<query::AuthQuery::Privilege> &privileges,
                       const TEditFun &edit_fun) {
    if (!std::regex_match(user_or_role, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user or role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      std::vector<auth::Permission> permissions;
      permissions.reserve(privileges.size());
      for (const auto &privilege : privileges) {
        permissions.push_back(glue::PrivilegeToPermission(privilege));
      }
      auto user = auth_->GetUser(user_or_role);
      auto role = auth_->GetRole(user_or_role);
      if (!user && !role) {
        throw query::QueryRuntimeException("User or role '{}' doesn't exist.", user_or_role);
      }
      if (user) {
        for (const auto &permission : permissions) {
          edit_fun(&user->permissions(), permission);
        }
        auth_->SaveUser(*user);
      } else {
        for (const auto &permission : permissions) {
          edit_fun(&role->permissions(), permission);
        }
        auth_->SaveRole(*role);
      }
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }
};
#else
class NoAuthInCommunity : public query::QueryRuntimeException {
 public:
  NoAuthInCommunity()
      : query::QueryRuntimeException::QueryRuntimeException("Auth is not supported in Memgraph Community!") {}
};

class AuthQueryHandler final : public query::AuthQueryHandler {
 public:
  bool CreateUser(const std::string &, const std::optional<std::string> &) override { throw NoAuthInCommunity(); }

  bool DropUser(const std::string &) override { throw NoAuthInCommunity(); }

  void SetPassword(const std::string &, const std::optional<std::string> &) override { throw NoAuthInCommunity(); }

  bool CreateRole(const std::string &) override { throw NoAuthInCommunity(); }

  bool DropRole(const std::string &) override { throw NoAuthInCommunity(); }

  std::vector<query::TypedValue> GetUsernames() override { throw NoAuthInCommunity(); }

  std::vector<query::TypedValue> GetRolenames() override { throw NoAuthInCommunity(); }

  std::optional<std::string> GetRolenameForUser(const std::string &) override { throw NoAuthInCommunity(); }

  std::vector<query::TypedValue> GetUsernamesForRole(const std::string &) override { throw NoAuthInCommunity(); }

  void SetRole(const std::string &, const std::string &) override { throw NoAuthInCommunity(); }

  void ClearRole(const std::string &) override { throw NoAuthInCommunity(); }

  std::vector<std::vector<query::TypedValue>> GetPrivileges(const std::string &) override { throw NoAuthInCommunity(); }

  void GrantPrivilege(const std::string &, const std::vector<query::AuthQuery::Privilege> &) override {
    throw NoAuthInCommunity();
  }

  void DenyPrivilege(const std::string &, const std::vector<query::AuthQuery::Privilege> &) override {
    throw NoAuthInCommunity();
  }

  void RevokePrivilege(const std::string &, const std::vector<query::AuthQuery::Privilege> &) override {
    throw NoAuthInCommunity();
  }
};
#endif

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

  MG_ASSERT(utils::SignalHandler::RegisterHandler(utils::Signal::Terminate, shutdown, block_shutdown_signals),
            "Unable to register SIGTERM handler!");
  MG_ASSERT(utils::SignalHandler::RegisterHandler(utils::Signal::Interupt, shutdown, block_shutdown_signals),
            "Unable to register SIGINT handler!");
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph database server");
  gflags::SetVersionString(version_string);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig("memgraph");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  ConfigureLogging();

  // Unhandled exception handler init.
  std::set_terminate(&utils::TerminateHandler);

  // Initialize Python
  auto *program_name = Py_DecodeLocale(argv[0], nullptr);
  MG_ASSERT(program_name);
  // Set program name, so Python can find its way to runtime libraries relative
  // to executable.
  Py_SetProgramName(program_name);
  PyImport_AppendInittab("_mgp", &query::procedure::PyInitMgpModule);
  Py_InitializeEx(0 /* = initsigs */);
  PyEval_InitThreads();
  Py_BEGIN_ALLOW_THREADS;

  // Add our Python modules to sys.path
  try {
    auto exe_path = utils::GetExecutablePath();
    auto py_support_dir = exe_path.parent_path() / "python_support";
    if (std::filesystem::is_directory(py_support_dir)) {
      auto gil = py::EnsureGIL();
      auto maybe_exc = py::AppendToSysPath(py_support_dir.c_str());
      if (maybe_exc) {
        spdlog::error("Unable to load support for embedded Python: {}", *maybe_exc);
      }
    } else {
      spdlog::error("Unable to load support for embedded Python: missing directory {}", py_support_dir);
    }
  } catch (const std::filesystem::filesystem_error &e) {
    spdlog::error("Unable to load support for embedded Python: {}", e.what());
  }

  // Initialize the communication library.
  communication::SSLInit sslInit;

  // Initialize the requests library.
  requests::Init();

  // Start memory warning logger.
  utils::Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    auto free_ram = utils::sysinfo::AvailableMemory();
    if (free_ram) {
      mem_log_scheduler.Run("Memory warning", std::chrono::seconds(3), [] {
        auto free_ram = utils::sysinfo::AvailableMemory();
        if (free_ram && *free_ram / 1024 < FLAGS_memory_warning_threshold)
          spdlog::warn("Running out of available RAM, only {} MB left", *free_ram / 1024);
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

  auto data_directory = std::filesystem::path(FLAGS_data_directory);

#ifdef MG_ENTERPRISE
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
  auth::Auth auth{data_directory / "auth"};

  // Audit log
  audit::Log audit_log{data_directory / "audit", FLAGS_audit_buffer_size, FLAGS_audit_buffer_flush_interval_ms};
  // Start the log if enabled.
  if (FLAGS_audit_enabled) {
    audit_log.Start();
  }
  // Setup SIGUSR2 to be used for reopening audit log files, when e.g. logrotate
  // rotates our audit logs.
  MG_ASSERT(utils::SignalHandler::RegisterHandler(utils::Signal::User2, [&audit_log]() { audit_log.ReopenLog(); }),
            "Unable to register SIGUSR2 handler!");

  // End enterprise features initialization
#endif

  const auto memory_limit = GetMemoryLimit();
  spdlog::info("Memory limit set to {}", utils::GetReadableSize(memory_limit));
  utils::total_memory_tracker.SetHardLimit(memory_limit);

  // Main storage and execution engines initialization
  storage::Config db_config{
      .gc = {.type = storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(FLAGS_storage_gc_cycle_sec)},
      .items = {.properties_on_edges = FLAGS_storage_properties_on_edges},
      .durability = {.storage_directory = FLAGS_data_directory,
                     .recover_on_startup = FLAGS_storage_recover_on_startup,
                     .snapshot_retention_count = FLAGS_storage_snapshot_retention_count,
                     .wal_file_size_kibibytes = FLAGS_storage_wal_file_size_kib,
                     .wal_file_flush_every_n_tx = FLAGS_storage_wal_file_flush_every_n_tx,
                     .snapshot_on_exit = FLAGS_storage_snapshot_on_exit},
      .transaction = {.isolation_level = ParseIsolationLevel()}};
  if (FLAGS_storage_snapshot_interval_sec == 0) {
    if (FLAGS_storage_wal_enabled) {
      LOG_FATAL(
          "In order to use write-ahead-logging you must enable "
          "periodic snapshots by setting the snapshot interval to a "
          "value larger than 0!");
      db_config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
    }
  } else {
    if (FLAGS_storage_wal_enabled) {
      db_config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    } else {
      db_config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    }
    db_config.durability.snapshot_interval = std::chrono::seconds(FLAGS_storage_snapshot_interval_sec);
  }
  storage::Storage db(db_config);
  query::InterpreterContext interpreter_context{&db, FLAGS_data_directory};

  query::SetExecutionTimeout(&interpreter_context, FLAGS_query_execution_timeout_sec);
#ifdef MG_ENTERPRISE
  SessionData session_data{&db, &interpreter_context, &auth, &audit_log};
#else
  SessionData session_data{&db, &interpreter_context};
#endif

  query::procedure::gModuleRegistry.SetModulesDirectory(query_modules_directories);
  query::procedure::gModuleRegistry.UnloadAndLoadModulesFromDirectories();

#ifdef MG_ENTERPRISE
  AuthQueryHandler auth_handler(&auth, std::regex(FLAGS_auth_user_or_role_name_regex));
#else
  AuthQueryHandler auth_handler;
#endif
  interpreter_context.auth = &auth_handler;

  ServerContext context;
  std::string service_name = "Bolt";
  if (!FLAGS_bolt_key_file.empty() && !FLAGS_bolt_cert_file.empty()) {
    context = ServerContext(FLAGS_bolt_key_file, FLAGS_bolt_cert_file);
    service_name = "BoltS";
    spdlog::info("Using secure Bolt connection (with SSL)");
  } else {
    spdlog::warn("Using non-secure Bolt connection (without SSL)");
  }

  ServerT server({FLAGS_bolt_address, static_cast<uint16_t>(FLAGS_bolt_port)}, &session_data, &context,
                 FLAGS_bolt_session_inactivity_timeout, service_name, FLAGS_bolt_num_workers);

  // Setup telemetry
  std::optional<telemetry::Telemetry> telemetry;
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace("https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/",
                      data_directory / "telemetry", std::chrono::minutes(10));
    telemetry->AddCollector("storage", [&db]() -> nlohmann::json {
      auto info = db.GetInfo();
      return {{"vertices", info.vertex_count}, {"edges", info.edge_count}};
    });
    telemetry->AddCollector("event_counters", []() -> nlohmann::json {
      nlohmann::json ret;
      for (size_t i = 0; i < EventCounter::End(); ++i) {
        ret[EventCounter::GetName(i)] = EventCounter::global_counters[i].load(std::memory_order_relaxed);
      }
      return ret;
    });
    telemetry->AddCollector("query_module_counters",
                            []() -> nlohmann::json { return query::plan::CallProcedure::GetAndResetCounters(); });
  }

  // Handler for regular termination signals
  auto shutdown = [&server, &interpreter_context] {
    // Server needs to be shutdown first and then the database. This prevents
    // a race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    // After the server is notified to stop accepting and processing
    // connections we tell the execution engine to stop processing all pending
    // queries.
    query::Shutdown(&interpreter_context);
  };
  InitSignalHandlers(shutdown);

  MG_ASSERT(server.Start(), "Couldn't start the Bolt server!");
  server.AwaitShutdown();
  query::procedure::gModuleRegistry.UnloadAllModules();

  Py_END_ALLOW_THREADS;
  // Shutdown Python
  Py_Finalize();
  PyMem_RawFree(program_name);

  utils::total_memory_tracker.LogPeakMemoryUsage();
  return 0;
}
