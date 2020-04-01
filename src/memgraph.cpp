#include <algorithm>
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
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/exceptions.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/init.hpp"
#include "communication/server.hpp"
#include "communication/session.hpp"
#include "glue/communication.hpp"
#include "helpers.hpp"
#include "py/py.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/procedure/module.hpp"
#include "query/procedure/py_module.hpp"
#include "requests/requests.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/signals.hpp"
#include "utils/string.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

#ifdef MG_ENTERPRISE
#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "glue/auth.hpp"
#endif

// Bolt server flags.
DEFINE_string(bolt_address, "0.0.0.0",
              "IP address on which the Bolt server should listen.");
DEFINE_VALIDATED_int32(bolt_port, 7687,
                       "Port on which the Bolt server should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
DEFINE_VALIDATED_int32(
    bolt_num_workers, std::max(std::thread::hardware_concurrency(), 1U),
    "Number of workers used by the Bolt server. By default, this will be the "
    "number of processing units available on the machine.",
    FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(
    bolt_session_inactivity_timeout, 1800,
    "Time in seconds after which inactive Bolt sessions will be "
    "closed.",
    FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_string(bolt_cert_file, "",
              "Certificate file which should be used for the Bolt server.");
DEFINE_string(bolt_key_file, "",
              "Key file which should be used for the Bolt server.");
DEFINE_string(bolt_server_name_for_init, "",
              "Server name which the database should send to the client in the "
              "Bolt INIT message.");

// General purpose flags.
// NOTE: The `data_directory` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
DEFINE_string(data_directory, "mg_data",
              "Path to directory in which to save all permanent data.");
DEFINE_string(log_file, "", "Path to where the log should be stored.");
DEFINE_HIDDEN_string(
    log_link_basename, "",
    "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");

// Storage flags.
DEFINE_VALIDATED_uint64(storage_gc_cycle_sec, 30,
                        "Storage garbage collector interval (in seconds).",
                        FLAG_IN_RANGE(1, 24 * 3600));
// NOTE: The `storage_properties_on_edges` flag must be the same here and in
// `mg_import_csv`. If you change it, make sure to change it there as well.
DEFINE_bool(storage_properties_on_edges, false,
            "Controls whether edges have properties.");
DEFINE_bool(storage_recover_on_startup, false,
            "Controls whether the storage recovers persisted data on startup.");
DEFINE_VALIDATED_uint64(storage_snapshot_interval_sec, 0,
                        "Storage snapshot creation interval (in seconds). Set "
                        "to 0 to disable periodic snapshot creation.",
                        FLAG_IN_RANGE(0, 7 * 24 * 3600));
DEFINE_bool(storage_wal_enabled, false,
            "Controls whether the storage uses write-ahead-logging. To enable "
            "WAL periodic snapshots must be enabled.");
DEFINE_VALIDATED_uint64(storage_snapshot_retention_count, 3,
                        "The number of snapshots that should always be kept.",
                        FLAG_IN_RANGE(1, 1000000));
DEFINE_VALIDATED_uint64(storage_wal_file_size_kib,
                        storage::Config::Durability().wal_file_size_kibibytes,
                        "Minimum file size of each WAL file.",
                        FLAG_IN_RANGE(1, 1000 * 1024));
DEFINE_VALIDATED_uint64(
    storage_wal_file_flush_every_n_tx,
    storage::Config::Durability().wal_file_flush_every_n_tx,
    "Issue a 'fsync' call after this amount of transactions are written to the "
    "WAL file. Set to 1 for fully synchronous operation.",
    FLAG_IN_RANGE(1, 1000000));
DEFINE_bool(storage_snapshot_on_exit, false,
            "Controls whether the storage creates another snapshot on exit.");

DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");

// Audit logging flags.
#ifdef MG_ENTERPRISE
DEFINE_bool(audit_enabled, false, "Set to true to enable audit logging.");
DEFINE_VALIDATED_int32(audit_buffer_size, audit::kBufferSizeDefault,
                       "Maximum number of items in the audit log buffer.",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(
    audit_buffer_flush_interval_ms, audit::kBufferFlushIntervalMillisDefault,
    "Interval (in milliseconds) used for flushing the audit log buffer.",
    FLAG_IN_RANGE(10, INT32_MAX));
#endif

// Query flags.
DEFINE_uint64(query_execution_timeout_sec, 180,
              "Maximum allowed query execution time. Queries exceeding this "
              "limit will be aborted. Value of 0 means no limit.");

DEFINE_VALIDATED_string(
    query_modules_directory, "",
    "Directory where modules with custom query procedures are stored.", {
      if (value.empty()) return true;
      if (utils::DirExists(value)) return true;
      std::cout << "Expected --" << flagname << " to point to a directory."
                << std::endl;
      return false;
    });

/// Encapsulates Dbms and Interpreter that are passed through the network server
/// and worker to the session.
#ifdef MG_ENTERPRISE
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
  SessionData(storage::Storage *db,
              query::InterpreterContext *interpreter_context, auth::Auth *auth,
              audit::Log *audit_log)
      : db(db),
        interpreter_context(interpreter_context),
        auth(auth),
        audit_log(audit_log) {}
  storage::Storage *db;
  query::InterpreterContext *interpreter_context;
  auth::Auth *auth;
  audit::Log *audit_log;
};
#else
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
  SessionData(storage::Storage *db,
              query::InterpreterContext *interpreter_context)
      : db(db), interpreter_context(interpreter_context) {}
  storage::Storage *db;
  query::InterpreterContext *interpreter_context;
};
#endif

class BoltSession final
    : public communication::bolt::Session<communication::InputStream,
                                          communication::OutputStream> {
 public:
  BoltSession(SessionData *data, const io::network::Endpoint &endpoint,
              communication::InputStream *input_stream,
              communication::OutputStream *output_stream)
      : communication::bolt::Session<communication::InputStream,
                                     communication::OutputStream>(
            input_stream, output_stream),
        db_(data->db),
        interpreter_(data->interpreter_context),
#ifdef MG_ENTERPRISE
        auth_(data->auth),
        audit_log_(data->audit_log),
#endif
        endpoint_(endpoint) {
  }

  using communication::bolt::Session<communication::InputStream,
                                     communication::OutputStream>::TEncoder;

  std::vector<std::string> Interpret(
      const std::string &query,
      const std::map<std::string, communication::bolt::Value> &params)
      override {
    std::map<std::string, storage::PropertyValue> params_pv;
    for (const auto &kv : params)
      params_pv.emplace(kv.first, glue::ToPropertyValue(kv.second));
#ifdef MG_ENTERPRISE
    audit_log_->Record(endpoint_.address(), user_ ? user_->username() : "",
                       query, storage::PropertyValue(params_pv));
#endif
    try {
      auto result = interpreter_.Prepare(query, params_pv);
#ifdef MG_ENTERPRISE
      if (user_) {
        const auto &permissions = user_->GetPermissions();
        for (const auto &privilege : result.second) {
          if (permissions.Has(glue::PrivilegeToPermission(privilege)) !=
              auth::PermissionLevel::GRANT) {
            interpreter_.Abort();
            throw communication::bolt::ClientError(
                "You are not authorized to execute this query! Please contact "
                "your database administrator.");
          }
        }
      }
#endif
      return result.first;

    } catch (const query::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw communication::bolt::ClientError(e.what());
    }
  }

  std::map<std::string, communication::bolt::Value> PullAll(
      TEncoder *encoder) override {
    try {
      TypedValueResultStream stream(encoder, db_);
      const auto &summary = interpreter_.PullAll(&stream);
      std::map<std::string, communication::bolt::Value> decoded_summary;
      for (const auto &kv : summary) {
        auto maybe_value =
            glue::ToBoltValue(kv.second, *db_, storage::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case storage::Error::DELETED_OBJECT:
            case storage::Error::SERIALIZATION_ERROR:
            case storage::Error::VERTEX_HAS_EDGES:
            case storage::Error::PROPERTIES_DISABLED:
            case storage::Error::NONEXISTENT_OBJECT:
              throw communication::bolt::ClientError(
                  "Unexpected storage error when streaming summary.");
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

  void Abort() override { interpreter_.Abort(); }

  bool Authenticate(const std::string &username,
                    const std::string &password) override {
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
  /// Wrapper around TEncoder which converts TypedValue to Value
  /// before forwarding the calls to original TEncoder.
  class TypedValueResultStream {
   public:
    TypedValueResultStream(TEncoder *encoder, const storage::Storage *db)
        : encoder_(encoder), db_(db) {}

    void Result(const std::vector<query::TypedValue> &values) {
      std::vector<communication::bolt::Value> decoded_values;
      decoded_values.reserve(values.size());
      for (const auto &v : values) {
        auto maybe_value = glue::ToBoltValue(v, *db_, storage::View::NEW);
        if (maybe_value.HasError()) {
          switch (maybe_value.GetError()) {
            case storage::Error::DELETED_OBJECT:
              throw communication::bolt::ClientError(
                  "Returning a deleted object as a result.");
            case storage::Error::NONEXISTENT_OBJECT:
              throw communication::bolt::ClientError(
                  "Returning a nonexistent object as a result.");
            case storage::Error::VERTEX_HAS_EDGES:
            case storage::Error::SERIALIZATION_ERROR:
            case storage::Error::PROPERTIES_DISABLED:
              throw communication::bolt::ClientError(
                  "Unexpected storage error when streaming results.");
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
DEFINE_string(
    auth_user_or_role_name_regex, "[a-zA-Z0-9_.+-@]+",
    "Set to the regular expression that each user or role name must fulfill.");

class AuthQueryHandler final : public query::AuthQueryHandler {
  auth::Auth *auth_;
  std::regex name_regex_;

 public:
  AuthQueryHandler(auth::Auth *auth, const std::regex &name_regex)
      : auth_(auth), name_regex_(name_regex) {}

  bool CreateUser(const std::string &username,
                  const std::optional<std::string> &password) override {
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

  void SetPassword(const std::string &username,
                   const std::optional<std::string> &password) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto user = auth_->GetUser(username);
      if (!user) {
        throw query::QueryRuntimeException("User '{}' doesn't exist.",
                                           username);
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

  std::optional<std::string> GetRolenameForUser(
      const std::string &username) override {
    if (!std::regex_match(username, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto user = auth_->GetUser(username);
      if (!user) {
        throw query::QueryRuntimeException("User '{}' doesn't exist .",
                                           username);
      }
      if (user->role()) return user->role()->rolename();
      return std::nullopt;
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  std::vector<query::TypedValue> GetUsernamesForRole(
      const std::string &rolename) override {
    if (!std::regex_match(rolename, name_regex_)) {
      throw query::QueryRuntimeException("Invalid role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      auto role = auth_->GetRole(rolename);
      if (!role) {
        throw query::QueryRuntimeException("Role '{}' doesn't exist.",
                                           rolename);
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

  void SetRole(const std::string &username,
               const std::string &rolename) override {
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
        throw query::QueryRuntimeException("User '{}' doesn't exist .",
                                           username);
      }
      auto role = auth_->GetRole(rolename);
      if (!role) {
        throw query::QueryRuntimeException("Role '{}' doesn't exist .",
                                           rolename);
      }
      if (user->role()) {
        throw query::QueryRuntimeException(
            "User '{}' is already a member of role '{}'.", username,
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
        throw query::QueryRuntimeException("User '{}' doesn't exist .",
                                           username);
      }
      user->ClearRole();
      auth_->SaveUser(*user);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  std::vector<std::vector<query::TypedValue>> GetPrivileges(
      const std::string &user_or_role) override {
    if (!std::regex_match(user_or_role, name_regex_)) {
      throw query::QueryRuntimeException("Invalid user or role name.");
    }
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      std::vector<std::vector<query::TypedValue>> grants;
      auto user = auth_->GetUser(user_or_role);
      auto role = auth_->GetRole(user_or_role);
      if (!user && !role) {
        throw query::QueryRuntimeException("User or role '{}' doesn't exist.",
                                           user_or_role);
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
            grants.push_back(
                {query::TypedValue(auth::PermissionToString(permission)),
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
            grants.push_back(
                {query::TypedValue(auth::PermissionToString(permission)),
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

  void GrantPrivilege(
      const std::string &user_or_role,
      const std::vector<query::AuthQuery::Privilege> &privileges) override {
    EditPermissions(user_or_role, privileges,
                    [](auto *permissions, const auto &permission) {
                      // TODO (mferencevic): should we first check that the
                      // privilege is granted/denied/revoked before
                      // unconditionally granting/denying/revoking it?
                      permissions->Grant(permission);
                    });
  }

  void DenyPrivilege(
      const std::string &user_or_role,
      const std::vector<query::AuthQuery::Privilege> &privileges) override {
    EditPermissions(user_or_role, privileges,
                    [](auto *permissions, const auto &permission) {
                      // TODO (mferencevic): should we first check that the
                      // privilege is granted/denied/revoked before
                      // unconditionally granting/denying/revoking it?
                      permissions->Deny(permission);
                    });
  }

  void RevokePrivilege(
      const std::string &user_or_role,
      const std::vector<query::AuthQuery::Privilege> &privileges) override {
    EditPermissions(user_or_role, privileges,
                    [](auto *permissions, const auto &permission) {
                      // TODO (mferencevic): should we first check that the
                      // privilege is granted/denied/revoked before
                      // unconditionally granting/denying/revoking it?
                      permissions->Revoke(permission);
                    });
  }

 private:
  template <class TEditFun>
  void EditPermissions(
      const std::string &user_or_role,
      const std::vector<query::AuthQuery::Privilege> &privileges,
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
        throw query::QueryRuntimeException("User or role '{}' doesn't exist.",
                                           user_or_role);
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
      : query::QueryRuntimeException::QueryRuntimeException(
            "Auth is not supported in Memgraph Community!") {}
};

class AuthQueryHandler final : public query::AuthQueryHandler {
 public:
  bool CreateUser(const std::string &,
                  const std::optional<std::string> &) override {
    throw NoAuthInCommunity();
  }

  bool DropUser(const std::string &) override { throw NoAuthInCommunity(); }

  void SetPassword(const std::string &,
                   const std::optional<std::string> &) override {
    throw NoAuthInCommunity();
  }

  bool CreateRole(const std::string &) override { throw NoAuthInCommunity(); }

  bool DropRole(const std::string &) override { throw NoAuthInCommunity(); }

  std::vector<query::TypedValue> GetUsernames() override {
    throw NoAuthInCommunity();
  }

  std::vector<query::TypedValue> GetRolenames() override {
    throw NoAuthInCommunity();
  }

  std::optional<std::string> GetRolenameForUser(const std::string &) override {
    throw NoAuthInCommunity();
  }

  std::vector<query::TypedValue> GetUsernamesForRole(
      const std::string &) override {
    throw NoAuthInCommunity();
  }

  void SetRole(const std::string &, const std::string &) override {
    throw NoAuthInCommunity();
  }

  void ClearRole(const std::string &) override { throw NoAuthInCommunity(); }

  std::vector<std::vector<query::TypedValue>> GetPrivileges(
      const std::string &) override {
    throw NoAuthInCommunity();
  }

  void GrantPrivilege(
      const std::string &,
      const std::vector<query::AuthQuery::Privilege> &) override {
    throw NoAuthInCommunity();
  }

  void DenyPrivilege(
      const std::string &,
      const std::vector<query::AuthQuery::Privilege> &) override {
    throw NoAuthInCommunity();
  }

  void RevokePrivilege(
      const std::string &,
      const std::vector<query::AuthQuery::Privilege> &) override {
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

  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Terminate,
                                              shutdown, block_shutdown_signals))
      << "Unable to register SIGTERM handler!";
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Interupt, shutdown,
                                              block_shutdown_signals))
      << "Unable to register SIGINT handler!";

  // Setup SIGUSR1 to be used for reopening log files, when e.g. logrotate
  // rotates our logs.
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::User1, []() {
    google::CloseLogDestination(google::INFO);
  })) << "Unable to register SIGUSR1 handler!";
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph database server");
  gflags::SetVersionString(version_string);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig("memgraph");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  google::InitGoogleLogging(argv[0]);
  google::SetLogDestination(google::INFO, FLAGS_log_file.c_str());
  google::SetLogSymlink(google::INFO, FLAGS_log_link_basename.c_str());

  // Unhandled exception handler init.
  std::set_terminate(&utils::TerminateHandler);

  // Initialize Python
  auto *program_name = Py_DecodeLocale(argv[0], nullptr);
  CHECK(program_name);
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
        LOG(ERROR) << "Unable to load support for embedded Python: "
                   << *maybe_exc;
      }
    } else {
      LOG(ERROR)
          << "Unable to load support for embedded Python: missing directory "
          << py_support_dir;
    }
  } catch (const std::filesystem::filesystem_error &e) {
    LOG(ERROR) << "Unable to load support for embedded Python: " << e.what();
  }

  // Initialize the communication library.
  communication::Init();

  // Initialize the requests library.
  requests::Init();

  // Start memory warning logger.
  utils::Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    auto free_ram = utils::sysinfo::AvailableMemoryKilobytes();
    if (free_ram) {
      mem_log_scheduler.Run("Memory warning", std::chrono::seconds(3), [] {
        auto free_ram = utils::sysinfo::AvailableMemoryKilobytes();
        if (free_ram && *free_ram / 1024 < FLAGS_memory_warning_threshold)
          LOG(WARNING) << "Running out of available RAM, only "
                       << *free_ram / 1024 << " MB left.";
      });
    } else {
      // Kernel version for the `MemAvailable` value is from: man procfs
      LOG(WARNING) << "You have an older kernel version (<3.14) or the /proc "
                      "filesystem isn't available so remaining memory warnings "
                      "won't be available.";
    }
  }

  std::cout << "You are running Memgraph v" << gflags::VersionString()
            << std::endl;

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
  audit::Log audit_log{data_directory / "audit", FLAGS_audit_buffer_size,
                       FLAGS_audit_buffer_flush_interval_ms};
  // Start the log if enabled.
  if (FLAGS_audit_enabled) {
    audit_log.Start();
  }
  // Setup SIGUSR2 to be used for reopening audit log files, when e.g. logrotate
  // rotates our audit logs.
  CHECK(utils::SignalHandler::RegisterHandler(
      utils::Signal::User2, [&audit_log]() { audit_log.ReopenLog(); }))
      << "Unable to register SIGUSR2 handler!";

  // End enterprise features initialization
#endif

  // Main storage and execution engines initialization

  storage::Config db_config{
      .gc = {.type = storage::Config::Gc::Type::PERIODIC,
             .interval = std::chrono::seconds(FLAGS_storage_gc_cycle_sec)},
      .items = {.properties_on_edges = FLAGS_storage_properties_on_edges},
      .durability = {
          .storage_directory = FLAGS_data_directory,
          .recover_on_startup = FLAGS_storage_recover_on_startup,
          .snapshot_retention_count = FLAGS_storage_snapshot_retention_count,
          .wal_file_size_kibibytes = FLAGS_storage_wal_file_size_kib,
          .wal_file_flush_every_n_tx = FLAGS_storage_wal_file_flush_every_n_tx,
          .snapshot_on_exit = FLAGS_storage_snapshot_on_exit}};
  if (FLAGS_storage_snapshot_interval_sec == 0) {
    LOG_IF(FATAL, FLAGS_storage_wal_enabled)
        << "In order to use write-ahead-logging you must enable "
           "periodic snapshots by setting the snapshot interval to a "
           "value larger than 0!";
    db_config.durability.snapshot_wal_mode =
        storage::Config::Durability::SnapshotWalMode::DISABLED;
  } else {
    if (FLAGS_storage_wal_enabled) {
      db_config.durability.snapshot_wal_mode = storage::Config::Durability::
          SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    } else {
      db_config.durability.snapshot_wal_mode =
          storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    }
    db_config.durability.snapshot_interval =
        std::chrono::seconds(FLAGS_storage_snapshot_interval_sec);
  }
  storage::Storage db(db_config);
  query::InterpreterContext interpreter_context{&db};
  query::SetExecutionTimeout(&interpreter_context,
                             FLAGS_query_execution_timeout_sec);
#ifdef MG_ENTERPRISE
  SessionData session_data{&db, &interpreter_context, &auth, &audit_log};
#else
  SessionData session_data{&db, &interpreter_context};
#endif

  // Register modules
  if (!FLAGS_query_modules_directory.empty()) {
    for (const auto &entry :
         std::filesystem::directory_iterator(FLAGS_query_modules_directory)) {
      if (entry.is_regular_file())
        query::procedure::gModuleRegistry.LoadModuleLibrary(entry.path());
    }
  }
  // Register modules END

#ifdef MG_ENTERPRISE
  AuthQueryHandler auth_handler(&auth,
                                std::regex(FLAGS_auth_user_or_role_name_regex));
#else
  AuthQueryHandler auth_handler;
#endif
  interpreter_context.auth = &auth_handler;

  ServerContext context;
  std::string service_name = "Bolt";
  if (!FLAGS_bolt_key_file.empty() && !FLAGS_bolt_cert_file.empty()) {
    context = ServerContext(FLAGS_bolt_key_file, FLAGS_bolt_cert_file);
    service_name = "BoltS";
  }

  ServerT server({FLAGS_bolt_address, static_cast<uint16_t>(FLAGS_bolt_port)},
                 &session_data, &context, FLAGS_bolt_session_inactivity_timeout,
                 service_name, FLAGS_bolt_num_workers);

  // Setup telemetry
  std::optional<telemetry::Telemetry> telemetry;
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace(
        "https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/",
        data_directory / "telemetry", std::chrono::minutes(10));
    telemetry->AddCollector("db", [&db]() -> nlohmann::json {
      auto info = db.GetInfo();
      return {{"vertices", info.vertex_count}, {"edges", info.edge_count}};
    });
  }

  // Handler for regular termination signals
  auto shutdown = [&server, &interpreter_context] {
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    // After the server is notified to stop accepting and processing connections
    // we tell the execution engine to stop processing all pending queries.
    query::Shutdown(&interpreter_context);
  };
  InitSignalHandlers(shutdown);

  CHECK(server.Start()) << "Couldn't start the Bolt server!";
  server.AwaitShutdown();
  query::procedure::gModuleRegistry.UnloadAllModules();

  Py_END_ALLOW_THREADS;
  // Shutdown Python
  Py_Finalize();
  PyMem_RawFree(program_name);
  return 0;
}
