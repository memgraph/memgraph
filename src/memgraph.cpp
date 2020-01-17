#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/server.hpp"
#ifdef MG_SINGLE_NODE_V2
#include "storage/v2/storage.hpp"
#else
#include "database/single_node/graph_db.hpp"
#endif
#include "glue/auth.hpp"
#include "memgraph_init.hpp"
#include "query/exceptions.hpp"
#include "query/procedure/module.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/string.hpp"

// General purpose flags.
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

#ifdef MG_SINGLE_NODE_V2
// General purpose flags.
DEFINE_string(data_directory, "mg_data",
              "Path to directory in which to save all permanent data.");

// Storage flags.
DEFINE_VALIDATED_uint64(storage_gc_cycle_sec, 30,
                        "Storage garbage collector interval (in seconds).",
                        FLAG_IN_RANGE(1, 24 * 3600));
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
#endif

DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");

// Audit logging flags.
DEFINE_bool(audit_enabled, false, "Set to true to enable audit logging.");
DEFINE_VALIDATED_int32(audit_buffer_size, audit::kBufferSizeDefault,
                       "Maximum number of items in the audit log buffer.",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(
    audit_buffer_flush_interval_ms, audit::kBufferFlushIntervalMillisDefault,
    "Interval (in milliseconds) used for flushing the audit log buffer.",
    FLAG_IN_RANGE(10, INT32_MAX));

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

using ServerT = communication::Server<BoltSession, SessionData>;
using communication::ServerContext;

class AuthQueryHandler final : public query::AuthQueryHandler {
  auth::Auth *auth_;

 public:
  explicit AuthQueryHandler(auth::Auth *auth) : auth_(auth) {}

  bool CreateUser(const std::string &username,
                  const std::optional<std::string> &password) override {
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      return !!auth_->AddUser(username, password);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  bool DropUser(const std::string &username) override {
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
    try {
      std::lock_guard<std::mutex> lock(auth_->WithLock());
      return !!auth_->AddRole(rolename);
    } catch (const auth::AuthException &e) {
      throw query::QueryRuntimeException(e.what());
    }
  }

  bool DropRole(const std::string &rolename) override {
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

void SingleNodeMain() {
  std::cout << "You are running Memgraph v" << gflags::VersionString()
            << std::endl;

  // All enterprise features should be constructed before the main database
  // storage. This will cause them to be destructed *after* the main database
  // storage. That way any errors that happen during enterprise features
  // destruction won't have an impact on the storage engine.
  // Example: When the main storage is destructed it makes a snapshot. When
  // audit logging is destructed it syncs all pending data to disk and that can
  // fail. That is why it must be destructed *after* the main database storage
  // to minimise the impact of their failure on the main storage.

  // Begin enterprise features initialization

#ifdef MG_SINGLE_NODE_V2
  auto data_directory = std::filesystem::path(FLAGS_data_directory);
#else
  auto data_directory = std::filesystem::path(FLAGS_durability_directory);
#endif

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

  // Main storage and execution engines initialization

#ifdef MG_SINGLE_NODE_V2
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
#else
  database::GraphDb db;
#endif
  query::InterpreterContext interpreter_context{&db};
  query::SetExecutionTimeout(&interpreter_context,
                             FLAGS_query_execution_timeout_sec);
  SessionData session_data{&db, &interpreter_context, &auth, &audit_log};

  // Register modules
  if (!FLAGS_query_modules_directory.empty()) {
    for (const auto &entry :
         std::filesystem::directory_iterator(FLAGS_query_modules_directory)) {
      if (entry.is_regular_file() && entry.path().extension() == ".so")
        query::procedure::gModuleRegistry.LoadModuleLibrary(entry.path());
    }
  }
  // Register modules END
  AuthQueryHandler auth_handler(&auth);
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
#ifdef MG_SINGLE_NODE_V2
    telemetry->AddCollector("db", [&db]() -> nlohmann::json {
      auto info = db.GetInfo();
      return {{"vertices", info.vertex_count}, {"edges", info.edge_count}};
    });
#else
    telemetry->AddCollector("db", [&db]() -> nlohmann::json {
      auto dba = db.Access();
      return {{"vertices", dba.VerticesCount()}, {"edges", dba.EdgesCount()}};
    });
#endif
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
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph database server");
  return WithInit(argc, argv, SingleNodeMain);
}
