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

#include "query/interpreter.hpp"
#include <bits/ranges_algo.h>
#include <fmt/core.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "csv/parsing.hpp"
#include "dbms/database.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/global.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "license/license.hpp"
#include "memory/memory_control.hpp"
#include "query/config.hpp"
#include "query/constants.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/dump.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/metadata.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/profile.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "query/procedure/module.hpp"
#include "query/stream.hpp"
#include "query/stream/common.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "replication/config.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_import_mode.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/algorithm.hpp"
#include "utils/build_info.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_histogram.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"
#include "utils/tsc.hpp"
#include "utils/typeinfo.hpp"
#include "utils/variant_helpers.hpp"

#include "dbms/dbms_handler.hpp"
#include "query/auth_query_handler.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"

namespace memgraph::metrics {
extern Event ReadQuery;
extern Event WriteQuery;
extern Event ReadWriteQuery;

extern const Event StreamsCreated;
extern const Event TriggersCreated;

extern const Event QueryExecutionLatency_us;

extern const Event CommitedTransactions;
extern const Event RollbackedTransactions;
extern const Event ActiveTransactions;
}  // namespace memgraph::metrics
void memgraph::query::CurrentDB::SetupDatabaseTransaction(
    std::optional<storage::IsolationLevel> override_isolation_level, bool could_commit, bool unique) {
  auto &db_acc = *db_acc_;
  if (unique) {
    db_transactional_accessor_ = db_acc->UniqueAccess(override_isolation_level);
  } else {
    db_transactional_accessor_ = db_acc->Access(override_isolation_level);
  }
  execution_db_accessor_.emplace(db_transactional_accessor_.get());

  if (db_acc->trigger_store()->HasTriggers() && could_commit) {
    trigger_context_collector_.emplace(db_acc->trigger_store()->GetEventTypes());
  }
}
void memgraph::query::CurrentDB::CleanupDBTransaction(bool abort) {
  if (abort && db_transactional_accessor_) {
    db_transactional_accessor_->Abort();
  }
  db_transactional_accessor_.reset();
  execution_db_accessor_.reset();
  trigger_context_collector_.reset();
}
// namespace memgraph::metrics

namespace memgraph::query {

template <typename>
constexpr auto kAlwaysFalse = false;

namespace {
template <typename T, typename K>
void Sort(std::vector<T, K> &vec) {
  std::sort(vec.begin(), vec.end());
}

template <typename K>
void Sort(std::vector<TypedValue, K> &vec) {
  std::sort(vec.begin(), vec.end(),
            [](const TypedValue &lv, const TypedValue &rv) { return lv.ValueString() < rv.ValueString(); });
}

// NOLINTNEXTLINE (misc-unused-parameters)
[[maybe_unused]] bool Same(const TypedValue &lv, const TypedValue &rv) {
  return TypedValue(lv).ValueString() == TypedValue(rv).ValueString();
}
// NOLINTNEXTLINE (misc-unused-parameters)
bool Same(const TypedValue &lv, const std::string &rv) { return std::string(TypedValue(lv).ValueString()) == rv; }
// NOLINTNEXTLINE (misc-unused-parameters)
[[maybe_unused]] bool Same(const std::string &lv, const TypedValue &rv) {
  return lv == std::string(TypedValue(rv).ValueString());
}
// NOLINTNEXTLINE (misc-unused-parameters)
bool Same(const std::string &lv, const std::string &rv) { return lv == rv; }

void UpdateTypeCount(const plan::ReadWriteTypeChecker::RWType type) {
  switch (type) {
    case plan::ReadWriteTypeChecker::RWType::R:
      memgraph::metrics::IncrementCounter(memgraph::metrics::ReadQuery);
      break;
    case plan::ReadWriteTypeChecker::RWType::W:
      memgraph::metrics::IncrementCounter(memgraph::metrics::WriteQuery);
      break;
    case plan::ReadWriteTypeChecker::RWType::RW:
      memgraph::metrics::IncrementCounter(memgraph::metrics::ReadWriteQuery);
      break;
    default:
      break;
  }
}

template <typename T>
concept HasEmpty = requires(T t) {
  { t.empty() } -> std::convertible_to<bool>;
};

template <typename T>
inline std::optional<T> GenOptional(const T &in) {
  return in.empty() ? std::nullopt : std::make_optional<T>(in);
}

struct Callback {
  std::vector<std::string> header;
  using CallbackFunction = std::function<std::vector<std::vector<TypedValue>>()>;
  CallbackFunction fn;
  bool should_abort_query{false};
};

TypedValue EvaluateOptionalExpression(Expression *expression, ExpressionVisitor<TypedValue> &eval) {
  return expression ? expression->Accept(eval) : TypedValue();
}

template <typename TResult>
std::optional<TResult> GetOptionalValue(query::Expression *expression, ExpressionVisitor<TypedValue> &evaluator) {
  if (expression != nullptr) {
    auto int_value = expression->Accept(evaluator);
    MG_ASSERT(int_value.IsNull() || int_value.IsInt());
    if (int_value.IsInt()) {
      return TResult{int_value.ValueInt()};
    }
  }
  return {};
};

std::optional<std::string> GetOptionalStringValue(query::Expression *expression,
                                                  ExpressionVisitor<TypedValue> &evaluator) {
  if (expression != nullptr) {
    auto value = expression->Accept(evaluator);
    MG_ASSERT(value.IsNull() || value.IsString());
    if (value.IsString()) {
      return {std::string(value.ValueString().begin(), value.ValueString().end())};
    }
  }
  return {};
};

bool IsAllShortestPathsQuery(const std::vector<memgraph::query::Clause *> &clauses) {
  for (const auto &clause : clauses) {
    if (clause->GetTypeInfo() != Match::kType) {
      continue;
    }
    auto *match_clause = utils::Downcast<Match>(clause);
    for (const auto &pattern : match_clause->patterns_) {
      for (const auto &atom : pattern->atoms_) {
        if (atom->GetTypeInfo() != EdgeAtom::kType) {
          continue;
        }
        auto *edge_atom = utils::Downcast<EdgeAtom>(atom);
        if (edge_atom->type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
          return true;
        }
      }
    }
  }
  return false;
}

class ReplQueryHandler final : public query::ReplicationQueryHandler {
 public:
  explicit ReplQueryHandler(storage::Storage *db) : db_(db) {}

  /// @throw QueryRuntimeException if an error ocurred.
  void SetReplicationRole(ReplicationQuery::ReplicationRole replication_role, std::optional<int64_t> port) override {
    if (replication_role == ReplicationQuery::ReplicationRole::MAIN) {
      if (!db_->SetReplicationRoleMain()) {
        throw QueryRuntimeException("Couldn't set role to main!");
      }
    } else {
      if (!port || *port < 0 || *port > std::numeric_limits<uint16_t>::max()) {
        throw QueryRuntimeException("Port number invalid!");
      }
      if (!db_->SetReplicationRoleReplica(memgraph::replication::ReplicationServerConfig{
              .ip_address = memgraph::replication::kDefaultReplicationServerIp,
              .port = static_cast<uint16_t>(*port),
          })) {
        throw QueryRuntimeException("Couldn't set role to replica!");
      }
    }
  }

  /// @throw QueryRuntimeException if an error ocurred.
  ReplicationQuery::ReplicationRole ShowReplicationRole() const override {
    auto const &replState = db_->replication_storage_state_.repl_state_;
    switch (replState.GetRole()) {
      case memgraph::replication::ReplicationRole::MAIN:
        return ReplicationQuery::ReplicationRole::MAIN;
      case memgraph::replication::ReplicationRole::REPLICA:
        return ReplicationQuery::ReplicationRole::REPLICA;
    }
    throw QueryRuntimeException("Couldn't show replication role - invalid role set!");
  }

  /// @throw QueryRuntimeException if an error ocurred.
  void RegisterReplica(const std::string &name, const std::string &socket_address,
                       const ReplicationQuery::SyncMode sync_mode,
                       const std::chrono::seconds replica_check_frequency) override {
    auto const &replState = db_->replication_storage_state_.repl_state_;
    if (replState.IsReplica()) {
      // replica can't register another replica
      throw QueryRuntimeException("Replica can't register another replica!");
    }

    if (name == memgraph::replication::kReservedReplicationRoleName) {
      throw QueryRuntimeException("This replica name is reserved and can not be used as replica name!");
    }

    memgraph::replication::ReplicationMode repl_mode;
    switch (sync_mode) {
      case ReplicationQuery::SyncMode::ASYNC: {
        repl_mode = memgraph::replication::ReplicationMode::ASYNC;
        break;
      }
      case ReplicationQuery::SyncMode::SYNC: {
        repl_mode = memgraph::replication::ReplicationMode::SYNC;
        break;
      }
    }

    auto maybe_ip_and_port =
        io::network::Endpoint::ParseSocketOrIpAddress(socket_address, memgraph ::replication::kDefaultReplicationPort);
    if (maybe_ip_and_port) {
      auto [ip, port] = *maybe_ip_and_port;
      auto ret =
          db_->RegisterReplica(storage::replication::RegistrationMode::MUST_BE_INSTANTLY_VALID,
                               replication::ReplicationClientConfig{.name = name,
                                                                    .mode = repl_mode,
                                                                    .ip_address = ip,
                                                                    .port = port,
                                                                    .replica_check_frequency = replica_check_frequency,
                                                                    .ssl = std::nullopt});
      if (ret.HasError()) {
        throw QueryRuntimeException(fmt::format("Couldn't register replica '{}'!", name));
      }
    } else {
      throw QueryRuntimeException("Invalid socket address!");
    }
  }

  /// @throw QueryRuntimeException if an error ocurred.
  void DropReplica(const std::string &replica_name) override {
    auto const &replState = db_->replication_storage_state_.repl_state_;
    if (replState.IsReplica()) {
      // replica can't unregister a replica
      throw QueryRuntimeException("Replica can't unregister a replica!");
    }
    if (!db_->UnregisterReplica(replica_name)) {
      throw QueryRuntimeException(fmt::format("Couldn't unregister the replica '{}'", replica_name));
    }
  }

  using Replica = ReplicationQueryHandler::Replica;
  std::vector<Replica> ShowReplicas() const override {
    auto const &replState = db_->replication_storage_state_.repl_state_;
    if (replState.IsReplica()) {
      // replica can't show registered replicas (it shouldn't have any)
      throw QueryRuntimeException("Replica can't show registered replicas (it shouldn't have any)!");
    }

    auto repl_infos = db_->ReplicasInfo();
    std::vector<Replica> replicas;
    replicas.reserve(repl_infos.size());

    const auto from_info = [](const auto &repl_info) -> Replica {
      Replica replica;
      replica.name = repl_info.name;
      replica.socket_address = repl_info.endpoint.SocketAddress();
      switch (repl_info.mode) {
        case memgraph::replication::ReplicationMode::SYNC:
          replica.sync_mode = ReplicationQuery::SyncMode::SYNC;
          break;
        case memgraph::replication::ReplicationMode::ASYNC:
          replica.sync_mode = ReplicationQuery::SyncMode::ASYNC;
          break;
      }

      replica.current_timestamp_of_replica = repl_info.timestamp_info.current_timestamp_of_replica;
      replica.current_number_of_timestamp_behind_master =
          repl_info.timestamp_info.current_number_of_timestamp_behind_master;

      switch (repl_info.state) {
        case storage::replication::ReplicaState::READY:
          replica.state = ReplicationQuery::ReplicaState::READY;
          break;
        case storage::replication::ReplicaState::REPLICATING:
          replica.state = ReplicationQuery::ReplicaState::REPLICATING;
          break;
        case storage::replication::ReplicaState::RECOVERY:
          replica.state = ReplicationQuery::ReplicaState::RECOVERY;
          break;
        case storage::replication::ReplicaState::INVALID:
          replica.state = ReplicationQuery::ReplicaState::INVALID;
          break;
      }

      return replica;
    };

    std::transform(repl_infos.begin(), repl_infos.end(), std::back_inserter(replicas), from_info);
    return replicas;
  }

 private:
  storage::Storage *db_;
};

/// returns false if the replication role can't be set
/// @throw QueryRuntimeException if an error ocurred.

Callback HandleAuthQuery(AuthQuery *auth_query, InterpreterContext *interpreter_context, const Parameters &parameters) {
  AuthQueryHandler *auth = interpreter_context->auth;
#ifdef MG_ENTERPRISE
  auto *db_handler = interpreter_context->db_handler;
#endif
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

  std::string username = auth_query->user_;
  std::string rolename = auth_query->role_;
  std::string user_or_role = auth_query->user_or_role_;
  std::string database = auth_query->database_;
  std::vector<AuthQuery::Privilege> privileges = auth_query->privileges_;
#ifdef MG_ENTERPRISE
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> label_privileges =
      auth_query->label_privileges_;
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges =
      auth_query->edge_type_privileges_;
#endif
  auto password = EvaluateOptionalExpression(auth_query->password_, evaluator);

  Callback callback;

  const auto license_check_result = license::global_license_checker.IsEnterpriseValid(utils::global_settings);

  static const std::unordered_set enterprise_only_methods{AuthQuery::Action::CREATE_ROLE,
                                                          AuthQuery::Action::DROP_ROLE,
                                                          AuthQuery::Action::SET_ROLE,
                                                          AuthQuery::Action::CLEAR_ROLE,
                                                          AuthQuery::Action::GRANT_PRIVILEGE,
                                                          AuthQuery::Action::DENY_PRIVILEGE,
                                                          AuthQuery::Action::REVOKE_PRIVILEGE,
                                                          AuthQuery::Action::SHOW_PRIVILEGES,
                                                          AuthQuery::Action::SHOW_USERS_FOR_ROLE,
                                                          AuthQuery::Action::SHOW_ROLE_FOR_USER,
                                                          AuthQuery::Action::GRANT_DATABASE_TO_USER,
                                                          AuthQuery::Action::REVOKE_DATABASE_FROM_USER,
                                                          AuthQuery::Action::SHOW_DATABASE_PRIVILEGES,
                                                          AuthQuery::Action::SET_MAIN_DATABASE};

  if (license_check_result.HasError() && enterprise_only_methods.contains(auth_query->action_)) {
    throw utils::BasicException(
        license::LicenseCheckErrorToString(license_check_result.GetError(), "advanced authentication features"));
  }

  switch (auth_query->action_) {
    case AuthQuery::Action::CREATE_USER:
      callback.fn = [auth, username, password, valid_enterprise_license = !license_check_result.HasError()] {
        MG_ASSERT(password.IsString() || password.IsNull());
        if (!auth->CreateUser(username, password.IsString() ? std::make_optional(std::string(password.ValueString()))
                                                            : std::nullopt)) {
          throw QueryRuntimeException("User '{}' already exists.", username);
        }

        // If the license is not valid we create users with admin access
        if (!valid_enterprise_license) {
          spdlog::warn("Granting all the privileges to {}.", username);
          auth->GrantPrivilege(username, kPrivilegesAll
#ifdef MG_ENTERPRISE
                               ,
                               {{{AuthQuery::FineGrainedPrivilege::CREATE_DELETE, {query::kAsterisk}}}},
                               {
                                 {
                                   {
                                     AuthQuery::FineGrainedPrivilege::CREATE_DELETE, { query::kAsterisk }
                                   }
                                 }
                               }
#endif
          );
        }

        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_USER:
      callback.fn = [auth, username] {
        if (!auth->DropUser(username)) {
          throw QueryRuntimeException("User '{}' doesn't exist.", username);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SET_PASSWORD:
      callback.fn = [auth, username, password] {
        MG_ASSERT(password.IsString() || password.IsNull());
        auth->SetPassword(username,
                          password.IsString() ? std::make_optional(std::string(password.ValueString())) : std::nullopt);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CREATE_ROLE:
      callback.fn = [auth, rolename] {
        if (!auth->CreateRole(rolename)) {
          throw QueryRuntimeException("Role '{}' already exists.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_ROLE:
      callback.fn = [auth, rolename] {
        if (!auth->DropRole(rolename)) {
          throw QueryRuntimeException("Role '{}' doesn't exist.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SHOW_USERS:
      callback.header = {"user"};
      callback.fn = [auth] {
        std::vector<std::vector<TypedValue>> rows;
        auto usernames = auth->GetUsernames();
        rows.reserve(usernames.size());
        for (auto &&username : usernames) {
          rows.emplace_back(std::vector<TypedValue>{username});
        }
        return rows;
      };
      return callback;
    case AuthQuery::Action::SHOW_ROLES:
      callback.header = {"role"};
      callback.fn = [auth] {
        std::vector<std::vector<TypedValue>> rows;
        auto rolenames = auth->GetRolenames();
        rows.reserve(rolenames.size());
        for (auto &&rolename : rolenames) {
          rows.emplace_back(std::vector<TypedValue>{rolename});
        }
        return rows;
      };
      return callback;
    case AuthQuery::Action::SET_ROLE:
      callback.fn = [auth, username, rolename] {
        auth->SetRole(username, rolename);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CLEAR_ROLE:
      callback.fn = [auth, username] {
        auth->ClearRole(username);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::GRANT_PRIVILEGE:
      callback.fn = [auth, user_or_role, privileges
#ifdef MG_ENTERPRISE
                     ,
                     label_privileges, edge_type_privileges
#endif
      ] {
        auth->GrantPrivilege(user_or_role, privileges
#ifdef MG_ENTERPRISE
                             ,
                             label_privileges, edge_type_privileges
#endif
        );
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DENY_PRIVILEGE:
      callback.fn = [auth, user_or_role, privileges] {
        auth->DenyPrivilege(user_or_role, privileges);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::REVOKE_PRIVILEGE: {
      callback.fn = [auth, user_or_role, privileges
#ifdef MG_ENTERPRISE
                     ,
                     label_privileges, edge_type_privileges
#endif
      ] {
        auth->RevokePrivilege(user_or_role, privileges
#ifdef MG_ENTERPRISE
                              ,
                              label_privileges, edge_type_privileges
#endif
        );
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case AuthQuery::Action::SHOW_PRIVILEGES:
      callback.header = {"privilege", "effective", "description"};
      callback.fn = [auth, user_or_role] { return auth->GetPrivileges(user_or_role); };
      return callback;
    case AuthQuery::Action::SHOW_ROLE_FOR_USER:
      callback.header = {"role"};
      callback.fn = [auth, username] {
        auto maybe_rolename = auth->GetRolenameForUser(username);
        return std::vector<std::vector<TypedValue>>{
            std::vector<TypedValue>{TypedValue(maybe_rolename ? *maybe_rolename : "null")}};
      };
      return callback;
    case AuthQuery::Action::SHOW_USERS_FOR_ROLE:
      callback.header = {"users"};
      callback.fn = [auth, rolename] {
        std::vector<std::vector<TypedValue>> rows;
        auto usernames = auth->GetUsernamesForRole(rolename);
        rows.reserve(usernames.size());
        for (auto &&username : usernames) {
          rows.emplace_back(std::vector<TypedValue>{username});
        }
        return rows;
      };
      return callback;
    case AuthQuery::Action::GRANT_DATABASE_TO_USER:
#ifdef MG_ENTERPRISE
      callback.fn = [auth, database, username, db_handler] {  // NOLINT
        try {
          std::optional<memgraph::dbms::DatabaseAccess> db =
              std::nullopt;  // Hold pointer to database to protect it until query is done
          if (database != memgraph::auth::kAllDatabases) {
            db = db_handler->Get(database);  // Will throw if databases doesn't exist and protect it during pull
          }
          if (!auth->GrantDatabaseToUser(database, username)) {
            throw QueryRuntimeException("Failed to grant database {} to user {}.", database, username);
          }
        } catch (memgraph::dbms::UnknownDatabaseException &e) {
          throw QueryRuntimeException(e.what());
        }
#else
      callback.fn = [] {
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::REVOKE_DATABASE_FROM_USER:
#ifdef MG_ENTERPRISE
      callback.fn = [auth, database, username, db_handler] {  // NOLINT
        try {
          std::optional<memgraph::dbms::DatabaseAccess> db =
              std::nullopt;  // Hold pointer to database to protect it until query is done
          if (database != memgraph::auth::kAllDatabases) {
            db = db_handler->Get(database);  // Will throw if databases doesn't exist and protect it during pull
          }
          if (!auth->RevokeDatabaseFromUser(database, username)) {
            throw QueryRuntimeException("Failed to revoke database {} from user {}.", database, username);
          }
        } catch (memgraph::dbms::UnknownDatabaseException &e) {
          throw QueryRuntimeException(e.what());
        }
#else
      callback.fn = [] {
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SHOW_DATABASE_PRIVILEGES:
      callback.header = {"grants", "denies"};
#ifdef MG_ENTERPRISE
      callback.fn = [auth, username] {  // NOLINT
        return auth->GetDatabasePrivileges(username);
      };
#else
      callback.fn = [] {  // NOLINT
        return std::vector<std::vector<TypedValue>>();
      };
#endif
      return callback;
    case AuthQuery::Action::SET_MAIN_DATABASE:
#ifdef MG_ENTERPRISE
      callback.fn = [auth, database, username, db_handler] {  // NOLINT
        try {
          const auto db =
              db_handler->Get(database);  // Will throw if databases doesn't exist and protect it during pull
          if (!auth->SetMainDatabase(database, username)) {
            throw QueryRuntimeException("Failed to set main database {} for user {}.", database, username);
          }
        } catch (memgraph::dbms::UnknownDatabaseException &e) {
          throw QueryRuntimeException(e.what());
        }
#else
      callback.fn = [] {
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    default:
      break;
  }
}  // namespace

Callback HandleReplicationQuery(ReplicationQuery *repl_query, const Parameters &parameters, storage::Storage *storage,
                                const query::InterpreterConfig &config, std::vector<Notification> *notifications) {
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

  Callback callback;
  switch (repl_query->action_) {
    case ReplicationQuery::Action::SET_REPLICATION_ROLE: {
      auto port = EvaluateOptionalExpression(repl_query->port_, evaluator);
      std::optional<int64_t> maybe_port;
      if (port.IsInt()) {
        maybe_port = port.ValueInt();
      }
      if (maybe_port == 7687 && repl_query->role_ == ReplicationQuery::ReplicationRole::REPLICA) {
        notifications->emplace_back(SeverityLevel::WARNING, NotificationCode::REPLICA_PORT_WARNING,
                                    "Be careful the replication port must be different from the memgraph port!");
      }
      callback.fn = [handler = ReplQueryHandler{storage}, role = repl_query->role_, maybe_port]() mutable {
        handler.SetReplicationRole(role, maybe_port);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(
          SeverityLevel::INFO, NotificationCode::SET_REPLICA,
          fmt::format("Replica role set to {}.",
                      repl_query->role_ == ReplicationQuery::ReplicationRole::MAIN ? "MAIN" : "REPLICA"));
      return callback;
    }
    case ReplicationQuery::Action::SHOW_REPLICATION_ROLE: {
      callback.header = {"replication role"};
      callback.fn = [handler = ReplQueryHandler{storage}] {
        auto mode = handler.ShowReplicationRole();
        switch (mode) {
          case ReplicationQuery::ReplicationRole::MAIN: {
            return std::vector<std::vector<TypedValue>>{{TypedValue("main")}};
          }
          case ReplicationQuery::ReplicationRole::REPLICA: {
            return std::vector<std::vector<TypedValue>>{{TypedValue("replica")}};
          }
        }
      };
      return callback;
    }
    case ReplicationQuery::Action::REGISTER_REPLICA: {
      const auto &name = repl_query->replica_name_;
      const auto &sync_mode = repl_query->sync_mode_;
      auto socket_address = repl_query->socket_address_->Accept(evaluator);
      const auto replica_check_frequency = config.replication_replica_check_frequency;

      callback.fn = [handler = ReplQueryHandler{storage}, name, socket_address, sync_mode,
                     replica_check_frequency]() mutable {
        handler.RegisterReplica(name, std::string(socket_address.ValueString()), sync_mode, replica_check_frequency);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::REGISTER_REPLICA,
                                  fmt::format("Replica {} is registered.", repl_query->replica_name_));
      return callback;
    }
    case ReplicationQuery::Action::DROP_REPLICA: {
      const auto &name = repl_query->replica_name_;
      callback.fn = [handler = ReplQueryHandler{storage}, name]() mutable {
        handler.DropReplica(name);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::DROP_REPLICA,
                                  fmt::format("Replica {} is dropped.", repl_query->replica_name_));
      return callback;
    }
    case ReplicationQuery::Action::SHOW_REPLICAS: {
      callback.header = {
          "name", "socket_address", "sync_mode", "current_timestamp_of_replica", "number_of_timestamp_behind_master",
          "state"};
      callback.fn = [handler = ReplQueryHandler{storage}, replica_nfields = callback.header.size()] {
        const auto &replicas = handler.ShowReplicas();
        auto typed_replicas = std::vector<std::vector<TypedValue>>{};
        typed_replicas.reserve(replicas.size());
        for (const auto &replica : replicas) {
          std::vector<TypedValue> typed_replica;
          typed_replica.reserve(replica_nfields);

          typed_replica.emplace_back(TypedValue(replica.name));
          typed_replica.emplace_back(TypedValue(replica.socket_address));

          switch (replica.sync_mode) {
            case ReplicationQuery::SyncMode::SYNC:
              typed_replica.emplace_back(TypedValue("sync"));
              break;
            case ReplicationQuery::SyncMode::ASYNC:
              typed_replica.emplace_back(TypedValue("async"));
              break;
          }

          typed_replica.emplace_back(TypedValue(static_cast<int64_t>(replica.current_timestamp_of_replica)));
          typed_replica.emplace_back(
              TypedValue(static_cast<int64_t>(replica.current_number_of_timestamp_behind_master)));

          switch (replica.state) {
            case ReplicationQuery::ReplicaState::READY:
              typed_replica.emplace_back(TypedValue("ready"));
              break;
            case ReplicationQuery::ReplicaState::REPLICATING:
              typed_replica.emplace_back(TypedValue("replicating"));
              break;
            case ReplicationQuery::ReplicaState::RECOVERY:
              typed_replica.emplace_back(TypedValue("recovery"));
              break;
            case ReplicationQuery::ReplicaState::INVALID:
              typed_replica.emplace_back(TypedValue("invalid"));
              break;
          }

          typed_replicas.emplace_back(std::move(typed_replica));
        }
        return typed_replicas;
      };
      return callback;
    }
  }
}

stream::CommonStreamInfo GetCommonStreamInfo(StreamQuery *stream_query, ExpressionVisitor<TypedValue> &evaluator) {
  return {
      .batch_interval = GetOptionalValue<std::chrono::milliseconds>(stream_query->batch_interval_, evaluator)
                            .value_or(stream::kDefaultBatchInterval),
      .batch_size = GetOptionalValue<int64_t>(stream_query->batch_size_, evaluator).value_or(stream::kDefaultBatchSize),
      .transformation_name = stream_query->transform_name_};
}

std::vector<std::string> EvaluateTopicNames(ExpressionVisitor<TypedValue> &evaluator,
                                            std::variant<Expression *, std::vector<std::string>> topic_variant) {
  return std::visit(utils::Overloaded{[&](Expression *expression) {
                                        auto topic_names = expression->Accept(evaluator);
                                        MG_ASSERT(topic_names.IsString());
                                        return utils::Split(topic_names.ValueString(), ",");
                                      },
                                      [&](std::vector<std::string> topic_names) { return topic_names; }},
                    std::move(topic_variant));
}

Callback::CallbackFunction GetKafkaCreateCallback(StreamQuery *stream_query, ExpressionVisitor<TypedValue> &evaluator,
                                                  memgraph::dbms::DatabaseAccess db_acc,
                                                  InterpreterContext *interpreter_context,
                                                  const std::optional<std::string> &username) {
  static constexpr std::string_view kDefaultConsumerGroup = "mg_consumer";
  std::string consumer_group{stream_query->consumer_group_.empty() ? kDefaultConsumerGroup
                                                                   : stream_query->consumer_group_};

  auto bootstrap = GetOptionalStringValue(stream_query->bootstrap_servers_, evaluator);
  if (bootstrap && bootstrap->empty()) {
    throw SemanticException("Bootstrap servers must not be an empty string!");
  }
  auto common_stream_info = GetCommonStreamInfo(stream_query, evaluator);

  const auto get_config_map = [&evaluator](std::unordered_map<Expression *, Expression *> map,
                                           std::string_view map_name) -> std::unordered_map<std::string, std::string> {
    std::unordered_map<std::string, std::string> config_map;
    for (const auto [key_expr, value_expr] : map) {
      const auto key = key_expr->Accept(evaluator);
      const auto value = value_expr->Accept(evaluator);
      if (!key.IsString() || !value.IsString()) {
        throw SemanticException("{} must contain only string keys and values!", map_name);
      }
      config_map.emplace(key.ValueString(), value.ValueString());
    }
    return config_map;
  };

  memgraph::metrics::IncrementCounter(memgraph::metrics::StreamsCreated);

  return [db_acc = std::move(db_acc), interpreter_context, stream_name = stream_query->stream_name_,
          topic_names = EvaluateTopicNames(evaluator, stream_query->topic_names_),
          consumer_group = std::move(consumer_group), common_stream_info = std::move(common_stream_info),
          bootstrap_servers = std::move(bootstrap), owner = username,
          configs = get_config_map(stream_query->configs_, "Configs"),
          credentials = get_config_map(stream_query->credentials_, "Credentials"),
          default_server = interpreter_context->config.default_kafka_bootstrap_servers]() mutable {
    std::string bootstrap = bootstrap_servers ? std::move(*bootstrap_servers) : std::move(default_server);

    db_acc->streams()->Create<query::stream::KafkaStream>(stream_name,
                                                          {.common_info = std::move(common_stream_info),
                                                           .topics = std::move(topic_names),
                                                           .consumer_group = std::move(consumer_group),
                                                           .bootstrap_servers = std::move(bootstrap),
                                                           .configs = std::move(configs),
                                                           .credentials = std::move(credentials)},
                                                          std::move(owner), db_acc, interpreter_context);

    return std::vector<std::vector<TypedValue>>{};
  };
}

Callback::CallbackFunction GetPulsarCreateCallback(StreamQuery *stream_query, ExpressionVisitor<TypedValue> &evaluator,
                                                   memgraph::dbms::DatabaseAccess db,
                                                   InterpreterContext *interpreter_context,
                                                   const std::optional<std::string> &username) {
  auto service_url = GetOptionalStringValue(stream_query->service_url_, evaluator);
  if (service_url && service_url->empty()) {
    throw SemanticException("Service URL must not be an empty string!");
  }
  auto common_stream_info = GetCommonStreamInfo(stream_query, evaluator);
  memgraph::metrics::IncrementCounter(memgraph::metrics::StreamsCreated);

  return [db = std::move(db), interpreter_context, stream_name = stream_query->stream_name_,
          topic_names = EvaluateTopicNames(evaluator, stream_query->topic_names_),
          common_stream_info = std::move(common_stream_info), service_url = std::move(service_url), owner = username,
          default_service = interpreter_context->config.default_pulsar_service_url]() mutable {
    std::string url = service_url ? std::move(*service_url) : std::move(default_service);
    db->streams()->Create<query::stream::PulsarStream>(
        stream_name,
        {.common_info = std::move(common_stream_info), .topics = std::move(topic_names), .service_url = std::move(url)},
        std::move(owner), db, interpreter_context);

    return std::vector<std::vector<TypedValue>>{};
  };
}

Callback HandleStreamQuery(StreamQuery *stream_query, const Parameters &parameters,
                           memgraph::dbms::DatabaseAccess &db_acc, InterpreterContext *interpreter_context,
                           const std::optional<std::string> &username, std::vector<Notification> *notifications) {
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parameters;
  PrimitiveLiteralExpressionEvaluator evaluator{evaluation_context};

  Callback callback;
  switch (stream_query->action_) {
    case StreamQuery::Action::CREATE_STREAM: {
      switch (stream_query->type_) {
        case StreamQuery::Type::KAFKA:
          callback.fn = GetKafkaCreateCallback(stream_query, evaluator, db_acc, interpreter_context, username);
          break;
        case StreamQuery::Type::PULSAR:
          callback.fn = GetPulsarCreateCallback(stream_query, evaluator, db_acc, interpreter_context, username);
          break;
      }
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::CREATE_STREAM,
                                  fmt::format("Created stream {}.", stream_query->stream_name_));
      return callback;
    }
    case StreamQuery::Action::START_STREAM: {
      const auto batch_limit = GetOptionalValue<int64_t>(stream_query->batch_limit_, evaluator);
      const auto timeout = GetOptionalValue<std::chrono::milliseconds>(stream_query->timeout_, evaluator);

      if (batch_limit.has_value()) {
        if (batch_limit.value() < 0) {
          throw utils::BasicException("Parameter BATCH_LIMIT cannot hold negative value");
        }

        callback.fn = [streams = db_acc->streams(), stream_name = stream_query->stream_name_, batch_limit, timeout]() {
          streams->StartWithLimit(stream_name, static_cast<uint64_t>(batch_limit.value()), timeout);
          return std::vector<std::vector<TypedValue>>{};
        };
      } else {
        callback.fn = [streams = db_acc->streams(), stream_name = stream_query->stream_name_]() {
          streams->Start(stream_name);
          return std::vector<std::vector<TypedValue>>{};
        };
        notifications->emplace_back(SeverityLevel::INFO, NotificationCode::START_STREAM,
                                    fmt::format("Started stream {}.", stream_query->stream_name_));
      }
      return callback;
    }
    case StreamQuery::Action::START_ALL_STREAMS: {
      callback.fn = [streams = db_acc->streams()]() {
        streams->StartAll();
        return std::vector<std::vector<TypedValue>>{};
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::START_ALL_STREAMS, "Started all streams.");
      return callback;
    }
    case StreamQuery::Action::STOP_STREAM: {
      callback.fn = [streams = db_acc->streams(), stream_name = stream_query->stream_name_]() {
        streams->Stop(stream_name);
        return std::vector<std::vector<TypedValue>>{};
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::STOP_STREAM,
                                  fmt::format("Stopped stream {}.", stream_query->stream_name_));
      return callback;
    }
    case StreamQuery::Action::STOP_ALL_STREAMS: {
      callback.fn = [streams = db_acc->streams()]() {
        streams->StopAll();
        return std::vector<std::vector<TypedValue>>{};
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::STOP_ALL_STREAMS, "Stopped all streams.");
      return callback;
    }
    case StreamQuery::Action::DROP_STREAM: {
      callback.fn = [streams = db_acc->streams(), stream_name = stream_query->stream_name_]() {
        streams->Drop(stream_name);
        return std::vector<std::vector<TypedValue>>{};
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::DROP_STREAM,
                                  fmt::format("Dropped stream {}.", stream_query->stream_name_));
      return callback;
    }
    case StreamQuery::Action::SHOW_STREAMS: {
      callback.header = {"name", "type", "batch_interval", "batch_size", "transformation_name", "owner", "is running"};
      callback.fn = [streams = db_acc->streams()]() {
        auto streams_status = streams->GetStreamInfo();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(streams_status.size());
        auto stream_info_as_typed_stream_info_emplace_in = [](auto &typed_status, const auto &stream_info) {
          typed_status.emplace_back(stream_info.batch_interval.count());
          typed_status.emplace_back(stream_info.batch_size);
          typed_status.emplace_back(stream_info.transformation_name);
        };

        for (const auto &status : streams_status) {
          std::vector<TypedValue> typed_status;
          typed_status.reserve(7);
          typed_status.emplace_back(status.name);
          typed_status.emplace_back(StreamSourceTypeToString(status.type));
          stream_info_as_typed_stream_info_emplace_in(typed_status, status.info);
          if (status.owner.has_value()) {
            typed_status.emplace_back(*status.owner);
          } else {
            typed_status.emplace_back();
          }
          typed_status.emplace_back(status.is_running);
          results.push_back(std::move(typed_status));
        }

        return results;
      };
      return callback;
    }
    case StreamQuery::Action::CHECK_STREAM: {
      callback.header = {"queries", "raw messages"};

      const auto batch_limit = GetOptionalValue<int64_t>(stream_query->batch_limit_, evaluator);
      if (batch_limit.has_value() && batch_limit.value() < 0) {
        throw utils::BasicException("Parameter BATCH_LIMIT cannot hold negative value");
      }

      callback.fn = [db_acc, stream_name = stream_query->stream_name_,
                     timeout = GetOptionalValue<std::chrono::milliseconds>(stream_query->timeout_, evaluator),
                     batch_limit]() mutable {
        // TODO Is this safe
        return db_acc->streams()->Check(stream_name, db_acc, timeout, batch_limit);
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::CHECK_STREAM,
                                  fmt::format("Checked stream {}.", stream_query->stream_name_));
      return callback;
    }
  }
}

Callback HandleConfigQuery() {
  Callback callback;
  callback.header = {"name", "default_value", "current_value", "description"};

  callback.fn = [] {
    std::vector<GFLAGS_NAMESPACE::CommandLineFlagInfo> flags;
    GetAllFlags(&flags);

    std::vector<std::vector<TypedValue>> results;

    for (const auto &flag : flags) {
      if (flag.hidden ||
          // These flags are not defined with gflags macros but are specified in config/flags.yaml
          flag.name == "help" || flag.name == "help_xml" || flag.name == "version") {
        continue;
      }

      std::vector<TypedValue> current_fields;
      current_fields.emplace_back(flag.name);
      current_fields.emplace_back(flag.default_value);
      current_fields.emplace_back(flag.current_value);
      current_fields.emplace_back(flag.description);

      results.emplace_back(std::move(current_fields));
    }

    return results;
  };
  return callback;
}

Callback HandleSettingQuery(SettingQuery *setting_query, const Parameters &parameters) {
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

  Callback callback;
  switch (setting_query->action_) {
    case SettingQuery::Action::SET_SETTING: {
      const auto setting_name = EvaluateOptionalExpression(setting_query->setting_name_, evaluator);
      if (!setting_name.IsString()) {
        throw utils::BasicException("Setting name should be a string literal");
      }

      const auto setting_value = EvaluateOptionalExpression(setting_query->setting_value_, evaluator);
      if (!setting_value.IsString()) {
        throw utils::BasicException("Setting value should be a string literal");
      }

      callback.fn = [setting_name = std::string{setting_name.ValueString()},
                     setting_value = std::string{setting_value.ValueString()}]() mutable {
        if (!utils::global_settings.SetValue(setting_name, setting_value)) {
          throw utils::BasicException("Unknown setting name '{}'", setting_name);
        }
        return std::vector<std::vector<TypedValue>>{};
      };
      return callback;
    }
    case SettingQuery::Action::SHOW_SETTING: {
      const auto setting_name = EvaluateOptionalExpression(setting_query->setting_name_, evaluator);
      if (!setting_name.IsString()) {
        throw utils::BasicException("Setting name should be a string literal");
      }

      callback.header = {"setting_value"};
      callback.fn = [setting_name = std::string{setting_name.ValueString()}] {
        auto maybe_value = utils::global_settings.GetValue(setting_name);
        if (!maybe_value) {
          throw utils::BasicException("Unknown setting name '{}'", setting_name);
        }
        std::vector<std::vector<TypedValue>> results;
        results.reserve(1);

        std::vector<TypedValue> setting_value;
        setting_value.reserve(1);

        setting_value.emplace_back(*maybe_value);
        results.push_back(std::move(setting_value));
        return results;
      };
      return callback;
    }
    case SettingQuery::Action::SHOW_ALL_SETTINGS: {
      callback.header = {"setting_name", "setting_value"};
      callback.fn = [] {
        auto all_settings = utils::global_settings.AllSettings();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(all_settings.size());

        for (const auto &[k, v] : all_settings) {
          std::vector<TypedValue> setting_info;
          setting_info.reserve(2);

          setting_info.emplace_back(k);
          setting_info.emplace_back(v);
          results.push_back(std::move(setting_info));
        }

        return results;
      };
      return callback;
    }
  }
}

// Struct for lazy pulling from a vector
struct PullPlanVector {
  explicit PullPlanVector(std::vector<std::vector<TypedValue>> values) : values_(std::move(values)) {}

  // @return true if there are more unstreamed elements in vector,
  // false otherwise.
  bool Pull(AnyStream *stream, std::optional<int> n) {
    int local_counter{0};
    while (global_counter < values_.size() && (!n || local_counter < n)) {
      stream->Result(values_[global_counter]);
      ++global_counter;
      ++local_counter;
    }

    return global_counter == values_.size();
  }

 private:
  int global_counter{0};
  std::vector<std::vector<TypedValue>> values_;
};

struct TxTimeout {
  TxTimeout() = default;
  explicit TxTimeout(std::chrono::duration<double> value) noexcept : value_{std::in_place, value} {
    // validation
    // - negative timeout makes no sense
    // - zero timeout means no timeout
    if (value_ <= std::chrono::milliseconds{0}) value_.reset();
  };
  explicit operator bool() const { return value_.has_value(); }

  /// Must call operator bool() first to know if safe
  auto ValueUnsafe() const -> std::chrono::duration<double> const & { return *value_; }

 private:
  std::optional<std::chrono::duration<double>> value_;
};

struct PullPlan {
  explicit PullPlan(std::shared_ptr<CachedPlan> plan, const Parameters &parameters, bool is_profile_query,
                    DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                    std::optional<std::string> username, std::atomic<TransactionStatus> *transaction_status,
                    std::shared_ptr<utils::AsyncTimer> tx_timer,
                    TriggerContextCollector *trigger_context_collector = nullptr,
                    std::optional<size_t> memory_limit = {}, bool use_monotonic_memory = true,
                    FrameChangeCollector *frame_change_collector_ = nullptr);

  std::optional<plan::ProfilingStatsWithTotalTime> Pull(AnyStream *stream, std::optional<int> n,
                                                        const std::vector<Symbol> &output_symbols,
                                                        std::map<std::string, TypedValue> *summary);

 private:
  std::shared_ptr<CachedPlan> plan_ = nullptr;
  plan::UniqueCursorPtr cursor_ = nullptr;
  Frame frame_;
  ExecutionContext ctx_;
  std::optional<size_t> memory_limit_;

  // As it's possible to query execution using multiple pulls
  // we need the keep track of the total execution time across
  // those pulls by accumulating the execution time.
  std::chrono::duration<double> execution_time_{0};

  // To pull the results from a query we call the `Pull` method on
  // the cursor which saves the results in a Frame.
  // Becuase we can't find out if there are some saved results in a frame,
  // and the cursor cannot deduce if the next pull will have a result,
  // we have to keep track of any unsent results from previous `PullPlan::Pull`
  // manually by using this flag.
  bool has_unsent_results_ = false;

  // In the case of LOAD CSV, we want to use only PoolResource without MonotonicMemoryResource
  // to reuse allocated memory. As LOAD CSV is processing row by row
  // it is possible to reduce memory usage significantly if MemoryResource deals with memory allocation
  // can reuse memory that was allocated on processing the first row on all subsequent rows.
  // This flag signals to `PullPlan::Pull` which MemoryResource to use
  bool use_monotonic_memory_;
};

PullPlan::PullPlan(const std::shared_ptr<CachedPlan> plan, const Parameters &parameters, const bool is_profile_query,
                   DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                   std::optional<std::string> username, std::atomic<TransactionStatus> *transaction_status,
                   std::shared_ptr<utils::AsyncTimer> tx_timer, TriggerContextCollector *trigger_context_collector,
                   const std::optional<size_t> memory_limit, bool use_monotonic_memory,
                   FrameChangeCollector *frame_change_collector)
    : plan_(plan),
      cursor_(plan->plan().MakeCursor(execution_memory)),
      frame_(plan->symbol_table().max_position(), execution_memory),
      memory_limit_(memory_limit),
      use_monotonic_memory_(use_monotonic_memory) {
  ctx_.db_accessor = dba;
  ctx_.symbol_table = plan->symbol_table();
  ctx_.evaluation_context.timestamp = QueryTimestamp();
  ctx_.evaluation_context.parameters = parameters;
  ctx_.evaluation_context.properties = NamesToProperties(plan->ast_storage().properties_, dba);
  ctx_.evaluation_context.labels = NamesToLabels(plan->ast_storage().labels_, dba);
#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast() && username.has_value() && dba) {
    // TODO How can we avoid creating this every time? If we must create it, it would be faster with an auth::User
    // instead of the username
    auto auth_checker = interpreter_context->auth_checker->GetFineGrainedAuthChecker(*username, dba);

    // if the user has global privileges to read, edit and write anything, we don't need to perform authorization
    // otherwise, we do assign the auth checker to check for label access control
    if (!auth_checker->HasGlobalPrivilegeOnVertices(AuthQuery::FineGrainedPrivilege::CREATE_DELETE) ||
        !auth_checker->HasGlobalPrivilegeOnEdges(AuthQuery::FineGrainedPrivilege::CREATE_DELETE)) {
      ctx_.auth_checker = std::move(auth_checker);
    }
  }
#endif
  ctx_.timer = std::move(tx_timer);
  ctx_.is_shutting_down = &interpreter_context->is_shutting_down;
  ctx_.transaction_status = transaction_status;
  ctx_.is_profile_query = is_profile_query;
  ctx_.trigger_context_collector = trigger_context_collector;
  ctx_.frame_change_collector = frame_change_collector;
}

std::optional<plan::ProfilingStatsWithTotalTime> PullPlan::Pull(AnyStream *stream, std::optional<int> n,
                                                                const std::vector<Symbol> &output_symbols,
                                                                std::map<std::string, TypedValue> *summary) {
  // Set up temporary memory for a single Pull. Initial memory comes from the
  // stack. 256 KiB should fit on the stack and should be more than enough for a
  // single `Pull`.
  static constexpr size_t stack_size = 256UL * 1024UL;
  char stack_data[stack_size];

  utils::ResourceWithOutOfMemoryException resource_with_exception;
  utils::MonotonicBufferResource monotonic_memory{&stack_data[0], stack_size, &resource_with_exception};
  std::optional<utils::PoolResource> pool_memory;
  static constexpr auto kMaxBlockPerChunks = 128;

  if (!use_monotonic_memory_) {
    pool_memory.emplace(kMaxBlockPerChunks, kExecutionPoolMaxBlockSize, &resource_with_exception,
                        &resource_with_exception);
  } else {
    // We can throw on every query because a simple queries for deleting will use only
    // the stack allocated buffer.
    // Also, we want to throw only when the query engine requests more memory and not the storage
    // so we add the exception to the allocator.
    // TODO (mferencevic): Tune the parameters accordingly.
    pool_memory.emplace(kMaxBlockPerChunks, 1024, &monotonic_memory, &resource_with_exception);
  }

  std::optional<utils::LimitedMemoryResource> maybe_limited_resource;
  if (memory_limit_) {
    maybe_limited_resource.emplace(&*pool_memory, *memory_limit_);
    ctx_.evaluation_context.memory = &*maybe_limited_resource;
  } else {
    ctx_.evaluation_context.memory = &*pool_memory;
  }

  // Returns true if a result was pulled.
  const auto pull_result = [&]() -> bool { return cursor_->Pull(frame_, ctx_); };

  const auto stream_values = [&]() {
    // TODO: The streamed values should also probably use the above memory.
    std::vector<TypedValue> values;
    values.reserve(output_symbols.size());

    for (const auto &symbol : output_symbols) {
      values.emplace_back(frame_[symbol]);
    }

    stream->Result(values);
  };

  // Get the execution time of all possible result pulls and streams.
  utils::Timer timer;

  int i = 0;
  if (has_unsent_results_ && !output_symbols.empty()) {
    // stream unsent results from previous pull
    stream_values();
    ++i;
  }

  for (; !n || i < n; ++i) {
    if (!pull_result()) {
      break;
    }

    if (!output_symbols.empty()) {
      stream_values();
    }
  }

  // If we finished because we streamed the requested n results,
  // we try to pull the next result to see if there is more.
  // If there is additional result, we leave the pulled result in the frame
  // and set the flag to true.
  has_unsent_results_ = i == n && pull_result();

  execution_time_ += timer.Elapsed();

  if (has_unsent_results_) {
    return std::nullopt;
  }

  summary->insert_or_assign("plan_execution_time", execution_time_.count());
  memgraph::metrics::Measure(memgraph::metrics::QueryExecutionLatency_us,
                             std::chrono::duration_cast<std::chrono::microseconds>(execution_time_).count());

  // We are finished with pulling all the data, therefore we can send any
  // metadata about the results i.e. notifications and statistics
  const bool is_any_counter_set =
      std::any_of(ctx_.execution_stats.counters.begin(), ctx_.execution_stats.counters.end(),
                  [](const auto &counter) { return counter > 0; });
  if (is_any_counter_set) {
    std::map<std::string, TypedValue> stats;
    for (size_t i = 0; i < ctx_.execution_stats.counters.size(); ++i) {
      stats.emplace(ExecutionStatsKeyToString(ExecutionStats::Key(i)), ctx_.execution_stats.counters[i]);
    }
    summary->insert_or_assign("stats", std::move(stats));
  }
  cursor_->Shutdown();
  ctx_.profile_execution_time = execution_time_;
  return GetStatsWithTotalTime(ctx_);
}

using RWType = plan::ReadWriteTypeChecker::RWType;

bool IsWriteQueryOnMainMemoryReplica(storage::Storage *storage,
                                     const query::plan::ReadWriteTypeChecker::RWType query_type) {
  if (auto storage_mode = storage->GetStorageMode(); storage_mode == storage::StorageMode::IN_MEMORY_ANALYTICAL ||
                                                     storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto const &replState = storage->replication_storage_state_.repl_state_;
    return replState.IsReplica() && (query_type == RWType::W || query_type == RWType::RW);
  }
  return false;
}

bool IsReplica(storage::Storage *storage) {
  if (auto storage_mode = storage->GetStorageMode(); storage_mode == storage::StorageMode::IN_MEMORY_ANALYTICAL ||
                                                     storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto const &replState = storage->replication_storage_state_.repl_state_;
    return replState.IsReplica();
  }
  return false;
}

}  // namespace

#ifdef MG_ENTERPRISE
InterpreterContext::InterpreterContext(InterpreterConfig interpreter_config, memgraph::dbms::DbmsHandler *handler,
                                       query::AuthQueryHandler *ah, query::AuthChecker *ac)
    : db_handler(handler), config(interpreter_config), auth(ah), auth_checker(ac) {}
#else
InterpreterContext::InterpreterContext(InterpreterConfig interpreter_config,
                                       memgraph::utils::Gatekeeper<memgraph::dbms::Database> *db_gatekeeper,
                                       query::AuthQueryHandler *ah, query::AuthChecker *ac)
    : db_gatekeeper(db_gatekeeper), config(interpreter_config), auth(ah), auth_checker(ac) {}
#endif

Interpreter::Interpreter(InterpreterContext *interpreter_context) : interpreter_context_(interpreter_context) {
  MG_ASSERT(interpreter_context_, "Interpreter context must not be NULL");
#ifndef MG_ENTERPRISE
  auto db_acc = interpreter_context_->db_gatekeeper->access();
  MG_ASSERT(db_acc, "Database accessor needs to be valid");
  current_db_.db_acc_ = std::move(db_acc);
#endif
}

Interpreter::Interpreter(InterpreterContext *interpreter_context, memgraph::dbms::DatabaseAccess db)
    : current_db_{std::move(db)}, interpreter_context_(interpreter_context) {
  MG_ASSERT(current_db_.db_acc_, "Database accessor needs to be valid");
  MG_ASSERT(interpreter_context_, "Interpreter context must not be NULL");
}

auto DetermineTxTimeout(std::optional<int64_t> tx_timeout_ms, InterpreterConfig const &config) -> TxTimeout {
  using double_seconds = std::chrono::duration<double>;

  auto const global_tx_timeout = double_seconds{flags::run_time::execution_timeout_sec_};
  auto const valid_global_tx_timeout = global_tx_timeout > double_seconds{0};

  if (tx_timeout_ms) {
    auto const timeout = std::chrono::duration_cast<double_seconds>(std::chrono::milliseconds{*tx_timeout_ms});
    if (valid_global_tx_timeout) return TxTimeout{std::min(global_tx_timeout, timeout)};
    return TxTimeout{timeout};
  }
  if (valid_global_tx_timeout) {
    return TxTimeout{global_tx_timeout};
  }
  return TxTimeout{};
}

auto CreateTimeoutTimer(QueryExtras const &extras, InterpreterConfig const &config)
    -> std::shared_ptr<utils::AsyncTimer> {
  if (auto const timeout = DetermineTxTimeout(extras.tx_timeout, config)) {
    return std::make_shared<utils::AsyncTimer>(timeout.ValueUnsafe().count());
  }
  return {};
}

PreparedQuery Interpreter::PrepareTransactionQuery(std::string_view query_upper, QueryExtras const &extras) {
  std::function<void()> handler;

  if (query_upper == "BEGIN") {
    query_executions_.clear();
    transaction_queries_->clear();
    // TODO: Evaluate doing move(extras). Currently the extras is very small, but this will be important if it ever
    // becomes large.
    handler = [this, extras = extras] {
      if (in_explicit_transaction_) {
        throw ExplicitTransactionUsageException("Nested transactions are not supported.");
      }
      SetupInterpreterTransaction(extras);
      in_explicit_transaction_ = true;
      expect_rollback_ = false;
      if (!current_db_.db_acc_) throw DatabaseContextRequiredException("No current database for transaction defined.");
      SetupDatabaseTransaction(true);
    };
  } else if (query_upper == "COMMIT") {
    handler = [this] {
      if (!in_explicit_transaction_) {
        throw ExplicitTransactionUsageException("No current transaction to commit.");
      }
      if (expect_rollback_) {
        throw ExplicitTransactionUsageException(
            "Transaction can't be committed because there was a previous "
            "error. Please invoke a rollback instead.");
      }

      try {
        Commit();
      } catch (const utils::BasicException &) {
        AbortCommand(nullptr);
        throw;
      }

      expect_rollback_ = false;
      in_explicit_transaction_ = false;
      metadata_ = std::nullopt;
      current_timeout_timer_.reset();
    };
  } else if (query_upper == "ROLLBACK") {
    handler = [this] {
      if (!in_explicit_transaction_) {
        throw ExplicitTransactionUsageException("No current transaction to rollback.");
      }

      memgraph::metrics::IncrementCounter(memgraph::metrics::RollbackedTransactions);

      Abort();
      expect_rollback_ = false;
      in_explicit_transaction_ = false;
      metadata_ = std::nullopt;
      current_timeout_timer_.reset();
    };
  } else {
    LOG_FATAL("Should not get here -- unknown transaction query!");
  }

  return {{},
          {},
          [handler = std::move(handler)](AnyStream *, std::optional<int>) {
            handler();
            return QueryHandlerResult::NOTHING;
          },
          RWType::NONE};
}

inline static void TryCaching(const AstStorage &ast_storage, FrameChangeCollector *frame_change_collector) {
  if (!frame_change_collector) return;
  for (const auto &tree : ast_storage.storage_) {
    if (tree->GetTypeInfo() != memgraph::query::InListOperator::kType) {
      continue;
    }
    auto *in_list_operator = utils::Downcast<InListOperator>(tree.get());
    const auto cached_id = memgraph::utils::GetFrameChangeId(*in_list_operator);
    if (!cached_id || cached_id->empty()) {
      continue;
    }
    frame_change_collector->AddTrackingKey(*cached_id);
    spdlog::trace("Tracking {} operator, by id: {}", InListOperator::kType.name, *cached_id);
  }
}

bool IsLoadCsvQuery(const std::vector<memgraph::query::Clause *> &clauses) {
  return std::any_of(clauses.begin(), clauses.end(),
                     [](memgraph::query::Clause const *clause) { return clause->GetTypeInfo() == LoadCsv::kType; });
}

bool IsCallBatchedProcedureQuery(const std::vector<memgraph::query::Clause *> &clauses) {
  EvaluationContext evaluation_context;

  return std::ranges::any_of(clauses, [&evaluation_context](memgraph::query::Clause *clause) -> bool {
    if (!(clause->GetTypeInfo() == CallProcedure::kType)) return false;
    auto *call_procedure_clause = utils::Downcast<CallProcedure>(clause);

    const auto &maybe_found = memgraph::query::procedure::FindProcedure(
        procedure::gModuleRegistry, call_procedure_clause->procedure_name_, evaluation_context.memory);
    if (!maybe_found) {
      throw QueryRuntimeException("There is no procedure named '{}'.", call_procedure_clause->procedure_name_);
    }
    const auto &[module, proc] = *maybe_found;
    if (!proc->info.is_batched) return false;
    spdlog::trace("Using PoolResource for batched query procedure");
    return true;
  });
}

PreparedQuery PrepareCypherQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
                                 InterpreterContext *interpreter_context, CurrentDB &current_db,
                                 utils::MemoryResource *execution_memory, std::vector<Notification> *notifications,
                                 std::optional<std::string> const &username,
                                 std::atomic<TransactionStatus> *transaction_status,
                                 std::shared_ptr<utils::AsyncTimer> tx_timer,
                                 FrameChangeCollector *frame_change_collector = nullptr) {
  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_query.query);

  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parsed_query.parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

  const auto memory_limit = EvaluateMemoryLimit(evaluator, cypher_query->memory_limit_, cypher_query->memory_scale_);
  if (memory_limit) {
    spdlog::info("Running query with memory limit of {}", utils::GetReadableSize(*memory_limit));
  }
  auto clauses = cypher_query->single_query_->clauses_;
  bool contains_csv = false;
  if (std::any_of(clauses.begin(), clauses.end(),
                  [](const auto *clause) { return clause->GetTypeInfo() == LoadCsv::kType; })) {
    notifications->emplace_back(
        SeverityLevel::INFO, NotificationCode::LOAD_CSV_TIP,
        "It's important to note that the parser parses the values as strings. It's up to the user to "
        "convert the parsed row values to the appropriate type. This can be done using the built-in "
        "conversion functions such as ToInteger, ToFloat, ToBoolean etc.");
    contains_csv = true;
  }

  // If this is LOAD CSV query, use PoolResource without MonotonicMemoryResource as we want to reuse allocated memory
  auto use_monotonic_memory =
      !contains_csv && !IsCallBatchedProcedureQuery(clauses) && !IsAllShortestPathsQuery(clauses);
  spdlog::trace("PrepareCypher has {} encountered all shortest paths and will {} use of monotonic memory",
                IsAllShortestPathsQuery(clauses) ? "" : "not", use_monotonic_memory ? "" : "not");

  MG_ASSERT(current_db.execution_db_accessor_, "Cypher query expects a current DB transaction");
  auto *dba =
      &*current_db
            .execution_db_accessor_;  // todo pass the full current_db into planner...make plan optimisation optional

  const auto is_cacheable = parsed_query.is_cacheable;
  auto *plan_cache = is_cacheable ? current_db.db_acc_->get()->plan_cache() : nullptr;

  auto plan = CypherQueryToPlan(parsed_query.stripped_query.hash(), std::move(parsed_query.ast_storage), cypher_query,
                                parsed_query.parameters, plan_cache, dba);

  TryCaching(plan->ast_storage(), frame_change_collector);
  summary->insert_or_assign("cost_estimate", plan->cost());
  auto rw_type_checker = plan::ReadWriteTypeChecker();
  rw_type_checker.InferRWType(const_cast<plan::LogicalOperator &>(plan->plan()));

  auto output_symbols = plan->plan().OutputSymbols(plan->symbol_table());

  std::vector<std::string> header;
  header.reserve(output_symbols.size());

  for (const auto &symbol : output_symbols) {
    // When the symbol is aliased or expanded from '*' (inside RETURN or
    // WITH), then there is no token position, so use symbol name.
    // Otherwise, find the name from stripped query.
    header.push_back(
        utils::FindOr(parsed_query.stripped_query.named_expressions(), symbol.token_position(), symbol.name()).first);
  }
  // TODO: pass current DB into plan, in future current can change during pull
  auto *trigger_context_collector =
      current_db.trigger_context_collector_ ? &*current_db.trigger_context_collector_ : nullptr;
  auto pull_plan = std::make_shared<PullPlan>(
      plan, parsed_query.parameters, false, dba, interpreter_context, execution_memory, username, transaction_status,
      std::move(tx_timer), trigger_context_collector, memory_limit, use_monotonic_memory,
      frame_change_collector->IsTrackingValues() ? frame_change_collector : nullptr);
  return PreparedQuery{std::move(header), std::move(parsed_query.required_privileges),
                       [pull_plan = std::move(pull_plan), output_symbols = std::move(output_symbols), summary](
                           AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         if (pull_plan->Pull(stream, n, output_symbols, summary)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       rw_type_checker.type};
}

PreparedQuery PrepareExplainQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
                                  InterpreterContext *interpreter_context, CurrentDB &current_db) {
  const std::string kExplainQueryStart = "explain ";
  MG_ASSERT(utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.query()), kExplainQueryStart),
            "Expected stripped query to start with '{}'", kExplainQueryStart);

  // Parse and cache the inner query separately (as if it was a standalone
  // query), producing a fresh AST. Note that currently we cannot just reuse
  // part of the already produced AST because the parameters within ASTs are
  // looked up using their positions within the string that was parsed. These
  // wouldn't match up if if we were to reuse the AST (produced by parsing the
  // full query string) when given just the inner query to execute.
  ParsedQuery parsed_inner_query =
      ParseQuery(parsed_query.query_string.substr(kExplainQueryStart.size()), parsed_query.user_parameters,
                 &interpreter_context->ast_cache, interpreter_context->config.query);

  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_inner_query.query);
  MG_ASSERT(cypher_query, "Cypher grammar should not allow other queries in EXPLAIN");

  MG_ASSERT(current_db.execution_db_accessor_, "Explain query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *plan_cache = parsed_inner_query.is_cacheable ? current_db.db_acc_->get()->plan_cache() : nullptr;

  auto cypher_query_plan =
      CypherQueryToPlan(parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage),
                        cypher_query, parsed_inner_query.parameters, plan_cache, dba);

  std::stringstream printed_plan;
  plan::PrettyPrint(*dba, &cypher_query_plan->plan(), &printed_plan);

  std::vector<std::vector<TypedValue>> printed_plan_rows;
  for (const auto &row : utils::Split(utils::RTrim(printed_plan.str()), "\n")) {
    printed_plan_rows.push_back(std::vector<TypedValue>{TypedValue(row)});
  }

  summary->insert_or_assign("explain", plan::PlanToJson(*dba, &cypher_query_plan->plan()).dump());

  return PreparedQuery{{"QUERY PLAN"},
                       std::move(parsed_query.required_privileges),
                       [pull_plan = std::make_shared<PullPlanVector>(std::move(printed_plan_rows))](
                           AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareProfileQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                  std::map<std::string, TypedValue> *summary, InterpreterContext *interpreter_context,
                                  CurrentDB &current_db, utils::MemoryResource *execution_memory,
                                  std::optional<std::string> const &username,
                                  std::atomic<TransactionStatus> *transaction_status,
                                  std::shared_ptr<utils::AsyncTimer> tx_timer,
                                  FrameChangeCollector *frame_change_collector) {
  const std::string kProfileQueryStart = "profile ";

  MG_ASSERT(utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.query()), kProfileQueryStart),
            "Expected stripped query to start with '{}'", kProfileQueryStart);

  // PROFILE isn't allowed inside multi-command (explicit) transactions. This is
  // because PROFILE executes each PROFILE'd query and collects additional
  // perfomance metadata that it displays to the user instead of the results
  // yielded by the query. Because PROFILE has side-effects, each transaction
  // that is used to execute a PROFILE query *MUST* be aborted. That isn't
  // possible when using multicommand (explicit) transactions (because the user
  // controls the lifetime of the transaction) and that is why PROFILE is
  // explicitly disabled here in multicommand (explicit) transactions.
  // NOTE: Unlike PROFILE, EXPLAIN doesn't have any unwanted side-effects (in
  // transaction terms) because it doesn't execute the query, it just prints its
  // query plan. That is why EXPLAIN can be used in multicommand (explicit)
  // transactions.
  if (in_explicit_transaction) {
    throw ProfileInMulticommandTxException();
  }

  if (!memgraph::utils::IsAvailableTSC()) {
    throw QueryException("TSC support is missing for PROFILE");
  }

  // Parse and cache the inner query separately (as if it was a standalone
  // query), producing a fresh AST. Note that currently we cannot just reuse
  // part of the already produced AST because the parameters within ASTs are
  // looked up using their positions within the string that was parsed. These
  // wouldn't match up if if we were to reuse the AST (produced by parsing the
  // full query string) when given just the inner query to execute.
  ParsedQuery parsed_inner_query =
      ParseQuery(parsed_query.query_string.substr(kProfileQueryStart.size()), parsed_query.user_parameters,
                 &interpreter_context->ast_cache, interpreter_context->config.query);

  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_inner_query.query);

  bool contains_csv = false;
  auto clauses = cypher_query->single_query_->clauses_;
  if (std::any_of(clauses.begin(), clauses.end(),
                  [](const auto *clause) { return clause->GetTypeInfo() == LoadCsv::kType; })) {
    contains_csv = true;
  }

  // If this is LOAD CSV, BatchedProcedure or AllShortest query, use PoolResource without MonotonicMemoryResource as we
  // want to reuse allocated memory
  auto use_monotonic_memory =
      !contains_csv && !IsCallBatchedProcedureQuery(clauses) && !IsAllShortestPathsQuery(clauses);

  MG_ASSERT(cypher_query, "Cypher grammar should not allow other queries in PROFILE");
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parsed_inner_query.parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
  const auto memory_limit = EvaluateMemoryLimit(evaluator, cypher_query->memory_limit_, cypher_query->memory_scale_);

  MG_ASSERT(current_db.execution_db_accessor_, "Profile query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *plan_cache = parsed_inner_query.is_cacheable ? current_db.db_acc_->get()->plan_cache() : nullptr;
  auto cypher_query_plan =
      CypherQueryToPlan(parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage),
                        cypher_query, parsed_inner_query.parameters, plan_cache, dba);
  TryCaching(cypher_query_plan->ast_storage(), frame_change_collector);
  auto rw_type_checker = plan::ReadWriteTypeChecker();

  rw_type_checker.InferRWType(const_cast<plan::LogicalOperator &>(cypher_query_plan->plan()));

  return PreparedQuery{{"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"},
                       std::move(parsed_query.required_privileges),
                       [plan = std::move(cypher_query_plan), parameters = std::move(parsed_inner_query.parameters),
                        summary, dba, interpreter_context, execution_memory, memory_limit, username,
                        // We want to execute the query we are profiling lazily, so we delay
                        // the construction of the corresponding context.
                        stats_and_total_time = std::optional<plan::ProfilingStatsWithTotalTime>{},
                        pull_plan = std::shared_ptr<PullPlanVector>(nullptr), transaction_status, use_monotonic_memory,
                        frame_change_collector, tx_timer = std::move(tx_timer)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         // No output symbols are given so that nothing is streamed.
                         if (!stats_and_total_time) {
                           stats_and_total_time =
                               PullPlan(plan, parameters, true, dba, interpreter_context, execution_memory, username,
                                        transaction_status, std::move(tx_timer), nullptr, memory_limit,
                                        use_monotonic_memory,
                                        frame_change_collector->IsTrackingValues() ? frame_change_collector : nullptr)
                                   .Pull(stream, {}, {}, summary);
                           pull_plan = std::make_shared<PullPlanVector>(ProfilingStatsToTable(*stats_and_total_time));
                         }

                         MG_ASSERT(stats_and_total_time, "Failed to execute the query!");

                         if (pull_plan->Pull(stream, n)) {
                           summary->insert_or_assign("profile", ProfilingStatsToJson(*stats_and_total_time).dump());
                           return QueryHandlerResult::ABORT;
                         }

                         return std::nullopt;
                       },
                       rw_type_checker.type};
}

PreparedQuery PrepareDumpQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
  MG_ASSERT(current_db.execution_db_accessor_, "Dump query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;
  auto plan = std::make_shared<PullPlanDump>(dba);
  return PreparedQuery{
      {"QUERY"},
      std::move(parsed_query.required_privileges),
      [pull_plan = std::move(plan)](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      },
      RWType::R};
}

std::vector<std::vector<TypedValue>> AnalyzeGraphQueryHandler::AnalyzeGraphCreateStatistics(
    const std::span<std::string> labels, DbAccessor *execution_db_accessor) {
  using LPIndex = std::pair<storage::LabelId, storage::PropertyId>;
  auto view = storage::View::OLD;

  auto erase_not_specified_label_indices = [&labels, execution_db_accessor](auto &index_info) {
    if (labels[0] == kAsterisk) {
      return;
    }

    for (auto it = index_info.cbegin(); it != index_info.cend();) {
      if (std::find(labels.begin(), labels.end(), execution_db_accessor->LabelToName(*it)) == labels.end()) {
        it = index_info.erase(it);
      } else {
        ++it;
      }
    }
  };

  auto erase_not_specified_label_property_indices = [&labels, execution_db_accessor](auto &index_info) {
    if (labels[0] == kAsterisk) {
      return;
    }

    for (auto it = index_info.cbegin(); it != index_info.cend();) {
      if (std::find(labels.begin(), labels.end(), execution_db_accessor->LabelToName(it->first)) == labels.end()) {
        it = index_info.erase(it);
      } else {
        ++it;
      }
    }
  };

  auto populate_label_stats = [execution_db_accessor, view](auto index_info) {
    std::vector<std::pair<storage::LabelId, storage::LabelIndexStats>> label_stats;
    label_stats.reserve(index_info.size());
    std::for_each(index_info.begin(), index_info.end(),
                  [execution_db_accessor, view, &label_stats](const storage::LabelId &label_id) {
                    auto vertices = execution_db_accessor->Vertices(view, label_id);
                    uint64_t no_vertices{0};
                    uint64_t total_degree{0};
                    std::for_each(vertices.begin(), vertices.end(),
                                  [&total_degree, &no_vertices, &view](const auto &vertex) {
                                    no_vertices++;
                                    total_degree += *vertex.OutDegree(view) + *vertex.InDegree(view);
                                  });

                    auto average_degree =
                        no_vertices > 0 ? static_cast<double>(total_degree) / static_cast<double>(no_vertices) : 0;
                    auto index_stats = storage::LabelIndexStats{.count = no_vertices, .avg_degree = average_degree};
                    execution_db_accessor->SetIndexStats(label_id, index_stats);
                    label_stats.emplace_back(label_id, index_stats);
                  });

    return label_stats;
  };

  auto populate_label_property_stats = [execution_db_accessor, view](auto &index_info) {
    std::map<LPIndex, std::map<storage::PropertyValue, int64_t>> label_property_counter;
    std::map<LPIndex, uint64_t> vertex_degree_counter;
    // Iterate over all label property indexed vertices
    std::for_each(
        index_info.begin(), index_info.end(),
        [execution_db_accessor, &label_property_counter, &vertex_degree_counter, view](const LPIndex &index_element) {
          auto &lp_counter = label_property_counter[index_element];
          auto &vd_counter = vertex_degree_counter[index_element];
          auto vertices = execution_db_accessor->Vertices(view, index_element.first, index_element.second);
          std::for_each(vertices.begin(), vertices.end(),
                        [&index_element, &lp_counter, &vd_counter, &view](const auto &vertex) {
                          lp_counter[*vertex.GetProperty(view, index_element.second)]++;
                          vd_counter += *vertex.OutDegree(view) + *vertex.InDegree(view);
                        });
        });

    std::vector<std::pair<LPIndex, storage::LabelPropertyIndexStats>> label_property_stats;
    label_property_stats.reserve(label_property_counter.size());
    std::for_each(
        label_property_counter.begin(), label_property_counter.end(),
        [execution_db_accessor, &vertex_degree_counter, &label_property_stats](const auto &counter_entry) {
          const auto &[label_property, values_map] = counter_entry;
          // Extract info
          uint64_t count_property_value = std::accumulate(
              values_map.begin(), values_map.end(), 0,
              [](uint64_t prev_value, const auto &prop_value_count) { return prev_value + prop_value_count.second; });
          // num_distinc_values will never be 0
          double avg_group_size = static_cast<double>(count_property_value) / static_cast<double>(values_map.size());
          double chi_squared_stat = std::accumulate(
              values_map.begin(), values_map.end(), 0.0, [avg_group_size](double prev_result, const auto &value_entry) {
                return prev_result + utils::ChiSquaredValue(value_entry.second, avg_group_size);
              });

          double average_degree = count_property_value > 0
                                      ? static_cast<double>(vertex_degree_counter[label_property]) /
                                            static_cast<double>(count_property_value)
                                      : 0;

          auto index_stats =
              storage::LabelPropertyIndexStats{.count = count_property_value,
                                               .distinct_values_count = static_cast<uint64_t>(values_map.size()),
                                               .statistic = chi_squared_stat,
                                               .avg_group_size = avg_group_size,
                                               .avg_degree = average_degree};
          execution_db_accessor->SetIndexStats(label_property.first, label_property.second, index_stats);
          label_property_stats.push_back(std::make_pair(label_property, index_stats));
        });

    return label_property_stats;
  };

  auto index_info = execution_db_accessor->ListAllIndices();

  std::vector<storage::LabelId> label_indices_info = index_info.label;
  erase_not_specified_label_indices(label_indices_info);
  auto label_stats = populate_label_stats(label_indices_info);

  std::vector<LPIndex> label_property_indices_info = index_info.label_property;
  erase_not_specified_label_property_indices(label_property_indices_info);
  auto label_property_stats = populate_label_property_stats(label_property_indices_info);

  std::vector<std::vector<TypedValue>> results;
  results.reserve(label_stats.size() + label_property_stats.size());

  std::for_each(label_stats.begin(), label_stats.end(), [execution_db_accessor, &results](const auto &stat_entry) {
    std::vector<TypedValue> result;
    result.reserve(kComputeStatisticsNumResults);

    result.emplace_back(execution_db_accessor->LabelToName(stat_entry.first));
    result.emplace_back(TypedValue());
    result.emplace_back(static_cast<int64_t>(stat_entry.second.count));
    result.emplace_back(TypedValue());
    result.emplace_back(TypedValue());
    result.emplace_back(TypedValue());
    result.emplace_back(stat_entry.second.avg_degree);
    results.push_back(std::move(result));
  });

  std::for_each(label_property_stats.begin(), label_property_stats.end(),
                [execution_db_accessor, &results](const auto &stat_entry) {
                  std::vector<TypedValue> result;
                  result.reserve(kComputeStatisticsNumResults);

                  result.emplace_back(execution_db_accessor->LabelToName(stat_entry.first.first));
                  result.emplace_back(execution_db_accessor->PropertyToName(stat_entry.first.second));
                  result.emplace_back(static_cast<int64_t>(stat_entry.second.count));
                  result.emplace_back(static_cast<int64_t>(stat_entry.second.distinct_values_count));
                  result.emplace_back(stat_entry.second.avg_group_size);
                  result.emplace_back(stat_entry.second.statistic);
                  result.emplace_back(stat_entry.second.avg_degree);
                  results.push_back(std::move(result));
                });

  return results;
}

std::vector<std::vector<TypedValue>> AnalyzeGraphQueryHandler::AnalyzeGraphDeleteStatistics(
    const std::span<std::string> labels, DbAccessor *execution_db_accessor) {
  auto erase_not_specified_label_indices = [&labels, execution_db_accessor](auto &index_info) {
    if (labels[0] == kAsterisk) {
      return;
    }

    for (auto it = index_info.cbegin(); it != index_info.cend();) {
      if (std::find(labels.begin(), labels.end(), execution_db_accessor->LabelToName(*it)) == labels.end()) {
        it = index_info.erase(it);
      } else {
        ++it;
      }
    }
  };

  auto erase_not_specified_label_property_indices = [&labels, execution_db_accessor](auto &index_info) {
    if (labels[0] == kAsterisk) {
      return;
    }

    for (auto it = index_info.cbegin(); it != index_info.cend();) {
      if (std::find(labels.begin(), labels.end(), execution_db_accessor->LabelToName(it->first)) == labels.end()) {
        it = index_info.erase(it);
      } else {
        ++it;
      }
    }
  };

  auto populate_label_results = [execution_db_accessor](auto index_info) {
    std::vector<storage::LabelId> label_results;
    label_results.reserve(index_info.size());
    std::for_each(index_info.begin(), index_info.end(),
                  [execution_db_accessor, &label_results](const storage::LabelId &label_id) {
                    const auto res = execution_db_accessor->DeleteLabelIndexStats(label_id);
                    if (res) label_results.emplace_back(label_id);
                  });

    return label_results;
  };

  auto populate_label_property_results = [execution_db_accessor](auto index_info) {
    std::vector<std::pair<storage::LabelId, storage::PropertyId>> label_property_results;
    label_property_results.reserve(index_info.size());
    std::for_each(index_info.begin(), index_info.end(),
                  [execution_db_accessor,
                   &label_property_results](const std::pair<storage::LabelId, storage::PropertyId> &label_property) {
                    const auto &res = execution_db_accessor->DeleteLabelPropertyIndexStats(label_property.first);
                    label_property_results.insert(label_property_results.end(), res.begin(), res.end());
                  });

    return label_property_results;
  };

  auto index_info = execution_db_accessor->ListAllIndices();

  std::vector<storage::LabelId> label_indices_info = index_info.label;
  erase_not_specified_label_indices(label_indices_info);
  auto label_results = populate_label_results(label_indices_info);

  std::vector<std::pair<storage::LabelId, storage::PropertyId>> label_property_indices_info = index_info.label_property;
  erase_not_specified_label_property_indices(label_property_indices_info);
  auto label_prop_results = populate_label_property_results(label_property_indices_info);

  std::vector<std::vector<TypedValue>> results;
  results.reserve(label_results.size() + label_prop_results.size());

  std::transform(
      label_results.begin(), label_results.end(), std::back_inserter(results),
      [execution_db_accessor](const auto &label_index) {
        return std::vector<TypedValue>{TypedValue(execution_db_accessor->LabelToName(label_index)), TypedValue("")};
      });

  std::transform(label_prop_results.begin(), label_prop_results.end(), std::back_inserter(results),
                 [execution_db_accessor](const auto &label_property_index) {
                   return std::vector<TypedValue>{
                       TypedValue(execution_db_accessor->LabelToName(label_property_index.first)),
                       TypedValue(execution_db_accessor->PropertyToName(label_property_index.second))};
                 });

  return results;
}

Callback HandleAnalyzeGraphQuery(AnalyzeGraphQuery *analyze_graph_query, DbAccessor *execution_db_accessor) {
  Callback callback;
  switch (analyze_graph_query->action_) {
    case AnalyzeGraphQuery::Action::ANALYZE: {
      callback.header = {"label",      "property",       "num estimation nodes",
                         "num groups", "avg group size", "chi-squared value",
                         "avg degree"};
      callback.fn = [handler = AnalyzeGraphQueryHandler(), labels = analyze_graph_query->labels_,
                     execution_db_accessor]() mutable {
        return handler.AnalyzeGraphCreateStatistics(labels, execution_db_accessor);
      };
      break;
    }
    case AnalyzeGraphQuery::Action::DELETE: {
      callback.header = {"label", "property"};
      callback.fn = [handler = AnalyzeGraphQueryHandler(), labels = analyze_graph_query->labels_,
                     execution_db_accessor]() mutable {
        return handler.AnalyzeGraphDeleteStatistics(labels, execution_db_accessor);
      };
      break;
    }
  }

  return callback;
}

PreparedQuery PrepareAnalyzeGraphQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw AnalyzeGraphInMulticommandTxException();
  }
  MG_ASSERT(current_db.db_acc_, "Analyze Graph query expects a current DB");

  // Creating an index influences computed plan costs.
  auto invalidate_plan_cache = [plan_cache = current_db.db_acc_->get()->plan_cache()] {
    auto access = plan_cache->access();
    for (auto &kv : access) {
      access.remove(kv.first);
    }
  };
  utils::OnScopeExit cache_invalidator(invalidate_plan_cache);

  auto *analyze_graph_query = utils::Downcast<AnalyzeGraphQuery>(parsed_query.query);
  MG_ASSERT(analyze_graph_query);

  MG_ASSERT(current_db.execution_db_accessor_, "Analyze Graph query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto callback = HandleAnalyzeGraphQuery(analyze_graph_query, dba);

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  auto *index_query = utils::Downcast<IndexQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  // TODO: we will need transaction for replication
  MG_ASSERT(current_db.db_acc_, "Index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  // Creating an index influences computed plan costs.
  auto invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    auto access = plan_cache->access();
    for (auto &kv : access) {
      access.remove(kv.first);
    }
  };

  auto *storage = db_acc->storage();
  auto label = storage->NameToLabel(index_query->label_.name);

  std::vector<storage::PropertyId> properties;
  std::vector<std::string> properties_string;
  properties.reserve(index_query->properties_.size());
  properties_string.reserve(index_query->properties_.size());
  for (const auto &prop : index_query->properties_) {
    properties.push_back(storage->NameToProperty(prop.name));
    properties_string.push_back(prop.name);
  }
  auto properties_stringified = utils::Join(properties_string, ", ");

  if (properties.size() > 1) {
    throw utils::NotYetImplemented("index on multiple properties");
  }

  Notification index_notification(SeverityLevel::INFO);
  switch (index_query->action_) {
    case IndexQuery::Action::CREATE: {
      index_notification.code = NotificationCode::CREATE_INDEX;
      index_notification.title =
          fmt::format("Created index on label {} on properties {}.", index_query->label_.name, properties_stringified);

      // TODO: not just storage + invalidate_plan_cache. Need a DB transaction (for replication)
      handler = [dba, label, properties_stringified = std::move(properties_stringified),
                 label_name = index_query->label_.name, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        MG_ASSERT(properties.size() <= 1U);
        auto maybe_index_error = properties.empty() ? dba->CreateIndex(label) : dba->CreateIndex(label, properties[0]);
        utils::OnScopeExit invalidator(invalidate_plan_cache);

        if (maybe_index_error.HasError()) {
          index_notification.code = NotificationCode::EXISTENT_INDEX;
          index_notification.title =
              fmt::format("Index on label {} on properties {} already exists.", label_name, properties_stringified);
          // ABORT?
        }
      };
      break;
    }
    case IndexQuery::Action::DROP: {
      index_notification.code = NotificationCode::DROP_INDEX;
      index_notification.title = fmt::format("Dropped index on label {} on properties {}.", index_query->label_.name,
                                             utils::Join(properties_string, ", "));
      // TODO: not just storage + invalidate_plan_cache. Need a DB transaction (for replication)
      handler = [dba, label, properties_stringified = std::move(properties_stringified),
                 label_name = index_query->label_.name, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        MG_ASSERT(properties.size() <= 1U);
        auto maybe_index_error = properties.empty() ? dba->DropIndex(label) : dba->DropIndex(label, properties[0]);
        utils::OnScopeExit invalidator(invalidate_plan_cache);

        if (maybe_index_error.HasError()) {
          index_notification.code = NotificationCode::NONEXISTENT_INDEX;
          index_notification.title =
              fmt::format("Index on label {} on properties {} doesn't exist.", label_name, properties_stringified);
        }
      };
      break;
    }
  }

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [handler = std::move(handler), notifications, index_notification = std::move(index_notification)](
          AnyStream * /*stream*/, std::optional<int> /*unused*/) mutable {
        handler(index_notification);
        notifications->push_back(index_notification);
        return QueryHandlerResult::COMMIT;  // TODO: Will need to become COMMIT when we fix replication
      },
      RWType::W};
}

PreparedQuery PrepareAuthQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                               InterpreterContext *interpreter_context) {
  if (in_explicit_transaction) {
    throw UserModificationInMulticommandTxException();
  }

  auto *auth_query = utils::Downcast<AuthQuery>(parsed_query.query);

  auto callback = HandleAuthQuery(auth_query, interpreter_context, parsed_query.parameters);

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [handler = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>(nullptr),
                        interpreter_context](  // NOLINT
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) {
                           // Run the specific query
                           auto results = handler();
                           pull_plan = std::make_shared<PullPlanVector>(std::move(results));
#ifdef MG_ENTERPRISE
                           // Invalidate auth cache after every type of AuthQuery
                           interpreter_context->auth_checker->ClearCache();
#endif
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareReplicationQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                      std::vector<Notification> *notifications, CurrentDB &current_db,
                                      const InterpreterConfig &config) {
  if (in_explicit_transaction) {
    throw ReplicationModificationInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Replication query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw ReplicationDisabledOnDiskStorage();
  }

  auto *replication_query = utils::Downcast<ReplicationQuery>(parsed_query.query);
  auto callback = HandleReplicationQuery(replication_query, parsed_query.parameters, storage, config, notifications);

  return PreparedQuery{callback.header, std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}

PreparedQuery PrepareLockPathQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw LockPathModificationInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Lock Path query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw LockPathDisabledOnDiskStorage();
  }

  auto *lock_path_query = utils::Downcast<LockPathQuery>(parsed_query.query);

  return PreparedQuery{
      {"STATUS"},
      std::move(parsed_query.required_privileges),
      [storage, action = lock_path_query->action_](AnyStream *stream,
                                                   std::optional<int> n) -> std::optional<QueryHandlerResult> {
        auto *mem_storage = static_cast<storage::InMemoryStorage *>(storage);
        std::vector<std::vector<TypedValue>> status;
        std::string res;

        switch (action) {
          case LockPathQuery::Action::LOCK_PATH: {
            const auto lock_success = mem_storage->LockPath();
            if (lock_success.HasError()) [[unlikely]] {
              throw QueryRuntimeException("Failed to lock the data directory");
            }
            res = lock_success.GetValue() ? "Data directory is now locked." : "Data directory is already locked.";
            break;
          }
          case LockPathQuery::Action::UNLOCK_PATH: {
            const auto unlock_success = mem_storage->UnlockPath();
            if (unlock_success.HasError()) [[unlikely]] {
              throw QueryRuntimeException("Failed to unlock the data directory");
            }
            res = unlock_success.GetValue() ? "Data directory is now unlocked." : "Data directory is already unlocked.";
            break;
          }
          case LockPathQuery::Action::STATUS: {
            const auto locked_status = mem_storage->IsPathLocked();
            if (locked_status.HasError()) [[unlikely]] {
              throw QueryRuntimeException("Failed to access the data directory");
            }
            res = locked_status.GetValue() ? "Data directory is locked." : "Data directory is unlocked.";
            break;
          }
        }

        status.emplace_back(std::vector<TypedValue>{TypedValue(res)});
        auto pull_plan = std::make_shared<PullPlanVector>(std::move(status));
        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      },
      RWType::NONE};
}

PreparedQuery PrepareFreeMemoryQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw FreeMemoryModificationInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Free Memory query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw FreeMemoryDisabledOnDiskStorage();
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [storage](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         storage->FreeMemory();
                         memory::PurgeUnusedMemory();
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareShowConfigQuery(ParsedQuery parsed_query, bool in_explicit_transaction) {
  if (in_explicit_transaction) {
    throw ShowConfigModificationInMulticommandTxException();
  }

  auto callback = HandleConfigQuery();

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) [[unlikely]] {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

TriggerEventType ToTriggerEventType(const TriggerQuery::EventType event_type) {
  switch (event_type) {
    case TriggerQuery::EventType::ANY:
      return TriggerEventType::ANY;

    case TriggerQuery::EventType::CREATE:
      return TriggerEventType::CREATE;

    case TriggerQuery::EventType::VERTEX_CREATE:
      return TriggerEventType::VERTEX_CREATE;

    case TriggerQuery::EventType::EDGE_CREATE:
      return TriggerEventType::EDGE_CREATE;

    case TriggerQuery::EventType::DELETE:
      return TriggerEventType::DELETE;

    case TriggerQuery::EventType::VERTEX_DELETE:
      return TriggerEventType::VERTEX_DELETE;

    case TriggerQuery::EventType::EDGE_DELETE:
      return TriggerEventType::EDGE_DELETE;

    case TriggerQuery::EventType::UPDATE:
      return TriggerEventType::UPDATE;

    case TriggerQuery::EventType::VERTEX_UPDATE:
      return TriggerEventType::VERTEX_UPDATE;

    case TriggerQuery::EventType::EDGE_UPDATE:
      return TriggerEventType::EDGE_UPDATE;
  }
}

Callback CreateTrigger(TriggerQuery *trigger_query,
                       const std::map<std::string, storage::PropertyValue> &user_parameters,
                       TriggerStore *trigger_store, InterpreterContext *interpreter_context, DbAccessor *dba,
                       std::optional<std::string> owner) {
  return {{},
          [trigger_name = std::move(trigger_query->trigger_name_),
           trigger_statement = std::move(trigger_query->statement_), event_type = trigger_query->event_type_,
           before_commit = trigger_query->before_commit_, trigger_store, interpreter_context, dba, user_parameters,
           owner = std::move(owner)]() mutable -> std::vector<std::vector<TypedValue>> {
            trigger_store->AddTrigger(std::move(trigger_name), trigger_statement, user_parameters,
                                      ToTriggerEventType(event_type),
                                      before_commit ? TriggerPhase::BEFORE_COMMIT : TriggerPhase::AFTER_COMMIT,
                                      &interpreter_context->ast_cache, dba, interpreter_context->config.query,
                                      std::move(owner), interpreter_context->auth_checker);
            memgraph::metrics::IncrementCounter(memgraph::metrics::TriggersCreated);
            return {};
          }};
}

Callback DropTrigger(TriggerQuery *trigger_query, TriggerStore *trigger_store) {
  return {{},
          [trigger_name = std::move(trigger_query->trigger_name_),
           trigger_store]() -> std::vector<std::vector<TypedValue>> {
            trigger_store->DropTrigger(trigger_name);
            return {};
          }};
}

Callback ShowTriggers(TriggerStore *trigger_store) {
  return {{"trigger name", "statement", "event type", "phase", "owner"}, [trigger_store] {
            std::vector<std::vector<TypedValue>> results;
            auto trigger_infos = trigger_store->GetTriggerInfo();
            results.reserve(trigger_infos.size());
            for (auto &trigger_info : trigger_infos) {
              std::vector<TypedValue> typed_trigger_info;
              typed_trigger_info.reserve(4);
              typed_trigger_info.emplace_back(std::move(trigger_info.name));
              typed_trigger_info.emplace_back(std::move(trigger_info.statement));
              typed_trigger_info.emplace_back(TriggerEventTypeToString(trigger_info.event_type));
              typed_trigger_info.emplace_back(trigger_info.phase == TriggerPhase::BEFORE_COMMIT ? "BEFORE COMMIT"
                                                                                                : "AFTER COMMIT");
              typed_trigger_info.emplace_back(trigger_info.owner.has_value() ? TypedValue{*trigger_info.owner}
                                                                             : TypedValue{});

              results.push_back(std::move(typed_trigger_info));
            }

            return results;
          }};
}

PreparedQuery PrepareTriggerQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                  std::vector<Notification> *notifications, CurrentDB &current_db,
                                  InterpreterContext *interpreter_context,
                                  const std::map<std::string, storage::PropertyValue> &user_parameters,
                                  std::optional<std::string> const &username) {
  if (in_explicit_transaction) {
    throw TriggerModificationInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Trigger query expects a current DB");
  TriggerStore *trigger_store = current_db.db_acc_->get()->trigger_store();
  MG_ASSERT(current_db.execution_db_accessor_, "Trigger query expects a current DB transaction");
  DbAccessor *dba = &*current_db.execution_db_accessor_;

  auto *trigger_query = utils::Downcast<TriggerQuery>(parsed_query.query);
  MG_ASSERT(trigger_query);

  std::optional<Notification> trigger_notification;
  auto callback = std::invoke([trigger_query, trigger_store, interpreter_context, dba, &user_parameters,
                               owner = username, &trigger_notification]() mutable {
    switch (trigger_query->action_) {
      case TriggerQuery::Action::CREATE_TRIGGER:
        trigger_notification.emplace(SeverityLevel::INFO, NotificationCode::CREATE_TRIGGER,
                                     fmt::format("Created trigger {}.", trigger_query->trigger_name_));
        return CreateTrigger(trigger_query, user_parameters, trigger_store, interpreter_context, dba, std::move(owner));
      case TriggerQuery::Action::DROP_TRIGGER:
        trigger_notification.emplace(SeverityLevel::INFO, NotificationCode::DROP_TRIGGER,
                                     fmt::format("Dropped trigger {}.", trigger_query->trigger_name_));
        return DropTrigger(trigger_query, trigger_store);
      case TriggerQuery::Action::SHOW_TRIGGERS:
        return ShowTriggers(trigger_store);
    }
  });

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr},
                        trigger_notification = std::move(trigger_notification), notifications](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           if (trigger_notification) {
                             notifications->push_back(std::move(*trigger_notification));
                           }
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}

PreparedQuery PrepareStreamQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                 std::vector<Notification> *notifications, CurrentDB &current_db,
                                 InterpreterContext *interpreter_context, const std::optional<std::string> &username) {
  if (in_explicit_transaction) {
    throw StreamQueryInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Stream query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  auto *stream_query = utils::Downcast<StreamQuery>(parsed_query.query);
  MG_ASSERT(stream_query);
  auto callback =
      HandleStreamQuery(stream_query, parsed_query.parameters, db_acc, interpreter_context, username, notifications);

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}

constexpr auto ToStorageIsolationLevel(const IsolationLevelQuery::IsolationLevel isolation_level) noexcept {
  switch (isolation_level) {
    case IsolationLevelQuery::IsolationLevel::SNAPSHOT_ISOLATION:
      return storage::IsolationLevel::SNAPSHOT_ISOLATION;
    case IsolationLevelQuery::IsolationLevel::READ_COMMITTED:
      return storage::IsolationLevel::READ_COMMITTED;
    case IsolationLevelQuery::IsolationLevel::READ_UNCOMMITTED:
      return storage::IsolationLevel::READ_UNCOMMITTED;
  }
}

constexpr auto ToStorageMode(const StorageModeQuery::StorageMode storage_mode) noexcept {
  switch (storage_mode) {
    case StorageModeQuery::StorageMode::IN_MEMORY_TRANSACTIONAL:
      return storage::StorageMode::IN_MEMORY_TRANSACTIONAL;
    case StorageModeQuery::StorageMode::IN_MEMORY_ANALYTICAL:
      return storage::StorageMode::IN_MEMORY_ANALYTICAL;
    case StorageModeQuery::StorageMode::ON_DISK_TRANSACTIONAL:
      return storage::StorageMode::ON_DISK_TRANSACTIONAL;
  }
}

constexpr auto ToEdgeImportMode(const EdgeImportModeQuery::Status status) noexcept {
  if (status == EdgeImportModeQuery::Status::ACTIVE) {
    return storage::EdgeImportMode::ACTIVE;
  }
  return storage::EdgeImportMode::INACTIVE;
}

bool SwitchingFromInMemoryToDisk(storage::StorageMode current_mode, storage::StorageMode next_mode) {
  return (current_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL ||
          current_mode == storage::StorageMode::IN_MEMORY_ANALYTICAL) &&
         next_mode == storage::StorageMode::ON_DISK_TRANSACTIONAL;
}

bool SwitchingFromDiskToInMemory(storage::StorageMode current_mode, storage::StorageMode next_mode) {
  return current_mode == storage::StorageMode::ON_DISK_TRANSACTIONAL &&
         (next_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL ||
          next_mode == storage::StorageMode::IN_MEMORY_ANALYTICAL);
}

PreparedQuery PrepareIsolationLevelQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                         CurrentDB &current_db, Interpreter *interpreter) {
  if (in_explicit_transaction) {
    throw IsolationLevelModificationInMulticommandTxException();
  }

  auto *isolation_level_query = utils::Downcast<IsolationLevelQuery>(parsed_query.query);
  MG_ASSERT(isolation_level_query);

  const auto isolation_level = ToStorageIsolationLevel(isolation_level_query->isolation_level_);

  std::function<void()> callback;
  switch (isolation_level_query->isolation_level_scope_) {
    case IsolationLevelQuery::IsolationLevelScope::GLOBAL:  // TODO:.....not GLOBAL is it?!
    {
      MG_ASSERT(current_db.db_acc_, "Storage Isolation Level query expects a current DB");
      storage::Storage *storage = current_db.db_acc_->get()->storage();
      callback = [storage, isolation_level] {
        if (auto maybe_error = storage->SetIsolationLevel(isolation_level); maybe_error.HasError()) {
          switch (maybe_error.GetError()) {
            case storage::Storage::SetIsolationLevelError::DisabledForAnalyticalMode:
              throw IsolationLevelModificationInAnalyticsException();
              break;
          }
        }
      };
      break;
    }

    case IsolationLevelQuery::IsolationLevelScope::SESSION: {
      callback = [interpreter, isolation_level] { interpreter->SetSessionIsolationLevel(isolation_level); };
      break;
    }
    case IsolationLevelQuery::IsolationLevelScope::NEXT: {
      callback = [interpreter, isolation_level] { interpreter->SetNextTransactionIsolationLevel(isolation_level); };
      break;
    }
  }

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [callback = std::move(callback)](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
        callback();
        return QueryHandlerResult::COMMIT;
      },
      RWType::NONE};
}

Callback SwitchMemoryDevice(storage::StorageMode current_mode, storage::StorageMode requested_mode,
                            memgraph::dbms::DatabaseAccess &db) {
  Callback callback;
  callback.fn = [current_mode, requested_mode, &db]() mutable {
    if (current_mode == requested_mode) {
      return std::vector<std::vector<TypedValue>>();
    }
    if (SwitchingFromDiskToInMemory(current_mode, requested_mode)) {
      throw utils::BasicException(
          "You cannot switch from the on-disk storage mode to an in-memory storage mode while the database is running. "
          "To make the switch, delete the data directory and restart the database. Once restarted, Memgraph will "
          "automatically start in the default in-memory transactional storage mode.");
    }
    if (SwitchingFromInMemoryToDisk(current_mode, requested_mode)) {
      if (!db.try_exclusively([](auto &in) {
            if (!in.streams()->GetStreamInfo().empty()) {
              throw utils::BasicException(
                  "You cannot switch from an in-memory storage mode to the on-disk storage mode when there are "
                  "associated streams. Drop all streams and retry.");
            }

            if (!in.trigger_store()->GetTriggerInfo().empty()) {
              throw utils::BasicException(
                  "You cannot switch from an in-memory storage mode to the on-disk storage mode when there are "
                  "associated triggers. Drop all triggers and retry.");
            }

            std::unique_lock main_guard{in.storage()->main_lock_};  // do we need this?
            if (auto vertex_cnt_approx = in.storage()->GetInfo().vertex_count; vertex_cnt_approx > 0) {
              throw utils::BasicException(
                  "You cannot switch from an in-memory storage mode to the on-disk storage mode when the database "
                  "contains data. Delete all entries from the database, run FREE MEMORY and then repeat this "
                  "query. ");
            }
            main_guard.unlock();
            in.SwitchToOnDisk();
          })) {  // Try exclusively failed
        throw utils::BasicException(
            "You cannot switch from an in-memory storage mode to the on-disk storage mode when there are "
            "multiple sessions active. Close all other sessions and try again. As Memgraph Lab uses "
            "multiple sessions to run queries in parallel, "
            "it is currently impossible to switch to the on-disk storage mode within Lab. "
            "Close it, connect to the instance with mgconsole "
            "and change the storage mode to on-disk from there. Then, you can reconnect with the Lab "
            "and continue to use the instance as usual.");
      }
    }
    return std::vector<std::vector<TypedValue>>();
  };
  return callback;
}

bool ActiveTransactionsExist(InterpreterContext *interpreter_context) {
  bool exists_active_transaction = interpreter_context->interpreters.WithLock([](const auto &interpreters_) {
    return std::any_of(interpreters_.begin(), interpreters_.end(), [](const auto &interpreter) {
      return interpreter->transaction_status_.load() != TransactionStatus::IDLE;
    });
  });
  return exists_active_transaction;
}

PreparedQuery PrepareStorageModeQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                      CurrentDB &current_db, InterpreterContext *interpreter_context) {
  if (in_explicit_transaction) {
    throw StorageModeModificationInMulticommandTxException();
  }
  MG_ASSERT(current_db.db_acc_, "Storage Mode query expects a current DB");
  memgraph::dbms::DatabaseAccess &db_acc = *current_db.db_acc_;

  auto *storage_mode_query = utils::Downcast<StorageModeQuery>(parsed_query.query);
  MG_ASSERT(storage_mode_query);
  const auto requested_mode = ToStorageMode(storage_mode_query->storage_mode_);
  auto current_mode = db_acc->GetStorageMode();

  std::function<void()> callback;

  if (current_mode == storage::StorageMode::ON_DISK_TRANSACTIONAL ||
      requested_mode == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    callback = SwitchMemoryDevice(current_mode, requested_mode, db_acc).fn;
  } else {
    // TODO: this needs to be filtered to just db_acc->storage()
    if (ActiveTransactionsExist(interpreter_context)) {
      spdlog::info(
          "Storage mode will be modified when there are no other active transactions. Check the status of the "
          "transactions using 'SHOW TRANSACTIONS' query and ensure no other transactions are active.");
    }

    callback = [requested_mode, storage = db_acc->storage()]() -> std::function<void()> {
      // SetStorageMode will probably be handled at the Database level
      return [storage, requested_mode] { storage->SetStorageMode(requested_mode); };
    }();
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [callback = std::move(callback)](AnyStream * /*stream*/,
                                                        std::optional<int> /*n*/) -> std::optional<QueryHandlerResult> {
                         callback();
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareEdgeImportModeQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
  MG_ASSERT(current_db.db_acc_, "Edge Import query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() != storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw EdgeImportModeQueryDisabledOnDiskStorage();
  }

  auto *edge_import_mode_query = utils::Downcast<EdgeImportModeQuery>(parsed_query.query);
  MG_ASSERT(edge_import_mode_query);
  const auto requested_status = ToEdgeImportMode(edge_import_mode_query->status_);

  auto callback = [requested_status, storage]() -> std::function<void()> {
    return [storage, requested_status] {
      auto *disk_storage = static_cast<storage::DiskStorage *>(storage);
      disk_storage->SetEdgeImportMode(requested_status);
    };
  }();

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [callback = std::move(callback)](AnyStream * /*stream*/,
                                                        std::optional<int> /*n*/) -> std::optional<QueryHandlerResult> {
                         callback();
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareCreateSnapshotQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                         CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw CreateSnapshotInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Create Snapshot query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw CreateSnapshotDisabledOnDiskStorage();
  }

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [storage](AnyStream * /*stream*/, std::optional<int> /*n*/) -> std::optional<QueryHandlerResult> {
        auto *mem_storage = static_cast<storage::InMemoryStorage *>(storage);
        if (auto maybe_error = mem_storage->CreateSnapshot(storage->replication_storage_state_.repl_state_, {});
            maybe_error.HasError()) {
          switch (maybe_error.GetError()) {
            case storage::InMemoryStorage::CreateSnapshotError::DisabledForReplica:
              throw utils::BasicException(
                  "Failed to create a snapshot. Replica instances are not allowed to create them.");
            case storage::InMemoryStorage::CreateSnapshotError::DisabledForAnalyticsPeriodicCommit:
              spdlog::warn(utils::MessageWithLink("Periodic snapshots are disabled for analytical mode.",
                                                  "https://memgr.ph/replication"));
              break;
            case storage::InMemoryStorage::CreateSnapshotError::ReachedMaxNumTries:
              spdlog::warn("Failed to create snapshot. Reached max number of tries. Please contact support");
              break;
          }
        }
        return QueryHandlerResult::COMMIT;
      },
      RWType::NONE};
}

PreparedQuery PrepareSettingQuery(ParsedQuery parsed_query, bool in_explicit_transaction) {
  if (in_explicit_transaction) {
    throw SettingConfigInMulticommandTxException{};
  }

  auto *setting_query = utils::Downcast<SettingQuery>(parsed_query.query);
  MG_ASSERT(setting_query);
  auto callback = HandleSettingQuery(setting_query, parsed_query.parameters);

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}

template <typename Func>
auto ShowTransactions(const std::unordered_set<Interpreter *> &interpreters, const std::optional<std::string> &username,
                      Func &&privilege_checker) -> std::vector<std::vector<TypedValue>> {
  std::vector<std::vector<TypedValue>> results;
  results.reserve(interpreters.size());
  for (Interpreter *interpreter : interpreters) {
    TransactionStatus alive_status = TransactionStatus::ACTIVE;
    // if it is just checking status, commit and abort should wait for the end of the check
    // ignore interpreters that already started committing or rollback
    if (!interpreter->transaction_status_.compare_exchange_strong(alive_status, TransactionStatus::VERIFYING)) {
      continue;
    }
    utils::OnScopeExit clean_status([interpreter]() {
      interpreter->transaction_status_.store(TransactionStatus::ACTIVE, std::memory_order_release);
    });
    std::optional<uint64_t> transaction_id = interpreter->GetTransactionId();

    auto get_interpreter_db_name = [&]() -> std::string const & {
      static std::string all;
      return interpreter->current_db_.db_acc_ ? interpreter->current_db_.db_acc_->get()->id() : all;
    };
    if (transaction_id.has_value() &&
        (interpreter->username_ == username || privilege_checker(get_interpreter_db_name()))) {
      const auto &typed_queries = interpreter->GetQueries();
      results.push_back({TypedValue(interpreter->username_.value_or("")),
                         TypedValue(std::to_string(transaction_id.value())), TypedValue(typed_queries)});
      // Handle user-defined metadata
      std::map<std::string, TypedValue> metadata_tv;
      if (interpreter->metadata_) {
        for (const auto &md : *(interpreter->metadata_)) {
          metadata_tv.emplace(md.first, TypedValue(md.second));
        }
      }
      results.back().push_back(TypedValue(metadata_tv));
    }
  }
  return results;
}

Callback HandleTransactionQueueQuery(TransactionQueueQuery *transaction_query,
                                     const std::optional<std::string> &username, const Parameters &parameters,
                                     InterpreterContext *interpreter_context) {
  auto privilege_checker = [username, auth_checker = interpreter_context->auth_checker](std::string const &db_name) {
    return auth_checker->IsUserAuthorized(username, {query::AuthQuery::Privilege::TRANSACTION_MANAGEMENT}, db_name);
  };

  Callback callback;
  switch (transaction_query->action_) {
    case TransactionQueueQuery::Action::SHOW_TRANSACTIONS: {
      auto show_transactions = [username, privilege_checker = std::move(privilege_checker)](const auto &interpreters) {
        return ShowTransactions(interpreters, username, privilege_checker);
      };
      callback.header = {"username", "transaction_id", "query", "metadata"};
      callback.fn = [interpreter_context, show_transactions = std::move(show_transactions)] {
        // Multiple simultaneous SHOW TRANSACTIONS aren't allowed
        return interpreter_context->interpreters.WithLock(show_transactions);
      };
      break;
    }
    case TransactionQueueQuery::Action::TERMINATE_TRANSACTIONS: {
      auto evaluation_context = EvaluationContext{.timestamp = QueryTimestamp(), .parameters = parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
      std::vector<std::string> maybe_kill_transaction_ids;
      std::transform(transaction_query->transaction_id_list_.begin(), transaction_query->transaction_id_list_.end(),
                     std::back_inserter(maybe_kill_transaction_ids), [&evaluator](Expression *expression) {
                       return std::string(expression->Accept(evaluator).ValueString());
                     });
      callback.header = {"transaction_id", "killed"};
      callback.fn = [interpreter_context, maybe_kill_transaction_ids = std::move(maybe_kill_transaction_ids), username,
                     privilege_checker = std::move(privilege_checker)]() mutable {
        return interpreter_context->TerminateTransactions(std::move(maybe_kill_transaction_ids), username,
                                                          std::move(privilege_checker));
      };
      break;
    }
  }

  return callback;
}

PreparedQuery PrepareTransactionQueueQuery(ParsedQuery parsed_query, const std::optional<std::string> &username,
                                           InterpreterContext *interpreter_context) {
  auto *transaction_queue_query = utils::Downcast<TransactionQueueQuery>(parsed_query.query);
  MG_ASSERT(transaction_queue_query);
  auto callback =
      HandleTransactionQueueQuery(transaction_queue_query, username, parsed_query.parameters, interpreter_context);

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareVersionQuery(ParsedQuery parsed_query, bool in_explicit_transaction) {
  if (in_explicit_transaction) {
    throw VersionInfoInMulticommandTxException();
  }

  return PreparedQuery{{"version"},
                       std::move(parsed_query.required_privileges),
                       [](AnyStream *stream, std::optional<int> /*n*/) {
                         std::vector<TypedValue> version_value;
                         version_value.reserve(1);

                         version_value.emplace_back(gflags::VersionString());
                         stream->Result(version_value);
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareDatabaseInfoQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw InfoInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Database info query expects a current DB");
  MG_ASSERT(current_db.db_transactional_accessor_, "Database ifo query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *info_query = utils::Downcast<DatabaseInfoQuery>(parsed_query.query);
  std::vector<std::string> header;
  std::function<std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()> handler;

  switch (info_query->info_type_) {
    case DatabaseInfoQuery::InfoType::INDEX: {
      header = {"index type", "label", "property"};
      handler = [storage = current_db.db_acc_->get()->storage(), dba] {
        const std::string_view label_index_mark{"label"};
        const std::string_view label_property_index_mark{"label+property"};
        auto info = dba->ListAllIndices();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.label.size() + info.label_property.size());
        for (const auto &item : info.label) {
          results.push_back({TypedValue(label_index_mark), TypedValue(storage->LabelToName(item)), TypedValue()});
        }
        for (const auto &item : info.label_property) {
          results.push_back({TypedValue(label_property_index_mark), TypedValue(storage->LabelToName(item.first)),
                             TypedValue(storage->PropertyToName(item.second))});
        }

        std::sort(results.begin(), results.end(), [&label_index_mark](const auto &record_1, const auto &record_2) {
          const auto type_1 = record_1[0].ValueString();
          const auto type_2 = record_2[0].ValueString();

          if (type_1 != type_2) {
            return type_1 < type_2;
          }

          const auto label_1 = record_1[1].ValueString();
          const auto label_2 = record_2[1].ValueString();
          if (type_1 == label_index_mark || label_1 != label_2) {
            return label_1 < label_2;
          }

          return record_1[2].ValueString() < record_2[2].ValueString();
        });

        return std::pair{results, QueryHandlerResult::COMMIT};
      };
      break;
    }
    case DatabaseInfoQuery::InfoType::CONSTRAINT: {
      header = {"constraint type", "label", "properties"};
      handler = [storage = current_db.db_acc_->get()->storage(), dba] {
        auto info = dba->ListAllConstraints();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.existence.size() + info.unique.size());
        for (const auto &item : info.existence) {
          results.push_back({TypedValue("exists"), TypedValue(storage->LabelToName(item.first)),
                             TypedValue(storage->PropertyToName(item.second))});
        }
        for (const auto &item : info.unique) {
          std::vector<TypedValue> properties;
          properties.reserve(item.second.size());
          for (const auto &property : item.second) {
            properties.emplace_back(storage->PropertyToName(property));
          }
          results.push_back(
              {TypedValue("unique"), TypedValue(storage->LabelToName(item.first)), TypedValue(std::move(properties))});
        }
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
      break;
    }
  }

  return PreparedQuery{std::move(header), std::move(parsed_query.required_privileges),
                       [handler = std::move(handler), action = QueryHandlerResult::NOTHING,
                        pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) {
                           auto [results, action_on_complete] = handler();
                           action = action_on_complete;
                           pull_plan = std::make_shared<PullPlanVector>(std::move(results));
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return action;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareSystemInfoQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db,
                                     std::optional<storage::IsolationLevel> interpreter_isolation_level,
                                     std::optional<storage::IsolationLevel> next_transaction_isolation_level) {
  if (in_explicit_transaction) {
    throw InfoInMulticommandTxException();
  }

  auto *info_query = utils::Downcast<SystemInfoQuery>(parsed_query.query);
  std::vector<std::string> header;
  std::function<std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()> handler;

  switch (info_query->info_type_) {
    case SystemInfoQuery::InfoType::STORAGE: {
      MG_ASSERT(current_db.db_acc_, "System storage info query expects a current DB");
      header = {"storage info", "value"};
      handler = [storage = current_db.db_acc_->get()->storage(), interpreter_isolation_level,
                 next_transaction_isolation_level] {
        auto info = storage->GetInfo();
        std::vector<std::vector<TypedValue>> results{
            {TypedValue("name"), TypedValue(storage->id())},
            {TypedValue("vertex_count"), TypedValue(static_cast<int64_t>(info.vertex_count))},
            {TypedValue("edge_count"), TypedValue(static_cast<int64_t>(info.edge_count))},
            {TypedValue("average_degree"), TypedValue(info.average_degree)},
            {TypedValue("memory_usage"), TypedValue(static_cast<int64_t>(info.memory_usage))},
            {TypedValue("disk_usage"), TypedValue(static_cast<int64_t>(info.disk_usage))},
            {TypedValue("memory_allocated"), TypedValue(static_cast<int64_t>(utils::total_memory_tracker.Amount()))},
            {TypedValue("allocation_limit"), TypedValue(static_cast<int64_t>(utils::total_memory_tracker.HardLimit()))},
            {TypedValue("global_isolation_level"), TypedValue(IsolationLevelToString(storage->GetIsolationLevel()))},
            {TypedValue("session_isolation_level"), TypedValue(IsolationLevelToString(interpreter_isolation_level))},
            {TypedValue("next_session_isolation_level"),
             TypedValue(IsolationLevelToString(next_transaction_isolation_level))},
            {TypedValue("storage_mode"), TypedValue(StorageModeToString(storage->GetStorageMode()))}};
        return std::pair{results, QueryHandlerResult::NOTHING};
      };
    } break;
    case SystemInfoQuery::InfoType::BUILD: {
      header = {"build info", "value"};
      handler = [] {
        std::vector<std::vector<TypedValue>> results{
            {TypedValue("build_type"), TypedValue(utils::GetBuildInfo().build_name)}};
        return std::pair{results, QueryHandlerResult::NOTHING};
      };
    } break;
  }

  return PreparedQuery{std::move(header), std::move(parsed_query.required_privileges),
                       [handler = std::move(handler), action = QueryHandlerResult::NOTHING,
                        pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) {
                           auto [results, action_on_complete] = handler();
                           action = action_on_complete;
                           pull_plan = std::make_shared<PullPlanVector>(std::move(results));
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return action;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareConstraintQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                     std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw ConstraintInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Constraint query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();
  MG_ASSERT(current_db.db_transactional_accessor_, "Constraint query expects a DB transactional access");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *constraint_query = utils::Downcast<ConstraintQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  auto label = storage->NameToLabel(constraint_query->constraint_.label.name);
  std::vector<storage::PropertyId> properties;
  std::vector<std::string> properties_string;
  properties.reserve(constraint_query->constraint_.properties.size());
  properties_string.reserve(constraint_query->constraint_.properties.size());
  for (const auto &prop : constraint_query->constraint_.properties) {
    properties.push_back(storage->NameToProperty(prop.name));
    properties_string.push_back(prop.name);
  }
  auto properties_stringified = utils::Join(properties_string, ", ");

  Notification constraint_notification(SeverityLevel::INFO);
  switch (constraint_query->action_type_) {
    case ConstraintQuery::ActionType::CREATE: {
      constraint_notification.code = NotificationCode::CREATE_CONSTRAINT;

      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS:
          if (properties.empty() || properties.size() > 1) {
            throw SyntaxException("Exactly one property must be used for existence constraints.");
          }
          constraint_notification.title = fmt::format("Created EXISTS constraint on label {} on properties {}.",
                                                      constraint_query->constraint_.label.name, properties_stringified);
          handler = [storage, dba, label, label_name = constraint_query->constraint_.label.name,
                     properties_stringified = std::move(properties_stringified),
                     properties = std::move(properties)](Notification &constraint_notification) {
            auto maybe_constraint_error = dba->CreateExistenceConstraint(label, properties[0]);

            if (maybe_constraint_error.HasError()) {
              const auto &error = maybe_constraint_error.GetError();
              std::visit(
                  [storage, &label_name, &properties_stringified, &constraint_notification]<typename T>(T &&arg) {
                    using ErrorType = std::remove_cvref_t<T>;
                    if constexpr (std::is_same_v<ErrorType, storage::ConstraintViolation>) {
                      auto &violation = arg;
                      MG_ASSERT(violation.properties.size() == 1U);
                      auto property_name = storage->PropertyToName(*violation.properties.begin());
                      throw QueryRuntimeException(
                          "Unable to create existence constraint :{}({}), because an "
                          "existing node violates it.",
                          label_name, property_name);
                    } else if constexpr (std::is_same_v<ErrorType, storage::ConstraintDefinitionError>) {
                      constraint_notification.code = NotificationCode::EXISTENT_CONSTRAINT;
                      constraint_notification.title =
                          fmt::format("Constraint EXISTS on label {} on properties {} already exists.", label_name,
                                      properties_stringified);
                    } else {
                      static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
                    }
                  },
                  error);
            }
          };
          break;
        case Constraint::Type::UNIQUE:
          std::set<storage::PropertyId> property_set;
          for (const auto &property : properties) {
            property_set.insert(property);
          }
          if (property_set.size() != properties.size()) {
            throw SyntaxException("The given set of properties contains duplicates.");
          }
          constraint_notification.title =
              fmt::format("Created UNIQUE constraint on label {} on properties {}.",
                          constraint_query->constraint_.label.name, utils::Join(properties_string, ", "));
          handler = [storage, dba, label, label_name = constraint_query->constraint_.label.name,
                     properties_stringified = std::move(properties_stringified),
                     property_set = std::move(property_set)](Notification &constraint_notification) {
            auto maybe_constraint_error = dba->CreateUniqueConstraint(label, property_set);
            if (maybe_constraint_error.HasError()) {
              const auto &error = maybe_constraint_error.GetError();
              std::visit(
                  [storage, &label_name, &properties_stringified, &constraint_notification]<typename T>(T &&arg) {
                    using ErrorType = std::remove_cvref_t<T>;
                    if constexpr (std::is_same_v<ErrorType, storage::ConstraintViolation>) {
                      auto &violation = arg;
                      auto violation_label_name = storage->LabelToName(violation.label);
                      std::stringstream property_names_stream;
                      utils::PrintIterable(
                          property_names_stream, violation.properties, ", ",
                          [storage](auto &stream, const auto &prop) { stream << storage->PropertyToName(prop); });
                      throw QueryRuntimeException(
                          "Unable to create unique constraint :{}({}), because an "
                          "existing node violates it.",
                          violation_label_name, property_names_stream.str());
                    } else if constexpr (std::is_same_v<ErrorType, storage::ConstraintDefinitionError>) {
                      constraint_notification.code = NotificationCode::EXISTENT_CONSTRAINT;
                      constraint_notification.title =
                          fmt::format("Constraint UNIQUE on label {} and properties {} couldn't be created.",
                                      label_name, properties_stringified);
                    } else {
                      static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
                    }
                  },
                  error);
            }
            switch (maybe_constraint_error.GetValue()) {
              case storage::UniqueConstraints::CreationStatus::EMPTY_PROPERTIES:
                throw SyntaxException(
                    "At least one property must be used for unique "
                    "constraints.");
              case storage::UniqueConstraints::CreationStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED:
                throw SyntaxException(
                    "Too many properties specified. Limit of {} properties "
                    "for unique constraints is exceeded.",
                    storage::kUniqueConstraintsMaxProperties);
              case storage::UniqueConstraints::CreationStatus::ALREADY_EXISTS:
                constraint_notification.code = NotificationCode::EXISTENT_CONSTRAINT;
                constraint_notification.title =
                    fmt::format("Constraint UNIQUE on label {} on properties {} already exists.", label_name,
                                properties_stringified);
                break;
              case storage::UniqueConstraints::CreationStatus::SUCCESS:
                break;
            }
          };
          break;
      }
    } break;
    case ConstraintQuery::ActionType::DROP: {
      constraint_notification.code = NotificationCode::DROP_CONSTRAINT;

      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS:
          if (properties.empty() || properties.size() > 1) {
            throw SyntaxException("Exactly one property must be used for existence constraints.");
          }
          constraint_notification.title =
              fmt::format("Dropped EXISTS constraint on label {} on properties {}.",
                          constraint_query->constraint_.label.name, utils::Join(properties_string, ", "));
          handler = [dba, label, label_name = constraint_query->constraint_.label.name,
                     properties_stringified = std::move(properties_stringified),
                     properties = std::move(properties)](Notification &constraint_notification) {
            auto maybe_constraint_error = dba->DropExistenceConstraint(label, properties[0]);
            if (maybe_constraint_error.HasError()) {
              constraint_notification.code = NotificationCode::NONEXISTENT_CONSTRAINT;
              constraint_notification.title = fmt::format(
                  "Constraint EXISTS on label {} on properties {} doesn't exist.", label_name, properties_stringified);
            }
            return std::vector<std::vector<TypedValue>>();
          };
          break;
        case Constraint::Type::UNIQUE:
          std::set<storage::PropertyId> property_set;
          for (const auto &property : properties) {
            property_set.insert(property);
          }
          if (property_set.size() != properties.size()) {
            throw SyntaxException("The given set of properties contains duplicates.");
          }
          constraint_notification.title =
              fmt::format("Dropped UNIQUE constraint on label {} on properties {}.",
                          constraint_query->constraint_.label.name, utils::Join(properties_string, ", "));
          handler = [dba, label, label_name = constraint_query->constraint_.label.name,
                     properties_stringified = std::move(properties_stringified),
                     property_set = std::move(property_set)](Notification &constraint_notification) {
            auto res = dba->DropUniqueConstraint(label, property_set);
            switch (res) {
              case storage::UniqueConstraints::DeletionStatus::EMPTY_PROPERTIES:
                throw SyntaxException(
                    "At least one property must be used for unique "
                    "constraints.");
                break;
              case storage::UniqueConstraints::DeletionStatus::PROPERTIES_SIZE_LIMIT_EXCEEDED:
                throw SyntaxException(
                    "Too many properties specified. Limit of {} properties for "
                    "unique constraints is exceeded.",
                    storage::kUniqueConstraintsMaxProperties);
                break;
              case storage::UniqueConstraints::DeletionStatus::NOT_FOUND:
                constraint_notification.code = NotificationCode::NONEXISTENT_CONSTRAINT;
                constraint_notification.title =
                    fmt::format("Constraint UNIQUE on label {} on properties {} doesn't exist.", label_name,
                                properties_stringified);
                break;
              case storage::UniqueConstraints::DeletionStatus::SUCCESS:
                break;
            }
            return std::vector<std::vector<TypedValue>>();
          };
      }
    } break;
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [handler = std::move(handler), constraint_notification = std::move(constraint_notification),
                        notifications](AnyStream * /*stream*/, std::optional<int> /*n*/) mutable {
                         handler(constraint_notification);
                         notifications->push_back(constraint_notification);
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
}

PreparedQuery PrepareMultiDatabaseQuery(ParsedQuery parsed_query, CurrentDB &current_db,
                                        InterpreterContext *interpreter_context,
                                        std::optional<std::function<void(std::string_view)>> on_change_cb) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryException("Trying to use enterprise feature without a valid license.");
  }
  // TODO: Remove once replicas support multi-tenant replication
  if (!current_db.db_acc_) throw DatabaseContextRequiredException("Multi database queries require a defined database.");
  if (IsReplica(current_db.db_acc_->get()->storage())) {
    throw QueryException("Query forbidden on the replica!");
  }

  auto *query = utils::Downcast<MultiDatabaseQuery>(parsed_query.query);
  auto *db_handler = interpreter_context->db_handler;

  switch (query->action_) {
    case MultiDatabaseQuery::Action::CREATE:
      return PreparedQuery{
          {"STATUS"},
          std::move(parsed_query.required_privileges),
          [db_name = query->db_name_, db_handler](AnyStream *stream,
                                                  std::optional<int> n) -> std::optional<QueryHandlerResult> {
            std::vector<std::vector<TypedValue>> status;
            std::string res;

            const auto success = db_handler->New(db_name);
            if (success.HasError()) {
              switch (success.GetError()) {
                case dbms::NewError::EXISTS:
                  res = db_name + " already exists.";
                  break;
                case dbms::NewError::DEFUNCT:
                  throw QueryRuntimeException(
                      "{} is defunct and in an unknown state. Try to delete it again or clean up storage and restart "
                      "Memgraph.",
                      db_name);
                case dbms::NewError::GENERIC:
                  throw QueryRuntimeException("Failed while creating {}", db_name);
                case dbms::NewError::NO_CONFIGS:
                  throw QueryRuntimeException("No configuration found while trying to create {}", db_name);
              }
            } else {
              res = "Successfully created database " + db_name;
            }
            status.emplace_back(std::vector<TypedValue>{TypedValue(res)});
            auto pull_plan = std::make_shared<PullPlanVector>(std::move(status));
            if (pull_plan->Pull(stream, n)) {
              return QueryHandlerResult::COMMIT;
            }
            return std::nullopt;
          },
          RWType::W,
          ""  // No target DB possible
      };

    case MultiDatabaseQuery::Action::USE:
      if (current_db.in_explicit_db_) {
        throw QueryException("Database switching is prohibited if session explicitly defines the used database");
      }
      return PreparedQuery{{"STATUS"},
                           std::move(parsed_query.required_privileges),
                           [db_name = query->db_name_, db_handler, &current_db, on_change_cb](
                               AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                             std::vector<std::vector<TypedValue>> status;
                             std::string res;

                             try {
                               if (current_db.db_acc_ && db_name == current_db.db_acc_->get()->id()) {
                                 res = "Already using " + db_name;
                               } else {
                                 auto tmp = db_handler->Get(db_name);
                                 if (on_change_cb) (*on_change_cb)(db_name);  // Will trow if cb fails
                                 current_db.SetCurrentDB(std::move(tmp), false);
                                 res = "Using " + db_name;
                               }
                             } catch (const utils::BasicException &e) {
                               throw QueryRuntimeException(e.what());
                             }

                             status.emplace_back(std::vector<TypedValue>{TypedValue(res)});
                             auto pull_plan = std::make_shared<PullPlanVector>(std::move(status));
                             if (pull_plan->Pull(stream, n)) {
                               return QueryHandlerResult::COMMIT;
                             }
                             return std::nullopt;
                           },
                           RWType::NONE,
                           query->db_name_};

    case MultiDatabaseQuery::Action::DROP:
      return PreparedQuery{
          {"STATUS"},
          std::move(parsed_query.required_privileges),
          [db_name = query->db_name_, db_handler, auth = interpreter_context->auth](
              AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
            std::vector<std::vector<TypedValue>> status;

            memgraph::dbms::DeleteResult success{};

            try {
              // Remove database
              success = db_handler->Delete(db_name);
              if (!success.HasError()) {
                // Remove from auth
                auth->DeleteDatabase(db_name);
              } else {
                switch (success.GetError()) {
                  case dbms::DeleteError::DEFAULT_DB:
                    throw QueryRuntimeException("Cannot delete the default database.");
                  case dbms::DeleteError::NON_EXISTENT:
                    throw QueryRuntimeException("{} does not exist.", db_name);
                  case dbms::DeleteError::USING:
                    throw QueryRuntimeException("Cannot delete {}, it is currently being used.", db_name);
                  case dbms::DeleteError::FAIL:
                    throw QueryRuntimeException("Failed while deleting {}", db_name);
                  case dbms::DeleteError::DISK_FAIL:
                    throw QueryRuntimeException("Failed to clean storage of {}", db_name);
                }
              }
            } catch (const utils::BasicException &e) {
              throw QueryRuntimeException(e.what());
            }

            status.emplace_back(std::vector<TypedValue>{TypedValue("Successfully deleted " + db_name)});
            auto pull_plan = std::make_shared<PullPlanVector>(std::move(status));
            if (pull_plan->Pull(stream, n)) {
              return QueryHandlerResult::COMMIT;
            }
            return std::nullopt;
          },
          RWType::W,
          query->db_name_};
  }
#else
  throw QueryException("Query not supported.");
#endif
}

PreparedQuery PrepareShowDatabasesQuery(ParsedQuery parsed_query, CurrentDB &current_db,
                                        InterpreterContext *interpreter_context,
                                        const std::optional<std::string> &username) {
#ifdef MG_ENTERPRISE

  // TODO: split query into two, Databases (no need for current_db), & Current database (uses current_db)
  MG_ASSERT(current_db.db_acc_, "Show Database Level query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryException("Trying to use enterprise feature without a valid license.");
  }
  // TODO: Remove once replicas support multi-tenant replication
  auto &replState = storage->replication_storage_state_.repl_state_;
  if (replState.IsReplica()) {
    throw QueryException("SHOW DATABASES forbidden on the replica!");
  }

  // TODO pick directly from ic
  auto *db_handler = interpreter_context->db_handler;
  AuthQueryHandler *auth = interpreter_context->auth;

  Callback callback;
  callback.header = {"Name", "Current"};
  callback.fn = [auth, storage, db_handler, username]() mutable -> std::vector<std::vector<TypedValue>> {
    std::vector<std::vector<TypedValue>> status;
    const auto &in_use = storage->id();
    bool found_current = false;

    auto gen_status = [&]<typename T, typename K>(T all, K denied) {
      Sort(all);
      Sort(denied);

      status.reserve(all.size());
      for (const auto &name : all) {
        TypedValue use("");
        if (!found_current && Same(name, in_use)) {
          use = TypedValue("*");
          found_current = true;
        }
        status.push_back({TypedValue(name), std::move(use)});
      }

      // No denied databases (no need to filter them out)
      if (denied.empty()) return;

      auto denied_itr = denied.begin();
      auto iter = std::remove_if(status.begin(), status.end(), [&denied_itr, &denied](auto &in) -> bool {
        while (denied_itr != denied.end() && denied_itr->ValueString() < in[0].ValueString()) ++denied_itr;
        return (denied_itr != denied.end() && denied_itr->ValueString() == in[0].ValueString());
      });
      status.erase(iter, status.end());
    };

    if (!username) {
      // No user, return all
      gen_status(db_handler->All(), std::vector<TypedValue>{});
    } else {
      // User has a subset of accessible dbs; this is synched with the SessionContextHandler
      const auto &db_priv = auth->GetDatabasePrivileges(*username);
      const auto &allowed = db_priv[0][0];
      const auto &denied = db_priv[0][1].ValueList();
      if (allowed.IsString() && allowed.ValueString() == auth::kAllDatabases) {
        // All databases are allowed
        gen_status(db_handler->All(), denied);
      } else {
        gen_status(allowed.ValueList(), denied);
      }
    }

    if (!found_current) throw QueryRuntimeException("Missing current database!");
    return status;
  };

  return PreparedQuery{
      std::move(callback.header), std::move(parsed_query.required_privileges),
      [handler = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
          AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        if (!pull_plan) {
          auto results = handler();
          pull_plan = std::make_shared<PullPlanVector>(std::move(results));
        }

        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::NOTHING;
        }
        return std::nullopt;
      },
      RWType::NONE,
      ""  // No target DB
  };
#else
  throw QueryException("Query not supported.");
#endif
}

std::optional<uint64_t> Interpreter::GetTransactionId() const { return current_transaction_; }

void Interpreter::BeginTransaction(QueryExtras const &extras) {
  const auto prepared_query = PrepareTransactionQuery("BEGIN", extras);
  prepared_query.query_handler(nullptr, {});
}

void Interpreter::CommitTransaction() {
  const auto prepared_query = PrepareTransactionQuery("COMMIT");
  prepared_query.query_handler(nullptr, {});
  query_executions_.clear();
  transaction_queries_->clear();
}

void Interpreter::RollbackTransaction() {
  const auto prepared_query = PrepareTransactionQuery("ROLLBACK");
  prepared_query.query_handler(nullptr, {});
  query_executions_.clear();
  transaction_queries_->clear();
}

#if MG_ENTERPRISE
// Before Prepare or during Prepare, but single-threaded.
// TODO: Is there any cleanup?
void Interpreter::SetCurrentDB(std::string_view db_name, bool in_explicit_db) {
  // Can throw
  // do we lock here?
  current_db_.SetCurrentDB(interpreter_context_->db_handler->Get(db_name), in_explicit_db);
}
#endif

Interpreter::PrepareResult Interpreter::Prepare(const std::string &query_string,
                                                const std::map<std::string, storage::PropertyValue> &params,
                                                QueryExtras const &extras) {
  // TODO: Remove once the interpreter is storage/tx independent and could run without an associated database
  if (!current_db_.db_acc_) {
    throw DatabaseContextRequiredException("Database required for the query.");
  }

  // Handle transaction control queries.
  const auto upper_case_query = utils::ToUpperCase(query_string);
  const auto trimmed_query = utils::Trim(upper_case_query);
  if (trimmed_query == "BEGIN" || trimmed_query == "COMMIT" || trimmed_query == "ROLLBACK") {
    auto resource = utils::MonotonicBufferResource(kExecutionMemoryBlockSize);
    auto prepared_query = PrepareTransactionQuery(trimmed_query, extras);
    auto &query_execution =
        query_executions_.emplace_back(QueryExecution::Create(std::move(resource), std::move(prepared_query)));
    std::optional<int> qid =
        in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};
    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid, {}};
  }

  if (!in_explicit_transaction_) {
    transaction_queries_->clear();
  }
  // Don't save BEGIN, COMMIT or ROLLBACK
  transaction_queries_->push_back(query_string);

  // All queries other than transaction control queries advance the command in
  // an explicit transaction block.
  if (in_explicit_transaction_) {
    AdvanceCommand();
  } else {
    query_executions_.clear();
    if (current_db_.db_transactional_accessor_ /* && !in_explicit_transaction_*/) {
      // If we're not in an explicit transaction block and we have an open
      // transaction, abort it since we're about to prepare a new query.
      AbortCommand(nullptr);
    }

    SetupInterpreterTransaction(extras);
  }

  std::unique_ptr<QueryExecution> *query_execution_ptr = nullptr;
  try {
    utils::Timer parsing_timer;
    ParsedQuery parsed_query =
        ParseQuery(query_string, params, &interpreter_context_->ast_cache, interpreter_context_->config.query);
    auto parsing_time = parsing_timer.Elapsed().count();

    CypherQuery const *const cypher_query = [&]() -> CypherQuery * {
      if (auto *cypher_query = utils::Downcast<CypherQuery>(parsed_query.query)) {
        return cypher_query;
      }
      if (auto *profile_query = utils::Downcast<ProfileQuery>(parsed_query.query)) {
        return profile_query->cypher_query_;
      }
      return nullptr;
    }();  // IILE

    auto const [usePool, hasAllShortestPaths] = [&]() -> std::pair<bool, bool> {
      if (!cypher_query) {
        return {false, false};
      }
      auto const &clauses = cypher_query->single_query_->clauses_;
      bool hasAllShortestPaths = IsAllShortestPathsQuery(clauses);
      // Using PoolResource without MonotonicMemoryResouce for LOAD CSV reduces memory usage.
      bool usePool = hasAllShortestPaths || IsCallBatchedProcedureQuery(clauses) || IsLoadCsvQuery(clauses);
      return {usePool, hasAllShortestPaths};
    }();  // IILE

    // Setup QueryExecution
    // its MemoryResource is mostly used for allocations done on Frame and storing `row`s
    if (usePool) {
      spdlog::trace("PrepareCypher has {} encountered all shortest paths, QueryExecution will use PoolResource",
                    hasAllShortestPaths ? "" : "not");
      query_executions_.emplace_back(QueryExecution::Create(utils::PoolResource(128, kExecutionPoolMaxBlockSize)));
    } else {
      query_executions_.emplace_back(QueryExecution::Create(utils::MonotonicBufferResource(kExecutionMemoryBlockSize)));
    }

    auto &query_execution = query_executions_.back();
    query_execution_ptr = &query_execution;

    std::optional<int> qid =
        in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};

    query_execution->summary["parsing_time"] = parsing_time;

    // Set a default cost estimate of 0. Individual queries can overwrite this
    // field with an improved estimate.
    query_execution->summary["cost_estimate"] = 0.0;

    // Some queries require an active transaction in order to be prepared.
    // TODO: make a better analysis visitor over the `parsed_query.query`
    bool requires_db_transaction =
        utils::Downcast<CypherQuery>(parsed_query.query) || utils::Downcast<ExplainQuery>(parsed_query.query) ||
        utils::Downcast<ProfileQuery>(parsed_query.query) || utils::Downcast<DumpQuery>(parsed_query.query) ||
        utils::Downcast<TriggerQuery>(parsed_query.query) || utils::Downcast<AnalyzeGraphQuery>(parsed_query.query) ||
        utils::Downcast<IndexQuery>(parsed_query.query) || utils::Downcast<DatabaseInfoQuery>(parsed_query.query) ||
        utils::Downcast<ConstraintQuery>(parsed_query.query);

    if (!in_explicit_transaction_ && requires_db_transaction) {
      // TODO: ATM only a single database, will change when we have multiple database transactions
      bool could_commit = utils::Downcast<CypherQuery>(parsed_query.query) != nullptr;
      bool unique = utils::Downcast<IndexQuery>(parsed_query.query) != nullptr ||
                    utils::Downcast<ConstraintQuery>(parsed_query.query) != nullptr;
      SetupDatabaseTransaction(could_commit, unique);
    }

    // TODO: none database transaction (assuming mutually exclusive from DB transactions)
    // if (!requires_db_transaction) {
    //   /* something */
    // }

    utils::Timer planning_timer;
    PreparedQuery prepared_query;
    utils::MemoryResource *memory_resource =
        std::visit([](auto &execution_memory) -> utils::MemoryResource * { return &execution_memory; },
                   query_execution->execution_memory);
    frame_change_collector_.reset();
    frame_change_collector_.emplace(memory_resource);
    if (utils::Downcast<CypherQuery>(parsed_query.query)) {
      prepared_query = PrepareCypherQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_,
                                          current_db_, memory_resource, &query_execution->notifications, username_,
                                          &transaction_status_, current_timeout_timer_, &*frame_change_collector_);
    } else if (utils::Downcast<ExplainQuery>(parsed_query.query)) {
      prepared_query =
          PrepareExplainQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_, current_db_);
    } else if (utils::Downcast<ProfileQuery>(parsed_query.query)) {
      prepared_query =
          PrepareProfileQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                              interpreter_context_, current_db_, &query_execution->execution_memory_with_exception,
                              username_, &transaction_status_, current_timeout_timer_, &*frame_change_collector_);
    } else if (utils::Downcast<DumpQuery>(parsed_query.query)) {
      prepared_query = PrepareDumpQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<IndexQuery>(parsed_query.query)) {
      prepared_query = PrepareIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                         &query_execution->notifications, current_db_);
    } else if (utils::Downcast<AnalyzeGraphQuery>(parsed_query.query)) {
      prepared_query = PrepareAnalyzeGraphQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<AuthQuery>(parsed_query.query)) {
      /// SYSTEM (Replication) PURE
      prepared_query = PrepareAuthQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_);
    } else if (utils::Downcast<DatabaseInfoQuery>(parsed_query.query)) {
      prepared_query = PrepareDatabaseInfoQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<SystemInfoQuery>(parsed_query.query)) {
      prepared_query = PrepareSystemInfoQuery(std::move(parsed_query), in_explicit_transaction_, current_db_,
                                              interpreter_isolation_level, next_transaction_isolation_level);
    } else if (utils::Downcast<ConstraintQuery>(parsed_query.query)) {
      prepared_query = PrepareConstraintQuery(std::move(parsed_query), in_explicit_transaction_,
                                              &query_execution->notifications, current_db_);
    } else if (utils::Downcast<ReplicationQuery>(parsed_query.query)) {
      /// TODO: make replication DB agnostic
      prepared_query =
          PrepareReplicationQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                                  current_db_, interpreter_context_->config);
    } else if (utils::Downcast<LockPathQuery>(parsed_query.query)) {
      prepared_query = PrepareLockPathQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<FreeMemoryQuery>(parsed_query.query)) {
      prepared_query = PrepareFreeMemoryQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<ShowConfigQuery>(parsed_query.query)) {
      /// SYSTEM PURE
      prepared_query = PrepareShowConfigQuery(std::move(parsed_query), in_explicit_transaction_);
    } else if (utils::Downcast<TriggerQuery>(parsed_query.query)) {
      prepared_query =
          PrepareTriggerQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                              current_db_, interpreter_context_, params, username_);
    } else if (utils::Downcast<StreamQuery>(parsed_query.query)) {
      prepared_query =
          PrepareStreamQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                             current_db_, interpreter_context_, username_);
    } else if (utils::Downcast<IsolationLevelQuery>(parsed_query.query)) {
      prepared_query = PrepareIsolationLevelQuery(std::move(parsed_query), in_explicit_transaction_, current_db_, this);
    } else if (utils::Downcast<CreateSnapshotQuery>(parsed_query.query)) {
      prepared_query = PrepareCreateSnapshotQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<SettingQuery>(parsed_query.query)) {
      /// SYSTEM PURE
      prepared_query = PrepareSettingQuery(std::move(parsed_query), in_explicit_transaction_);
    } else if (utils::Downcast<VersionQuery>(parsed_query.query)) {
      /// SYSTEM PURE
      prepared_query = PrepareVersionQuery(std::move(parsed_query), in_explicit_transaction_);
    } else if (utils::Downcast<StorageModeQuery>(parsed_query.query)) {
      prepared_query =
          PrepareStorageModeQuery(std::move(parsed_query), in_explicit_transaction_, current_db_, interpreter_context_);
    } else if (utils::Downcast<TransactionQueueQuery>(parsed_query.query)) {
      /// INTERPRETER
      if (in_explicit_transaction_) {
        throw TransactionQueueInMulticommandTxException();
      }
      prepared_query = PrepareTransactionQueueQuery(std::move(parsed_query), username_, interpreter_context_);
    } else if (utils::Downcast<MultiDatabaseQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw MultiDatabaseQueryInMulticommandTxException();
      }
      /// SYSTEM (Replication) + INTERPRETER
      prepared_query =
          PrepareMultiDatabaseQuery(std::move(parsed_query), current_db_, interpreter_context_, on_change_);
    } else if (utils::Downcast<ShowDatabasesQuery>(parsed_query.query)) {
      /// SYSTEM PURE ("SHOW DATABASES")
      /// INTERPRETER (TODO: "SHOW DATABASE")
      prepared_query = PrepareShowDatabasesQuery(std::move(parsed_query), current_db_, interpreter_context_, username_);
    } else if (utils::Downcast<EdgeImportModeQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw EdgeImportModeModificationInMulticommandTxException();
      }
      prepared_query = PrepareEdgeImportModeQuery(std::move(parsed_query), current_db_);
    } else {
      LOG_FATAL("Should not get here -- unknown query type!");
    }

    query_execution->summary["planning_time"] = planning_timer.Elapsed().count();
    query_execution->prepared_query.emplace(std::move(prepared_query));

    const auto rw_type = query_execution->prepared_query->rw_type;
    query_execution->summary["type"] = plan::ReadWriteTypeChecker::TypeToString(rw_type);

    UpdateTypeCount(rw_type);

    if (IsWriteQueryOnMainMemoryReplica(current_db_.db_acc_->get()->storage(), rw_type)) {
      query_execution = nullptr;
      throw QueryException("Write query forbidden on the replica!");
    }

    // Set the target db to the current db (some queries have different target from the current db)
    if (!query_execution->prepared_query->db) {
      query_execution->prepared_query->db = current_db_.db_acc_->get()->id();
    }
    query_execution->summary["db"] = *query_execution->prepared_query->db;

    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid,
            query_execution->prepared_query->db};
  } catch (const utils::BasicException &) {
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedQuery);
    AbortCommand(query_execution_ptr);
    throw;
  }
}
void Interpreter::SetupDatabaseTransaction(bool couldCommit, bool unique) {
  current_db_.SetupDatabaseTransaction(GetIsolationLevelOverride(), couldCommit, unique);
}
void Interpreter::SetupInterpreterTransaction(const QueryExtras &extras) {
  metrics::IncrementCounter(metrics::ActiveTransactions);
  transaction_status_.store(TransactionStatus::ACTIVE, std::memory_order_release);
  current_transaction_ = interpreter_context_->id_handler.next();
  metadata_ = GenOptional(extras.metadata_pv);
  current_timeout_timer_ = CreateTimeoutTimer(extras, interpreter_context_->config);
}

std::vector<TypedValue> Interpreter::GetQueries() {
  auto typed_queries = std::vector<TypedValue>();
  transaction_queries_.WithLock([&typed_queries](const auto &transaction_queries) {
    std::for_each(transaction_queries.begin(), transaction_queries.end(),
                  [&typed_queries](const auto &query) { typed_queries.emplace_back(query); });
  });
  return typed_queries;
}

void Interpreter::Abort() {
  auto expected = TransactionStatus::ACTIVE;
  while (!transaction_status_.compare_exchange_weak(expected, TransactionStatus::STARTED_ROLLBACK)) {
    if (expected == TransactionStatus::TERMINATED || expected == TransactionStatus::IDLE) {
      transaction_status_.store(TransactionStatus::STARTED_ROLLBACK);
      break;
    }
    expected = TransactionStatus::ACTIVE;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  utils::OnScopeExit clean_status(
      [this]() { transaction_status_.store(TransactionStatus::IDLE, std::memory_order_release); });

  expect_rollback_ = false;
  in_explicit_transaction_ = false;
  metadata_ = std::nullopt;
  current_timeout_timer_.reset();
  current_transaction_.reset();

  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveTransactions);

  // if (!current_db_.db_transactional_accessor_) return;
  current_db_.CleanupDBTransaction(true);
  for (auto &qe : query_executions_) {
    if (qe) qe->CleanRuntimeData();
  }
  frame_change_collector_.reset();
}

namespace {
void RunTriggersAfterCommit(dbms::DatabaseAccess db_acc, InterpreterContext *interpreter_context,
                            TriggerContext original_trigger_context,
                            std::atomic<TransactionStatus> *transaction_status) {
  // Run the triggers
  for (const auto &trigger : db_acc->trigger_store()->AfterCommitTriggers().access()) {
    utils::MonotonicBufferResource execution_memory{kExecutionMemoryBlockSize};

    // create a new transaction for each trigger
    auto tx_acc = db_acc->Access();
    DbAccessor db_accessor{tx_acc.get()};

    // On-disk storage removes all Vertex/Edge Accessors because previous trigger tx finished.
    // So we need to adapt TriggerContext based on user transaction which is still alive.
    auto trigger_context = original_trigger_context;
    trigger_context.AdaptForAccessor(&db_accessor);
    try {
      trigger.Execute(&db_accessor, &execution_memory, flags::run_time::execution_timeout_sec_,
                      &interpreter_context->is_shutting_down, transaction_status, trigger_context,
                      interpreter_context->auth_checker);
    } catch (const utils::BasicException &exception) {
      spdlog::warn("Trigger '{}' failed with exception:\n{}", trigger.Name(), exception.what());
      db_accessor.Abort();
      continue;
    }

    auto maybe_commit_error = db_accessor.Commit();
    if (maybe_commit_error.HasError()) {
      const auto &error = maybe_commit_error.GetError();

      std::visit(
          [&trigger, &db_accessor]<typename T>(T &&arg) {
            using ErrorType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<ErrorType, storage::ReplicationError>) {
              spdlog::warn("At least one SYNC replica has not confirmed execution of the trigger '{}'.",
                           trigger.Name());
            } else if constexpr (std::is_same_v<ErrorType, storage::ConstraintViolation>) {
              const auto &constraint_violation = arg;
              switch (constraint_violation.type) {
                case storage::ConstraintViolation::Type::EXISTENCE: {
                  const auto &label_name = db_accessor.LabelToName(constraint_violation.label);
                  MG_ASSERT(constraint_violation.properties.size() == 1U);
                  const auto &property_name = db_accessor.PropertyToName(*constraint_violation.properties.begin());
                  spdlog::warn("Trigger '{}' failed to commit due to existence constraint violation on: {}({}) ",
                               trigger.Name(), label_name, property_name);
                }
                case storage::ConstraintViolation::Type::UNIQUE: {
                  const auto &label_name = db_accessor.LabelToName(constraint_violation.label);
                  std::stringstream property_names_stream;
                  utils::PrintIterable(
                      property_names_stream, constraint_violation.properties, ", ",
                      [&](auto &stream, const auto &prop) { stream << db_accessor.PropertyToName(prop); });
                  spdlog::warn("Trigger '{}' failed to commit due to unique constraint violation on :{}({})",
                               trigger.Name(), label_name, property_names_stream.str());
                }
              }
            } else if constexpr (std::is_same_v<ErrorType, storage::SerializationError>) {
              throw QueryException("Unable to commit due to serialization error.");
            } else if constexpr (std::is_same_v<ErrorType, storage::PersistenceError>) {
              throw QueryException("Unable to commit due to persistance error.");
            } else {
              static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
            }
          },
          error);
    }
  }
}
}  // namespace

void Interpreter::Commit() {
  // It's possible that some queries did not finish because the user did
  // not pull all of the results from the query.
  // For now, we will not check if there are some unfinished queries.
  // We should document clearly that all results should be pulled to complete
  // a query.
  current_transaction_.reset();
  if (!current_db_.db_transactional_accessor_) return;

  // TODO: Better (or removed) check
  if (!current_db_.db_acc_) return;
  auto *db = current_db_.db_acc_->get();

  /*
  At this point we must check that the transaction is alive to start committing. The only other possible state is
  verifying and in that case we must check if the transaction was terminated and if yes abort committing. Exception
  should suffice.
  */
  auto expected = TransactionStatus::ACTIVE;
  while (!transaction_status_.compare_exchange_weak(expected, TransactionStatus::STARTED_COMMITTING)) {
    if (expected == TransactionStatus::TERMINATED) {
      throw memgraph::utils::BasicException(
          "Aborting transaction commit because the transaction was requested to stop from other session. ");
    }
    expected = TransactionStatus::ACTIVE;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Clean transaction status if something went wrong
  utils::OnScopeExit clean_status(
      [this]() { transaction_status_.store(TransactionStatus::IDLE, std::memory_order_release); });

  auto current_storage_mode = db->GetStorageMode();
  auto creation_mode = current_db_.db_transactional_accessor_->GetCreationStorageMode();
  if (creation_mode != storage::StorageMode::ON_DISK_TRANSACTIONAL &&
      current_storage_mode == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw QueryException(
        "Cannot commit transaction because the storage mode has changed from in-memory storage to on-disk storage.");
  }

  utils::OnScopeExit update_metrics([]() {
    memgraph::metrics::IncrementCounter(memgraph::metrics::CommitedTransactions);
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveTransactions);
  });

  std::optional<TriggerContext> trigger_context = std::nullopt;
  if (current_db_.trigger_context_collector_) {
    trigger_context.emplace(std::move(*current_db_.trigger_context_collector_).TransformToTriggerContext());
    current_db_.trigger_context_collector_.reset();
  }

  if (frame_change_collector_) {
    frame_change_collector_.reset();
  }

  if (trigger_context) {
    // Run the triggers
    for (const auto &trigger : db->trigger_store()->BeforeCommitTriggers().access()) {
      utils::MonotonicBufferResource execution_memory{kExecutionMemoryBlockSize};
      AdvanceCommand();
      try {
        trigger.Execute(&*current_db_.execution_db_accessor_, &execution_memory,
                        flags::run_time::execution_timeout_sec_, &interpreter_context_->is_shutting_down,
                        &transaction_status_, *trigger_context, interpreter_context_->auth_checker);
      } catch (const utils::BasicException &e) {
        throw utils::BasicException(
            fmt::format("Trigger '{}' caused the transaction to fail.\nException: {}", trigger.Name(), e.what()));
      }
    }
    SPDLOG_DEBUG("Finished executing before commit triggers");
  }

  const auto reset_necessary_members = [this]() {
    for (auto &qe : query_executions_) {
      if (qe) qe->CleanRuntimeData();
    }
    current_db_.CleanupDBTransaction(false);
  };
  utils::OnScopeExit members_reseter(reset_necessary_members);

  auto commit_confirmed_by_all_sync_repplicas = true;

  auto maybe_commit_error = current_db_.db_transactional_accessor_->Commit();
  if (maybe_commit_error.HasError()) {
    const auto &error = maybe_commit_error.GetError();

    std::visit(
        [&execution_db_accessor = current_db_.execution_db_accessor_,
         &commit_confirmed_by_all_sync_repplicas]<typename T>(T &&arg) {
          using ErrorType = std::remove_cvref_t<T>;
          if constexpr (std::is_same_v<ErrorType, storage::ReplicationError>) {
            commit_confirmed_by_all_sync_repplicas = false;
          } else if constexpr (std::is_same_v<ErrorType, storage::ConstraintViolation>) {
            const auto &constraint_violation = arg;
            auto &label_name = execution_db_accessor->LabelToName(constraint_violation.label);
            switch (constraint_violation.type) {
              case storage::ConstraintViolation::Type::EXISTENCE: {
                MG_ASSERT(constraint_violation.properties.size() == 1U);
                auto &property_name = execution_db_accessor->PropertyToName(*constraint_violation.properties.begin());
                throw QueryException("Unable to commit due to existence constraint violation on :{}({})", label_name,
                                     property_name);
              }
              case storage::ConstraintViolation::Type::UNIQUE: {
                std::stringstream property_names_stream;
                utils::PrintIterable(property_names_stream, constraint_violation.properties, ", ",
                                     [&execution_db_accessor](auto &stream, const auto &prop) {
                                       stream << execution_db_accessor->PropertyToName(prop);
                                     });
                throw QueryException("Unable to commit due to unique constraint violation on :{}({})", label_name,
                                     property_names_stream.str());
              }
            }
          } else if constexpr (std::is_same_v<ErrorType, storage::SerializationError>) {
            throw QueryException("Unable to commit due to serialization error.");
          } else if constexpr (std::is_same_v<ErrorType, storage::PersistenceError>) {
            throw QueryException("Unable to commit due to persistance error.");
          } else {
            static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
          }
        },
        error);
  }

  // The ordered execution of after commit triggers is heavily depending on the exclusiveness of
  // db_accessor_->Commit(): only one of the transactions can be commiting at the same time, so when the commit is
  // finished, that transaction probably will schedule its after commit triggers, because the other transactions that
  // want to commit are still waiting for commiting or one of them just started commiting its changes. This means the
  // ordered execution of after commit triggers are not guaranteed.
  if (trigger_context && db->trigger_store()->AfterCommitTriggers().size() > 0) {
    db->AddTask([this, trigger_context = std::move(*trigger_context),
                 user_transaction = std::shared_ptr(std::move(current_db_.db_transactional_accessor_))]() mutable {
      // TODO: Should this take the db_ and not Access()?
      RunTriggersAfterCommit(*current_db_.db_acc_, interpreter_context_, std::move(trigger_context),
                             &this->transaction_status_);
      user_transaction->FinalizeTransaction();
      SPDLOG_DEBUG("Finished executing after commit triggers");  // NOLINT(bugprone-lambda-function-name)
    });
  }

  SPDLOG_DEBUG("Finished committing the transaction");
  if (!commit_confirmed_by_all_sync_repplicas) {
    throw ReplicationException("At least one SYNC replica has not confirmed committing last transaction.");
  }
}

void Interpreter::AdvanceCommand() {
  if (!current_db_.db_transactional_accessor_) return;
  current_db_.db_transactional_accessor_->AdvanceCommand();
}

void Interpreter::AbortCommand(std::unique_ptr<QueryExecution> *query_execution) {
  if (query_execution) {
    query_execution->reset(nullptr);
  }
  if (in_explicit_transaction_) {
    expect_rollback_ = true;
  } else {
    Abort();
  }
}

std::optional<storage::IsolationLevel> Interpreter::GetIsolationLevelOverride() {
  if (next_transaction_isolation_level) {
    const auto isolation_level = *next_transaction_isolation_level;
    next_transaction_isolation_level.reset();
    return isolation_level;
  }

  return interpreter_isolation_level;
}

void Interpreter::SetNextTransactionIsolationLevel(const storage::IsolationLevel isolation_level) {
  next_transaction_isolation_level.emplace(isolation_level);
}

void Interpreter::SetSessionIsolationLevel(const storage::IsolationLevel isolation_level) {
  interpreter_isolation_level.emplace(isolation_level);
}
void Interpreter::ResetUser() { username_.reset(); }
void Interpreter::SetUser(std::string_view username) { username_ = username; }

}  // namespace memgraph::query
