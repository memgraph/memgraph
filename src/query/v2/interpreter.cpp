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

#include "query/v2/interpreter.hpp"

#include <fmt/core.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>

#include "coordinator/coordinator_client.hpp"
#include "expr/ast/ast_visitor.hpp"
#include "io/local_transport/local_system.hpp"
#include "io/local_transport/local_transport.hpp"
#include "memory/memory_control.hpp"
#include "parser/opencypher/parser.hpp"
#include "query/v2/bindings/eval.hpp"
#include "query/v2/bindings/frame.hpp"
#include "query/v2/bindings/symbol_table.hpp"
#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/common.hpp"
#include "query/v2/constants.hpp"
#include "query/v2/context.hpp"
#include "query/v2/cypher_query_interpreter.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/exceptions.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/frontend/semantic/required_privileges.hpp"
#include "query/v2/metadata.hpp"
#include "query/v2/plan/planner.hpp"
#include "query/v2/plan/profile.hpp"
#include "query/v2/plan/vertex_count_cache.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/storage.hpp"
#include "utils/algorithm.hpp"
#include "utils/csv_parsing.hpp"
#include "utils/event_counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/license.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/readable_size.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"
#include "utils/tsc.hpp"
#include "utils/variant_helpers.hpp"

namespace EventCounter {
extern Event ReadQuery;
extern Event WriteQuery;
extern Event ReadWriteQuery;

extern const Event LabelIndexCreated;
extern const Event LabelPropertyIndexCreated;

extern const Event StreamsCreated;
extern const Event TriggersCreated;
}  // namespace EventCounter

namespace memgraph::query::v2 {

namespace {
void UpdateTypeCount(const plan::ReadWriteTypeChecker::RWType type) {
  switch (type) {
    case plan::ReadWriteTypeChecker::RWType::R:
      EventCounter::IncrementCounter(EventCounter::ReadQuery);
      break;
    case plan::ReadWriteTypeChecker::RWType::W:
      EventCounter::IncrementCounter(EventCounter::WriteQuery);
      break;
    case plan::ReadWriteTypeChecker::RWType::RW:
      EventCounter::IncrementCounter(EventCounter::ReadWriteQuery);
      break;
    default:
      break;
  }
}

struct Callback {
  std::vector<std::string> header;
  using CallbackFunction = std::function<std::vector<std::vector<TypedValue>>()>;
  CallbackFunction fn;
  bool should_abort_query{false};
};

TypedValue EvaluateOptionalExpression(Expression *expression, ExpressionEvaluator *eval) {
  return expression ? expression->Accept(*eval) : TypedValue();
}

template <typename TResult>
std::optional<TResult> GetOptionalValue(query::v2::Expression *expression, ExpressionEvaluator &evaluator) {
  if (expression != nullptr) {
    auto int_value = expression->Accept(evaluator);
    MG_ASSERT(int_value.IsNull() || int_value.IsInt());
    if (int_value.IsInt()) {
      return TResult{int_value.ValueInt()};
    }
  }
  return {};
};

class ReplQueryHandler final : public query::v2::ReplicationQueryHandler {
 public:
  explicit ReplQueryHandler(storage::v3::Shard * /*db*/) {}

  /// @throw QueryRuntimeException if an error ocurred.
  void SetReplicationRole(ReplicationQuery::ReplicationRole /*replication_role*/,
                          std::optional<int64_t> /*port*/) override {}

  /// @throw QueryRuntimeException if an error ocurred.
  ReplicationQuery::ReplicationRole ShowReplicationRole() const override { return {}; }

  /// @throw QueryRuntimeException if an error ocurred.
  void RegisterReplica(const std::string & /*name*/, const std::string & /*socket_address*/,
                       const ReplicationQuery::SyncMode /*sync_mode*/, const std::optional<double> /*timeout*/,
                       const std::chrono::seconds /*replica_check_frequency*/) override {}

  /// @throw QueryRuntimeException if an error ocurred.
  void DropReplica(const std::string & /*replica_name*/) override {}

  using Replica = ReplicationQueryHandler::Replica;
  std::vector<Replica> ShowReplicas() const override { return {}; }
};
/// returns false if the replication role can't be set
/// @throw QueryRuntimeException if an error ocurred.

Callback HandleAuthQuery(AuthQuery *auth_query, AuthQueryHandler *auth, const Parameters &parameters,
                         msgs::ShardRequestManagerInterface *manager) {
  // Empty frame for evaluation of password expression. This is OK since
  // password should be either null or string literal and it's evaluation
  // should not depend on frame.
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, manager, storage::v3::View::OLD);

  std::string username = auth_query->user_;
  std::string rolename = auth_query->role_;
  std::string user_or_role = auth_query->user_or_role_;
  std::vector<AuthQuery::Privilege> privileges = auth_query->privileges_;
  auto password = EvaluateOptionalExpression(auth_query->password_, &evaluator);

  Callback callback;

  const auto license_check_result = utils::license::global_license_checker.IsValidLicense(utils::global_settings);

  static const std::unordered_set enterprise_only_methods{
      AuthQuery::Action::CREATE_ROLE,       AuthQuery::Action::DROP_ROLE,       AuthQuery::Action::SET_ROLE,
      AuthQuery::Action::CLEAR_ROLE,        AuthQuery::Action::GRANT_PRIVILEGE, AuthQuery::Action::DENY_PRIVILEGE,
      AuthQuery::Action::REVOKE_PRIVILEGE,  AuthQuery::Action::SHOW_PRIVILEGES, AuthQuery::Action::SHOW_USERS_FOR_ROLE,
      AuthQuery::Action::SHOW_ROLE_FOR_USER};

  if (license_check_result.HasError() && enterprise_only_methods.contains(auth_query->action_)) {
    throw utils::BasicException(
        utils::license::LicenseCheckErrorToString(license_check_result.GetError(), "advanced authentication features"));
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
          auth->GrantPrivilege(username, kPrivilegesAll);
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
      callback.fn = [auth, user_or_role, privileges] {
        auth->GrantPrivilege(user_or_role, privileges);
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
      callback.fn = [auth, user_or_role, privileges] {
        auth->RevokePrivilege(user_or_role, privileges);
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
    default:
      break;
  }
}

Callback HandleReplicationQuery(ReplicationQuery *repl_query, const Parameters &parameters,
                                InterpreterContext *interpreter_context, msgs::ShardRequestManagerInterface *manager,
                                std::vector<Notification> *notifications) {
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, manager, storage::v3::View::OLD);

  Callback callback;
  switch (repl_query->action_) {
    case ReplicationQuery::Action::SET_REPLICATION_ROLE: {
      auto port = EvaluateOptionalExpression(repl_query->port_, &evaluator);
      std::optional<int64_t> maybe_port;
      if (port.IsInt()) {
        maybe_port = port.ValueInt();
      }
      if (maybe_port == 7687 && repl_query->role_ == ReplicationQuery::ReplicationRole::REPLICA) {
        notifications->emplace_back(SeverityLevel::WARNING, NotificationCode::REPLICA_PORT_WARNING,
                                    "Be careful the replication port must be different from the memgraph port!");
      }
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, role = repl_query->role_,
                     maybe_port]() mutable {
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
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}] {
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
      auto timeout = EvaluateOptionalExpression(repl_query->timeout_, &evaluator);
      const auto replica_check_frequency = interpreter_context->config.replication_replica_check_frequency;
      std::optional<double> maybe_timeout;
      if (timeout.IsDouble()) {
        maybe_timeout = timeout.ValueDouble();
      } else if (timeout.IsInt()) {
        maybe_timeout = static_cast<double>(timeout.ValueInt());
      }
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, name, socket_address, sync_mode,
                     maybe_timeout, replica_check_frequency]() mutable {
        handler.RegisterReplica(name, std::string(socket_address.ValueString()), sync_mode, maybe_timeout,
                                replica_check_frequency);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::REGISTER_REPLICA,
                                  fmt::format("Replica {} is registered.", repl_query->replica_name_));
      return callback;
    }

    case ReplicationQuery::Action::DROP_REPLICA: {
      const auto &name = repl_query->replica_name_;
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, name]() mutable {
        handler.DropReplica(name);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::DROP_REPLICA,
                                  fmt::format("Replica {} is dropped.", repl_query->replica_name_));
      return callback;
    }

    case ReplicationQuery::Action::SHOW_REPLICAS: {
      callback.header = {"name", "socket_address", "sync_mode", "timeout", "state"};
      callback.fn = [handler = ReplQueryHandler{interpreter_context->db}, replica_nfields = callback.header.size()] {
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

          if (replica.timeout) {
            typed_replica.emplace_back(TypedValue(*replica.timeout));
          } else {
            typed_replica.emplace_back(TypedValue());
          }

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

Callback HandleSettingQuery(SettingQuery *setting_query, const Parameters &parameters,
                            msgs::ShardRequestManagerInterface *manager) {
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  evaluation_context.timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  evaluation_context.parameters = parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, manager, storage::v3::View::OLD);

  Callback callback;
  switch (setting_query->action_) {
    case SettingQuery::Action::SET_SETTING: {
      const auto setting_name = EvaluateOptionalExpression(setting_query->setting_name_, &evaluator);
      if (!setting_name.IsString()) {
        throw utils::BasicException("Setting name should be a string literal");
      }

      const auto setting_value = EvaluateOptionalExpression(setting_query->setting_value_, &evaluator);
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
      const auto setting_name = EvaluateOptionalExpression(setting_query->setting_name_, &evaluator);
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

Callback HandleSchemaQuery(SchemaQuery *schema_query, InterpreterContext *interpreter_context,
                           std::vector<Notification> *notifications) {
  Callback callback;
  switch (schema_query->action_) {
    case SchemaQuery::Action::SHOW_SCHEMAS: {
      callback.header = {"label", "primary_key"};
      callback.fn = [interpreter_context]() {
        auto *db = interpreter_context->db;
        auto schemas_info = db->ListAllSchemas();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(schemas_info.schemas.size());

        for (const auto &[label_id, schema_types] : schemas_info.schemas) {
          std::vector<TypedValue> schema_info_row;
          schema_info_row.reserve(3);

          schema_info_row.emplace_back(db->LabelToName(label_id));
          std::vector<std::string> primary_key_properties;
          primary_key_properties.reserve(schema_types.size());
          std::transform(schema_types.begin(), schema_types.end(), std::back_inserter(primary_key_properties),
                         [&db](const auto &schema_type) {
                           return db->PropertyToName(schema_type.property_id) +
                                  "::" + storage::v3::SchemaTypeToString(schema_type.type);
                         });

          schema_info_row.emplace_back(utils::Join(primary_key_properties, ", "));
          results.push_back(std::move(schema_info_row));
        }
        return results;
      };
      return callback;
    }
    case SchemaQuery::Action::SHOW_SCHEMA: {
      callback.header = {"property_name", "property_type"};
      callback.fn = [interpreter_context, primary_label = schema_query->label_]() {
        auto *db = interpreter_context->db;
        const auto label = interpreter_context->NameToLabelId(primary_label.name);
        const auto *schema = db->GetSchema(label);
        std::vector<std::vector<TypedValue>> results;
        if (schema) {
          for (const auto &schema_property : schema->second) {
            std::vector<TypedValue> schema_info_row;
            schema_info_row.reserve(2);
            schema_info_row.emplace_back(db->PropertyToName(schema_property.property_id));
            schema_info_row.emplace_back(storage::v3::SchemaTypeToString(schema_property.type));
            results.push_back(std::move(schema_info_row));
          }
          return results;
        }
        throw QueryException(fmt::format("Schema on label :{} not found!", primary_label.name));
      };
      return callback;
    }
    case SchemaQuery::Action::CREATE_SCHEMA: {
      auto schema_type_map = schema_query->schema_type_map_;
      if (schema_query->schema_type_map_.empty()) {
        throw SyntaxException("One or more types have to be defined in schema definition.");
      }
      callback.fn = [interpreter_context, primary_label = schema_query->label_,
                     schema_type_map = std::move(schema_type_map)]() {
        auto *db = interpreter_context->db;
        const auto label = interpreter_context->NameToLabelId(primary_label.name);
        std::vector<storage::v3::SchemaProperty> schemas_types;
        schemas_types.reserve(schema_type_map.size());
        for (const auto &schema_type : schema_type_map) {
          auto property_id = interpreter_context->NameToPropertyId(schema_type.first.name);
          schemas_types.push_back({property_id, schema_type.second});
        }
        if (!db->CreateSchema(label, schemas_types)) {
          throw QueryException(fmt::format("Schema on label :{} already exists!", primary_label.name));
        }
        return std::vector<std::vector<TypedValue>>{};
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::CREATE_SCHEMA,
                                  fmt::format("Create schema on label :{}", schema_query->label_.name));
      return callback;
    }
    case SchemaQuery::Action::DROP_SCHEMA: {
      callback.fn = [interpreter_context, primary_label = schema_query->label_]() {
        auto *db = interpreter_context->db;
        const auto label = interpreter_context->NameToLabelId(primary_label.name);
        if (!db->DropSchema(label)) {
          throw QueryException(fmt::format("Schema on label :{} does not exist!", primary_label.name));
        }

        return std::vector<std::vector<TypedValue>>{};
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::DROP_SCHEMA,
                                  fmt::format("Dropped schema on label :{}", schema_query->label_.name));
      return callback;
    }
  }
  return callback;
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

struct PullPlan {
  explicit PullPlan(std::shared_ptr<CachedPlan> plan, const Parameters &parameters, bool is_profile_query,
                    DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                    msgs::ShardRequestManagerInterface *shard_request_manager = nullptr,
                    //                    TriggerContextCollector *trigger_context_collector = nullptr,
                    std::optional<size_t> memory_limit = {});
  std::optional<plan::ProfilingStatsWithTotalTime> Pull(AnyStream *stream, std::optional<int> n,
                                                        const std::vector<Symbol> &output_symbols,
                                                        std::map<std::string, TypedValue> *summary);
  std::optional<plan::ProfilingStatsWithTotalTime> PullMultiple(AnyStream *stream, std::optional<int> n,
                                                                const std::vector<Symbol> &output_symbols,
                                                                std::map<std::string, TypedValue> *summary);

 private:
  std::shared_ptr<CachedPlan> plan_ = nullptr;
  plan::UniqueCursorPtr cursor_ = nullptr;
  Frame frame_;
  MultiFrame multi_frame_;
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
};

PullPlan::PullPlan(const std::shared_ptr<CachedPlan> plan, const Parameters &parameters, const bool is_profile_query,
                   DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                   msgs::ShardRequestManagerInterface *shard_request_manager, const std::optional<size_t> memory_limit)
    : plan_(plan),
      cursor_(plan->plan().MakeCursor(execution_memory)),
      frame_(plan->symbol_table().max_position(), execution_memory),
      multi_frame_({.frames = utils::pmr::vector<Frame>(1000, frame_, execution_memory), .valid_frames = 0}),
      memory_limit_(memory_limit) {
  ctx_.db_accessor = dba;
  ctx_.symbol_table = plan->symbol_table();
  ctx_.evaluation_context.timestamp = QueryTimestamp();
  ctx_.evaluation_context.parameters = parameters;
  ctx_.evaluation_context.properties = NamesToProperties(plan->ast_storage().properties_, shard_request_manager);
  ctx_.evaluation_context.labels = NamesToLabels(plan->ast_storage().labels_, shard_request_manager);
  if (interpreter_context->config.execution_timeout_sec > 0) {
    ctx_.timer = utils::AsyncTimer{interpreter_context->config.execution_timeout_sec};
  }
  ctx_.is_shutting_down = &interpreter_context->is_shutting_down;
  ctx_.is_profile_query = is_profile_query;
  ctx_.shard_request_manager = shard_request_manager;
  ctx_.edge_ids_alloc = &interpreter_context->edge_ids_alloc;
}

std::optional<plan::ProfilingStatsWithTotalTime> PullPlan::Pull(AnyStream *stream, std::optional<int> n,
                                                                const std::vector<Symbol> &output_symbols,
                                                                std::map<std::string, TypedValue> *summary) {
  if (!output_symbols.empty() && output_symbols[0].name() == "mmm") {
    return PullMultiple(stream, n, output_symbols, summary);
  }
  // Set up temporary memory for a single Pull. Initial memory comes from the
  // stack. 256 KiB should fit on the stack and should be more than enough for a
  // single `Pull`.
  static constexpr size_t stack_size = 256UL * 1024UL;
  char stack_data[stack_size];
  utils::ResourceWithOutOfMemoryException resource_with_exception;
  utils::MonotonicBufferResource monotonic_memory(&stack_data[0], stack_size, &resource_with_exception);
  // We can throw on every query because a simple queries for deleting will use only
  // the stack allocated buffer.
  // Also, we want to throw only when the query engine requests more memory and not the storage
  // so we add the exception to the allocator.
  // TODO (mferencevic): Tune the parameters accordingly.
  utils::PoolResource pool_memory(128, 1024, &monotonic_memory);
  std::optional<utils::LimitedMemoryResource> maybe_limited_resource;

  if (memory_limit_) {
    maybe_limited_resource.emplace(&pool_memory, *memory_limit_);
    ctx_.evaluation_context.memory = &*maybe_limited_resource;
  } else {
    ctx_.evaluation_context.memory = &pool_memory;
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

std::optional<plan::ProfilingStatsWithTotalTime> PullPlan::PullMultiple(AnyStream *stream, std::optional<int> n,
                                                                        const std::vector<Symbol> &output_symbols,
                                                                        std::map<std::string, TypedValue> *summary) {
  // Set up temporary memory for a single Pull. Initial memory comes from the
  // stack. 256 KiB should fit on the stack and should be more than enough for a
  // single `Pull`.
  MG_ASSERT(!n.has_value(), "should pull all!");
  static constexpr size_t stack_size = 256UL * 1024UL;
  char stack_data[stack_size];
  utils::ResourceWithOutOfMemoryException resource_with_exception;
  utils::MonotonicBufferResource monotonic_memory(&stack_data[0], stack_size, &resource_with_exception);
  // We can throw on every query because a simple queries for deleting will use only
  // the stack allocated buffer.
  // Also, we want to throw only when the query engine requests more memory and not the storage
  // so we add the exception to the allocator.
  // TODO (mferencevic): Tune the parameters accordingly.
  utils::PoolResource pool_memory(128, 1024, &monotonic_memory);
  std::optional<utils::LimitedMemoryResource> maybe_limited_resource;

  if (memory_limit_) {
    maybe_limited_resource.emplace(&pool_memory, *memory_limit_);
    ctx_.evaluation_context.memory = &*maybe_limited_resource;
  } else {
    ctx_.evaluation_context.memory = &pool_memory;
  }

  // Returns true if a result was pulled.
  const auto pull_result = [&]() -> bool {
    cursor_->PullMultiple(multi_frame_, ctx_);
    return multi_frame_.valid_frames > 0;
  };

  const auto stream_values = [&output_symbols, &stream](Frame &frame) {
    // TODO: The streamed values should also probably use the above memory.
    std::vector<TypedValue> values;
    values.reserve(output_symbols.size());

    for (const auto &symbol : output_symbols) {
      values.emplace_back(frame[symbol]);
    }

    stream->Result(values);
  };

  // Get the execution time of all possible result pulls and streams.
  utils::Timer timer;

  int i = 0;
  if (has_unsent_results_ && !output_symbols.empty()) {
    // stream unsent results from previous pull
    for (auto frame_index = 0U; frame_index < multi_frame_.valid_frames; ++frame_index) {
      stream_values(multi_frame_.frames[frame_index]);
      ++i;
    }
    multi_frame_.valid_frames = 0;
  }

  for (; !n || i < n;) {
    if (!pull_result()) {
      break;
    }

    if (!output_symbols.empty()) {
      for (auto frame_index = 0U; frame_index < multi_frame_.valid_frames; ++frame_index) {
        stream_values(multi_frame_.frames[frame_index]);
        ++i;
      }
    }
    multi_frame_.valid_frames = 0;
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
}  // namespace

InterpreterContext::InterpreterContext(storage::v3::Shard *db, const InterpreterConfig config,
                                       const std::filesystem::path & /*data_directory*/,
                                       io::Io<io::local_transport::LocalTransport> io,
                                       coordinator::Address coordinator_addr)
    : db(db), config(config), io{std::move(io)}, coordinator_address{coordinator_addr} {}

Interpreter::Interpreter(InterpreterContext *interpreter_context) : interpreter_context_(interpreter_context) {
  MG_ASSERT(interpreter_context_, "Interpreter context must not be NULL");
  auto query_io = interpreter_context_->io.ForkLocal();
  shard_request_manager_ = std::make_unique<msgs::ShardRequestManager<io::local_transport::LocalTransport>>(
      coordinator::CoordinatorClient<io::local_transport::LocalTransport>(
          query_io, interpreter_context_->coordinator_address, std::vector{interpreter_context_->coordinator_address}),
      std::move(query_io));
  // Get edge ids
  coordinator::CoordinatorWriteRequests requests{coordinator::AllocateEdgeIdBatchRequest{.batch_size = 1000000}};
  io::rsm::WriteRequest<coordinator::CoordinatorWriteRequests> ww;
  ww.operation = requests;
  auto resp = interpreter_context_->io
                  .Request<io::rsm::WriteRequest<coordinator::CoordinatorWriteRequests>,
                           io::rsm::WriteResponse<coordinator::CoordinatorWriteResponses>>(
                      interpreter_context_->coordinator_address, ww)
                  .Wait();
  if (resp.HasValue()) {
    const auto alloc_edge_id_reps =
        std::get<coordinator::AllocateEdgeIdBatchResponse>(resp.GetValue().message.write_return);
    interpreter_context_->edge_ids_alloc = {alloc_edge_id_reps.low, alloc_edge_id_reps.high};
  }
}

PreparedQuery Interpreter::PrepareTransactionQuery(std::string_view query_upper) {
  std::function<void()> handler;

  if (query_upper == "BEGIN") {
    handler = [this] {
      if (in_explicit_transaction_) {
        throw ExplicitTransactionUsageException("Nested transactions are not supported.");
      }
      in_explicit_transaction_ = true;
      expect_rollback_ = false;
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
    };
  } else if (query_upper == "ROLLBACK") {
    handler = [this] {
      if (!in_explicit_transaction_) {
        throw ExplicitTransactionUsageException("No current transaction to rollback.");
      }
      Abort();
      expect_rollback_ = false;
      in_explicit_transaction_ = false;
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

PreparedQuery PrepareCypherQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
                                 InterpreterContext *interpreter_context, DbAccessor *dba,
                                 utils::MemoryResource *execution_memory, std::vector<Notification> *notifications,
                                 msgs::ShardRequestManagerInterface *shard_request_manager) {
  //                                 TriggerContextCollector *trigger_context_collector = nullptr) {
  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_query.query);

  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parsed_query.parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, shard_request_manager,
                                storage::v3::View::OLD);
  const auto memory_limit =
      expr::EvaluateMemoryLimit(&evaluator, cypher_query->memory_limit_, cypher_query->memory_scale_);
  if (memory_limit) {
    spdlog::info("Running query with memory limit of {}", utils::GetReadableSize(*memory_limit));
  }

  if (const auto &clauses = cypher_query->single_query_->clauses_; std::any_of(
          clauses.begin(), clauses.end(), [](const auto *clause) { return clause->GetTypeInfo() == LoadCsv::kType; })) {
    notifications->emplace_back(
        SeverityLevel::INFO, NotificationCode::LOAD_CSV_TIP,
        "It's important to note that the parser parses the values as strings. It's up to the user to "
        "convert the parsed row values to the appropriate type. This can be done using the built-in "
        "conversion functions such as ToInteger, ToFloat, ToBoolean etc.");
  }
  auto plan = CypherQueryToPlan(
      parsed_query.stripped_query.hash(), std::move(parsed_query.ast_storage), cypher_query, parsed_query.parameters,
      parsed_query.is_cacheable ? &interpreter_context->plan_cache : nullptr, shard_request_manager);

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
  auto pull_plan = std::make_shared<PullPlan>(plan, parsed_query.parameters, false, dba, interpreter_context,
                                              execution_memory, shard_request_manager, memory_limit);
  //                                              execution_memory, trigger_context_collector, memory_limit);
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
                                  InterpreterContext *interpreter_context,
                                  msgs::ShardRequestManagerInterface *shard_request_manager,
                                  utils::MemoryResource *execution_memory) {
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

  auto cypher_query_plan = CypherQueryToPlan(
      parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage), cypher_query,
      parsed_inner_query.parameters, parsed_inner_query.is_cacheable ? &interpreter_context->plan_cache : nullptr,
      shard_request_manager);

  std::stringstream printed_plan;
  plan::PrettyPrint(*shard_request_manager, &cypher_query_plan->plan(), &printed_plan);

  std::vector<std::vector<TypedValue>> printed_plan_rows;
  for (const auto &row : utils::Split(utils::RTrim(printed_plan.str()), "\n")) {
    printed_plan_rows.push_back(std::vector<TypedValue>{TypedValue(row)});
  }

  summary->insert_or_assign("explain", plan::PlanToJson(*shard_request_manager, &cypher_query_plan->plan()).dump());

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
                                  DbAccessor *dba, utils::MemoryResource *execution_memory,
                                  msgs::ShardRequestManagerInterface *shard_request_manager = nullptr) {
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

  if (!interpreter_context->tsc_frequency) {
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
  MG_ASSERT(cypher_query, "Cypher grammar should not allow other queries in PROFILE");
  Frame frame(0);
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parsed_inner_query.parameters;
  ExpressionEvaluator evaluator(&frame, symbol_table, evaluation_context, shard_request_manager,
                                storage::v3::View::OLD);
  const auto memory_limit =
      expr::EvaluateMemoryLimit(&evaluator, cypher_query->memory_limit_, cypher_query->memory_scale_);

  auto cypher_query_plan = CypherQueryToPlan(
      parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage), cypher_query,
      parsed_inner_query.parameters, parsed_inner_query.is_cacheable ? &interpreter_context->plan_cache : nullptr,
      shard_request_manager);
  auto rw_type_checker = plan::ReadWriteTypeChecker();
  rw_type_checker.InferRWType(const_cast<plan::LogicalOperator &>(cypher_query_plan->plan()));

  return PreparedQuery{{"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME", "CUSTOM DATA"},
                       std::move(parsed_query.required_privileges),
                       [plan = std::move(cypher_query_plan), parameters = std::move(parsed_inner_query.parameters),
                        summary, dba, interpreter_context, execution_memory, memory_limit, shard_request_manager,
                        // We want to execute the query we are profiling lazily, so we delay
                        // the construction of the corresponding context.
                        stats_and_total_time = std::optional<plan::ProfilingStatsWithTotalTime>{},
                        pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         // No output symbols are given so that nothing is streamed.
                         if (!stats_and_total_time) {
                           stats_and_total_time = PullPlan(plan, parameters, true, dba, interpreter_context,
                                                           execution_memory, shard_request_manager, memory_limit)
                                                      .PullMultiple(stream, {}, {}, summary);
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

PreparedQuery PrepareDumpQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary, DbAccessor *dba,
                               utils::MemoryResource *execution_memory) {
  throw QueryRuntimeException("Dump query is not supported!");
}

PreparedQuery PrepareIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                std::vector<Notification> *notifications, InterpreterContext *interpreter_context) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  auto *index_query = utils::Downcast<IndexQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  // Creating an index influences computed plan costs.
  auto invalidate_plan_cache = [plan_cache = &interpreter_context->plan_cache] {
    auto access = plan_cache->access();
    for (auto &kv : access) {
      access.remove(kv.first);
    }
  };

  const auto label = interpreter_context->NameToLabelId(index_query->label_.name);

  std::vector<storage::v3::PropertyId> properties;
  std::vector<std::string> properties_string;
  properties.reserve(index_query->properties_.size());
  properties_string.reserve(index_query->properties_.size());
  for (const auto &prop : index_query->properties_) {
    properties.push_back(interpreter_context->NameToPropertyId(prop.name));
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

      handler = [interpreter_context, label, properties_stringified = std::move(properties_stringified),
                 label_name = index_query->label_.name, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        if (properties.empty()) {
          if (!interpreter_context->db->CreateIndex(label)) {
            index_notification.code = NotificationCode::EXISTANT_INDEX;
            index_notification.title =
                fmt::format("Index on label {} on properties {} already exists.", label_name, properties_stringified);
          }
          EventCounter::IncrementCounter(EventCounter::LabelIndexCreated);
        } else {
          MG_ASSERT(properties.size() == 1U);
          if (!interpreter_context->db->CreateIndex(label, properties[0])) {
            index_notification.code = NotificationCode::EXISTANT_INDEX;
            index_notification.title =
                fmt::format("Index on label {} on properties {} already exists.", label_name, properties_stringified);
          }
          EventCounter::IncrementCounter(EventCounter::LabelPropertyIndexCreated);
        }
        invalidate_plan_cache();
      };
      break;
    }
    case IndexQuery::Action::DROP: {
      index_notification.code = NotificationCode::DROP_INDEX;
      index_notification.title = fmt::format("Dropped index on label {} on properties {}.", index_query->label_.name,
                                             utils::Join(properties_string, ", "));
      handler = [interpreter_context, label, properties_stringified = std::move(properties_stringified),
                 label_name = index_query->label_.name, properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        if (properties.empty()) {
          if (!interpreter_context->db->DropIndex(label)) {
            index_notification.code = NotificationCode::NONEXISTANT_INDEX;
            index_notification.title =
                fmt::format("Index on label {} on properties {} doesn't exist.", label_name, properties_stringified);
          }
        } else {
          MG_ASSERT(properties.size() == 1U);
          if (!interpreter_context->db->DropIndex(label, properties[0])) {
            index_notification.code = NotificationCode::NONEXISTANT_INDEX;
            index_notification.title =
                fmt::format("Index on label {} on properties {} doesn't exist.", label_name, properties_stringified);
          }
        }
        invalidate_plan_cache();
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
        return QueryHandlerResult::NOTHING;
      },
      RWType::W};
}

PreparedQuery PrepareAuthQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                               std::map<std::string, TypedValue> *summary, InterpreterContext *interpreter_context,
                               DbAccessor *dba, utils::MemoryResource *execution_memory,
                               msgs::ShardRequestManagerInterface *manager) {
  if (in_explicit_transaction) {
    throw UserModificationInMulticommandTxException();
  }

  auto *auth_query = utils::Downcast<AuthQuery>(parsed_query.query);

  auto callback = HandleAuthQuery(auth_query, interpreter_context->auth, parsed_query.parameters, manager);

  SymbolTable symbol_table;
  std::vector<Symbol> output_symbols;
  for (const auto &column : callback.header) {
    output_symbols.emplace_back(symbol_table.CreateSymbol(column, "false"));
  }

  auto plan = std::make_shared<CachedPlan>(std::make_unique<SingleNodeLogicalPlan>(
      std::make_unique<plan::OutputTable>(output_symbols,
                                          [fn = callback.fn](Frame *, ExecutionContext *) { return fn(); }),
      0.0, AstStorage{}, symbol_table));

  auto pull_plan =
      std::make_shared<PullPlan>(plan, parsed_query.parameters, false, dba, interpreter_context, execution_memory);
  return PreparedQuery{
      callback.header, std::move(parsed_query.required_privileges),
      [pull_plan = std::move(pull_plan), callback = std::move(callback), output_symbols = std::move(output_symbols),
       summary](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
        if (pull_plan->Pull(stream, n, output_symbols, summary)) {
          return callback.should_abort_query ? QueryHandlerResult::ABORT : QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      },
      RWType::NONE};
}

PreparedQuery PrepareReplicationQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                      std::vector<Notification> *notifications, InterpreterContext *interpreter_context,
                                      msgs::ShardRequestManagerInterface *manager) {
  if (in_explicit_transaction) {
    throw ReplicationModificationInMulticommandTxException();
  }

  auto *replication_query = utils::Downcast<ReplicationQuery>(parsed_query.query);
  auto callback =
      HandleReplicationQuery(replication_query, parsed_query.parameters, interpreter_context, manager, notifications);

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

PreparedQuery PrepareLockPathQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                   InterpreterContext *interpreter_context, DbAccessor *dba) {
  throw SemanticException("LockPath query is not supported!");
}

PreparedQuery PrepareFreeMemoryQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                     InterpreterContext * /*interpreter_context*/) {
  if (in_explicit_transaction) {
    throw FreeMemoryModificationInMulticommandTxException();
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [](AnyStream * /*stream*/, std::optional<int> /*n*/) -> std::optional<QueryHandlerResult> {
                         memory::PurgeUnusedMemory();
                         return QueryHandlerResult::COMMIT;
                       },
                       RWType::NONE};
}

constexpr auto ToStorageIsolationLevel(const IsolationLevelQuery::IsolationLevel isolation_level) noexcept {
  switch (isolation_level) {
    case IsolationLevelQuery::IsolationLevel::SNAPSHOT_ISOLATION:
      return storage::v3::IsolationLevel::SNAPSHOT_ISOLATION;
    case IsolationLevelQuery::IsolationLevel::READ_COMMITTED:
      return storage::v3::IsolationLevel::READ_COMMITTED;
    case IsolationLevelQuery::IsolationLevel::READ_UNCOMMITTED:
      return storage::v3::IsolationLevel::READ_UNCOMMITTED;
  }
}

PreparedQuery PrepareIsolationLevelQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                         InterpreterContext *interpreter_context, Interpreter *interpreter) {
  if (in_explicit_transaction) {
    throw IsolationLevelModificationInMulticommandTxException();
  }

  auto *isolation_level_query = utils::Downcast<IsolationLevelQuery>(parsed_query.query);
  MG_ASSERT(isolation_level_query);

  const auto isolation_level = ToStorageIsolationLevel(isolation_level_query->isolation_level_);

  auto callback = [isolation_level_query, isolation_level, interpreter_context,
                   interpreter]() -> std::function<void()> {
    switch (isolation_level_query->isolation_level_scope_) {
      case IsolationLevelQuery::IsolationLevelScope::GLOBAL:
        return [interpreter_context, isolation_level] { interpreter_context->db->SetIsolationLevel(isolation_level); };
      case IsolationLevelQuery::IsolationLevelScope::SESSION:
        return [interpreter, isolation_level] { interpreter->SetSessionIsolationLevel(isolation_level); };
      case IsolationLevelQuery::IsolationLevelScope::NEXT:
        return [interpreter, isolation_level] { interpreter->SetNextTransactionIsolationLevel(isolation_level); };
    }
  }();

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [callback = std::move(callback)](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
        callback();
        return QueryHandlerResult::COMMIT;
      },
      RWType::NONE};
}

PreparedQuery PrepareCreateSnapshotQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                         InterpreterContext *interpreter_context) {
  throw SemanticException("CreateSnapshot query is not supported!");
}

PreparedQuery PrepareSettingQuery(ParsedQuery parsed_query, const bool in_explicit_transaction,
                                  msgs::ShardRequestManagerInterface *manager) {
  if (in_explicit_transaction) {
    throw SettingConfigInMulticommandTxException{};
  }

  auto *setting_query = utils::Downcast<SettingQuery>(parsed_query.query);
  MG_ASSERT(setting_query);
  auto callback = HandleSettingQuery(setting_query, parsed_query.parameters, manager);

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

PreparedQuery PrepareVersionQuery(ParsedQuery parsed_query, const bool in_explicit_transaction) {
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

PreparedQuery PrepareInfoQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                               std::map<std::string, TypedValue> * /*summary*/, InterpreterContext *interpreter_context,
                               storage::v3::Shard *db, utils::MemoryResource * /*execution_memory*/) {
  if (in_explicit_transaction) {
    throw InfoInMulticommandTxException();
  }

  auto *info_query = utils::Downcast<InfoQuery>(parsed_query.query);
  std::vector<std::string> header;
  std::function<std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()> handler;

  switch (info_query->info_type_) {
    case InfoQuery::InfoType::STORAGE:
      header = {"storage info", "value"};
      handler = [db] {
        auto info = db->GetInfo();
        std::vector<std::vector<TypedValue>> results{
            {TypedValue("vertex_count"), TypedValue(static_cast<int64_t>(info.vertex_count))},
            {TypedValue("edge_count"), TypedValue(static_cast<int64_t>(info.edge_count))},
            {TypedValue("average_degree"), TypedValue(info.average_degree)},
            {TypedValue("memory_usage"), TypedValue(static_cast<int64_t>(info.memory_usage))},
            {TypedValue("memory_allocated"), TypedValue(static_cast<int64_t>(utils::total_memory_tracker.Amount()))},
            {TypedValue("allocation_limit"),
             TypedValue(static_cast<int64_t>(utils::total_memory_tracker.HardLimit()))}};
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
      break;
    case InfoQuery::InfoType::INDEX:
      header = {"index type", "label", "property"};
      handler = [interpreter_context] {
        auto *db = interpreter_context->db;
        auto info = db->ListAllIndices();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.label.size() + info.label_property.size());
        for (const auto &item : info.label) {
          results.push_back({TypedValue("label"), TypedValue(db->LabelToName(item)), TypedValue()});
        }
        for (const auto &item : info.label_property) {
          results.push_back({TypedValue("label+property"), TypedValue(db->LabelToName(item.first)),
                             TypedValue(db->PropertyToName(item.second))});
        }
        return std::pair{results, QueryHandlerResult::NOTHING};
      };
      break;
    case InfoQuery::InfoType::CONSTRAINT:
      throw SemanticException("Constraints are not yet supported!");
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
                                     std::vector<Notification> *notifications,
                                     InterpreterContext *interpreter_context) {
  throw SemanticException("Constraint query is not supported!");
}

PreparedQuery PrepareSchemaQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                 InterpreterContext *interpreter_context, std::vector<Notification> *notifications) {
  if (in_explicit_transaction) {
    throw ConstraintInMulticommandTxException();
  }
  auto *schema_query = utils::Downcast<SchemaQuery>(parsed_query.query);
  MG_ASSERT(schema_query);
  auto callback = HandleSchemaQuery(schema_query, interpreter_context, notifications);

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
                       [handler = std::move(callback.fn), action = QueryHandlerResult::NOTHING,
                        pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) {
                           auto results = handler();
                           pull_plan = std::make_shared<PullPlanVector>(std::move(results));
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return action;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
}

void Interpreter::BeginTransaction() {
  const auto prepared_query = PrepareTransactionQuery("BEGIN");
  prepared_query.query_handler(nullptr, {});
}

void Interpreter::CommitTransaction() {
  const auto prepared_query = PrepareTransactionQuery("COMMIT");
  prepared_query.query_handler(nullptr, {});
  query_executions_.clear();
}

void Interpreter::RollbackTransaction() {
  const auto prepared_query = PrepareTransactionQuery("ROLLBACK");
  prepared_query.query_handler(nullptr, {});
  query_executions_.clear();
}

Interpreter::PrepareResult Interpreter::Prepare(const std::string &query_string,
                                                const std::map<std::string, storage::v3::PropertyValue> &params,
                                                const std::string *username) {
  if (!in_explicit_transaction_) {
    query_executions_.clear();
  }

  query_executions_.emplace_back(std::make_unique<QueryExecution>());
  auto &query_execution = query_executions_.back();
  std::optional<int> qid =
      in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};

  // Handle transaction control queries.

  const auto upper_case_query = utils::ToUpperCase(query_string);
  const auto trimmed_query = utils::Trim(upper_case_query);

  if (trimmed_query == "BEGIN" || trimmed_query == "COMMIT" || trimmed_query == "ROLLBACK") {
    query_execution->prepared_query.emplace(PrepareTransactionQuery(trimmed_query));
    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid};
  }

  // All queries other than transaction control queries advance the command in
  // an explicit transaction block.
  if (in_explicit_transaction_) {
    AdvanceCommand();
  }
  // If we're not in an explicit transaction block and we have an open
  // transaction, abort it since we're about to prepare a new query.
  else if (db_accessor_) {
    AbortCommand(&query_execution);
  }

  try {
    // Set a default cost estimate of 0. Individual queries can overwrite this
    // field with an improved estimate.
    query_execution->summary["cost_estimate"] = 0.0;

    utils::Timer parsing_timer;
    ParsedQuery parsed_query =
        ParseQuery(query_string, params, &interpreter_context_->ast_cache, interpreter_context_->config.query);
    query_execution->summary["parsing_time"] = parsing_timer.Elapsed().count();
    if (!in_explicit_transaction_ &&
        (utils::Downcast<CypherQuery>(parsed_query.query) || utils::Downcast<ExplainQuery>(parsed_query.query) ||
         utils::Downcast<ProfileQuery>(parsed_query.query))) {
      shard_request_manager_->StartTransaction();
    }

    utils::Timer planning_timer;
    PreparedQuery prepared_query;

    if (utils::Downcast<CypherQuery>(parsed_query.query)) {
      prepared_query = PrepareCypherQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_,
                                          &*execution_db_accessor_, &query_execution->execution_memory,
                                          &query_execution->notifications, shard_request_manager_.get());
    } else if (utils::Downcast<ExplainQuery>(parsed_query.query)) {
      prepared_query = PrepareExplainQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_,
                                           &*shard_request_manager_, &query_execution->execution_memory_with_exception);
    } else if (utils::Downcast<ProfileQuery>(parsed_query.query)) {
      prepared_query = PrepareProfileQuery(
          std::move(parsed_query), in_explicit_transaction_, &query_execution->summary, interpreter_context_,
          &*execution_db_accessor_, &query_execution->execution_memory_with_exception, shard_request_manager_.get());
    } else if (utils::Downcast<DumpQuery>(parsed_query.query)) {
      prepared_query = PrepareDumpQuery(std::move(parsed_query), &query_execution->summary, &*execution_db_accessor_,
                                        &query_execution->execution_memory);
    } else if (utils::Downcast<IndexQuery>(parsed_query.query)) {
      prepared_query = PrepareIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                         &query_execution->notifications, interpreter_context_);
    } else if (utils::Downcast<AuthQuery>(parsed_query.query)) {
      prepared_query = PrepareAuthQuery(
          std::move(parsed_query), in_explicit_transaction_, &query_execution->summary, interpreter_context_,
          &*execution_db_accessor_, &query_execution->execution_memory_with_exception, shard_request_manager_.get());
    } else if (utils::Downcast<InfoQuery>(parsed_query.query)) {
      prepared_query = PrepareInfoQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                                        interpreter_context_, interpreter_context_->db,
                                        &query_execution->execution_memory_with_exception);
    } else if (utils::Downcast<ConstraintQuery>(parsed_query.query)) {
      prepared_query = PrepareConstraintQuery(std::move(parsed_query), in_explicit_transaction_,
                                              &query_execution->notifications, interpreter_context_);
    } else if (utils::Downcast<ReplicationQuery>(parsed_query.query)) {
      prepared_query =
          PrepareReplicationQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                                  interpreter_context_, shard_request_manager_.get());
    } else if (utils::Downcast<LockPathQuery>(parsed_query.query)) {
      prepared_query = PrepareLockPathQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_,
                                            &*execution_db_accessor_);
    } else if (utils::Downcast<FreeMemoryQuery>(parsed_query.query)) {
      prepared_query = PrepareFreeMemoryQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_);
    } else if (utils::Downcast<TriggerQuery>(parsed_query.query)) {
      throw std::runtime_error("Unimplemented");
    } else if (utils::Downcast<StreamQuery>(parsed_query.query)) {
      throw std::runtime_error("unimplemented");
    } else if (utils::Downcast<IsolationLevelQuery>(parsed_query.query)) {
      prepared_query =
          PrepareIsolationLevelQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_, this);
    } else if (utils::Downcast<CreateSnapshotQuery>(parsed_query.query)) {
      prepared_query =
          PrepareCreateSnapshotQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_);
    } else if (utils::Downcast<SettingQuery>(parsed_query.query)) {
      prepared_query =
          PrepareSettingQuery(std::move(parsed_query), in_explicit_transaction_, shard_request_manager_.get());
    } else if (utils::Downcast<VersionQuery>(parsed_query.query)) {
      prepared_query = PrepareVersionQuery(std::move(parsed_query), in_explicit_transaction_);
    } else if (utils::Downcast<SchemaQuery>(parsed_query.query)) {
      prepared_query = PrepareSchemaQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_,
                                          &query_execution->notifications);
    } else {
      LOG_FATAL("Should not get here -- unknown query type!");
    }

    query_execution->summary["planning_time"] = planning_timer.Elapsed().count();
    query_execution->prepared_query.emplace(std::move(prepared_query));

    const auto rw_type = query_execution->prepared_query->rw_type;
    query_execution->summary["type"] = plan::ReadWriteTypeChecker::TypeToString(rw_type);

    UpdateTypeCount(rw_type);

    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid};
  } catch (const utils::BasicException &) {
    EventCounter::IncrementCounter(EventCounter::FailedQuery);
    AbortCommand(&query_execution);
    throw;
  }
}

void Interpreter::Abort() {
  expect_rollback_ = false;
  in_explicit_transaction_ = false;
  if (!db_accessor_) return;
  db_accessor_->Abort();
  execution_db_accessor_.reset();
  db_accessor_.reset();
}

void Interpreter::Commit() {
  // It's possible that some queries did not finish because the user did
  // not pull all of the results from the query.
  // For now, we will not check if there are some unfinished queries.
  // We should document clearly that all results should be pulled to complete
  // a query.
  shard_request_manager_->Commit();
  if (!db_accessor_) return;

  const auto reset_necessary_members = [this]() {
    execution_db_accessor_.reset();
    db_accessor_.reset();
  };

  db_accessor_->Commit(coordinator::Hlc{});

  reset_necessary_members();

  SPDLOG_DEBUG("Finished committing the transaction");
}

void Interpreter::AdvanceCommand() {
  if (!db_accessor_) return;
  db_accessor_->AdvanceCommand();
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

std::optional<storage::v3::IsolationLevel> Interpreter::GetIsolationLevelOverride() {
  if (next_transaction_isolation_level) {
    const auto isolation_level = *next_transaction_isolation_level;
    next_transaction_isolation_level.reset();
    return isolation_level;
  }

  return interpreter_isolation_level;
}

void Interpreter::SetNextTransactionIsolationLevel(const storage::v3::IsolationLevel isolation_level) {
  next_transaction_isolation_level.emplace(isolation_level);
}

void Interpreter::SetSessionIsolationLevel(const storage::v3::IsolationLevel isolation_level) {
  interpreter_isolation_level.emplace(isolation_level);
}

}  // namespace memgraph::query::v2
