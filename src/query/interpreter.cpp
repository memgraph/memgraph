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
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "coordination/coordinator_state.hpp"
#include "coordination/register_main_replica_coordinator_status.hpp"
#include "csv/parsing.hpp"
#include "dbms/coordinator_handler.hpp"
#include "dbms/database.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/global.hpp"
#include "dbms/inmemory/storage_helper.hpp"
#include "flags/experimental.hpp"
#include "flags/replication.hpp"
#include "flags/run_time_configurable.hpp"
#include "glue/communication.hpp"
#include "io/network/endpoint.hpp"
#include "license/license.hpp"
#include "memory/global_memory_control.hpp"
#include "memory/query_memory_control.hpp"
#include "query/auth_query_handler.hpp"
#include "query/config.hpp"
#include "query/constants.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/dump.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/semantic/required_privileges.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/hops_limit.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/interpreter_context.hpp"
#include "query/metadata.hpp"
#include "query/parameters.hpp"
#include "query/plan/hint_provider.hpp"
#include "query/plan/planner.hpp"
#include "query/plan/profile.hpp"
#include "query/plan/vertex_count_cache.hpp"
#include "query/procedure/module.hpp"
#include "query/query_user.hpp"
#include "query/replication_query_handler.hpp"
#include "query/stream.hpp"
#include "query/stream/common.hpp"
#include "query/stream/sources.hpp"
#include "query/stream/streams.hpp"
#include "query/time_to_live/time_to_live.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
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
#include "utils/functional.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"
#include "utils/settings.hpp"
#include "utils/stat.hpp"
#include "utils/string.hpp"
#include "utils/tsc.hpp"
#include "utils/typeinfo.hpp"
#include "utils/variant_helpers.hpp"

#ifdef MG_ENTERPRISE
#include "flags/experimental.hpp"
#endif

namespace r = ranges;
namespace rv = ranges::views;

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

extern const Event ShowSchema;
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

constexpr std::string_view kSchemaAssert = "SCHEMA.ASSERT";
constexpr int kSystemTxTryMS = 100;  //!< Duration of the unique try_lock_for

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
[[maybe_unused]] bool Same(const TypedValue &lv, const std::string &rv) {
  return std::string_view(TypedValue(lv).ValueString()) == rv;
}
// NOLINTNEXTLINE (misc-unused-parameters)
[[maybe_unused]] bool Same(const std::string &lv, const TypedValue &rv) {
  return lv == std::string_view(TypedValue(rv).ValueString());
}
// NOLINTNEXTLINE (misc-unused-parameters)
[[maybe_unused]] bool Same(const std::string &lv, const std::string &rv) { return lv == rv; }

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

inline auto convertFromCoordinatorToReplicationMode(const CoordinatorQuery::SyncMode &sync_mode)
    -> replication_coordination_glue::ReplicationMode {
  switch (sync_mode) {
    case CoordinatorQuery::SyncMode::ASYNC: {
      return replication_coordination_glue::ReplicationMode::ASYNC;
    }
    case CoordinatorQuery::SyncMode::SYNC: {
      return replication_coordination_glue::ReplicationMode::SYNC;
    }
  }
  // TODO: C++23 std::unreachable()
  return replication_coordination_glue::ReplicationMode::ASYNC;
}

inline auto convertToReplicationMode(const ReplicationQuery::SyncMode &sync_mode)
    -> replication_coordination_glue::ReplicationMode {
  switch (sync_mode) {
    case ReplicationQuery::SyncMode::ASYNC: {
      return replication_coordination_glue::ReplicationMode::ASYNC;
    }
    case ReplicationQuery::SyncMode::SYNC: {
      return replication_coordination_glue::ReplicationMode::SYNC;
    }
  }
  // TODO: C++23 std::unreachable()
  return replication_coordination_glue::ReplicationMode::ASYNC;
}

class ReplQueryHandler {
 public:
  explicit ReplQueryHandler(query::ReplicationQueryHandler &replication_query_handler)
      : handler_{&replication_query_handler} {}

  /// @throw QueryRuntimeException if an error ocurred.
  void SetReplicationRole(ReplicationQuery::ReplicationRole replication_role, std::optional<int64_t> port) {
    auto ValidatePort = [](std::optional<int64_t> port) -> void {
      if (!port || *port < 0 || *port > std::numeric_limits<uint16_t>::max()) {
        throw QueryRuntimeException("Port number invalid!");
      }
    };
    if (replication_role == ReplicationQuery::ReplicationRole::MAIN) {
      if (!handler_->SetReplicationRoleMain()) {
        throw QueryRuntimeException("Couldn't set replication role to main!");
      }
    } else {
      ValidatePort(port);

      auto const config = memgraph::replication::ReplicationServerConfig{
          .repl_server = memgraph::io::network::Endpoint(memgraph::replication::kDefaultReplicationServerIp,
                                                         static_cast<uint16_t>(*port))};

      if (!handler_->TrySetReplicationRoleReplica(config, std::nullopt)) {
        throw QueryRuntimeException("Couldn't set role to replica!");
      }
    }
  }

  /// @throw QueryRuntimeException if an error ocurred.
  ReplicationQuery::ReplicationRole ShowReplicationRole() const {
    switch (handler_->GetRole()) {
      case memgraph::replication_coordination_glue::ReplicationRole::MAIN:
        return ReplicationQuery::ReplicationRole::MAIN;
      case memgraph::replication_coordination_glue::ReplicationRole::REPLICA:
        return ReplicationQuery::ReplicationRole::REPLICA;
    }
    throw QueryRuntimeException("Couldn't show replication role - invalid role set!");
  }

  /// @throw QueryRuntimeException if an error ocurred.
  void RegisterReplica(const std::string &name, const std::string &socket_address,
                       const ReplicationQuery::SyncMode sync_mode, const std::chrono::seconds replica_check_frequency) {
    // Coordinator is main by default so this check is OK although it should actually be nothing (neither main nor
    // replica)
    if (handler_->IsReplica()) {
      // replica can't register another replica
      throw QueryRuntimeException("Replica can't register another replica!");
    }

    const auto repl_mode = convertToReplicationMode(sync_mode);

    auto maybe_endpoint = io::network::Endpoint::ParseAndCreateSocketOrAddress(
        socket_address, memgraph::replication::kDefaultReplicationPort);
    if (maybe_endpoint) {
      const auto replication_config = replication::ReplicationClientConfig{
          .name = name,
          .mode = repl_mode,
          .repl_server_endpoint = std::move(*maybe_endpoint),  // don't resolve early
          .replica_check_frequency = replica_check_frequency,
          .ssl = std::nullopt};

      const auto error = handler_->TryRegisterReplica(replication_config).HasError();

      if (error) {
        throw QueryRuntimeException(fmt::format("Couldn't register replica '{}'!", name));
      }

    } else {
      throw QueryRuntimeException("Invalid socket address!");
    }
  }

  /// @throw QueryRuntimeException if an error occurred.
  void DropReplica(std::string_view replica_name) {
    auto const result = handler_->UnregisterReplica(replica_name);
    switch (result) {
      using enum memgraph::query::UnregisterReplicaResult;
      case NOT_MAIN:
        throw QueryRuntimeException("Replica can't unregister a replica!");
      case COULD_NOT_BE_PERSISTED:
        [[fallthrough]];
      case CAN_NOT_UNREGISTER:
        throw QueryRuntimeException(fmt::format("Couldn't unregister the replica '{}'", replica_name));
      case SUCCESS:
        break;
    }
  }

  std::vector<ReplicasInfo> ShowReplicas() const {
    auto info = handler_->ShowReplicas();
    if (info.HasError()) {
      switch (info.GetError()) {
        case ShowReplicaError::NOT_MAIN:
          throw QueryRuntimeException("Replica can't show registered replicas (it shouldn't have any)!");
      }
    }

    return info.GetValue().entries_;
  }

 private:
  query::ReplicationQueryHandler *handler_;
};

#ifdef MG_ENTERPRISE
class CoordQueryHandler final : public query::CoordinatorQueryHandler {
 public:
  explicit CoordQueryHandler(coordination::CoordinatorState &coordinator_state)

      : coordinator_handler_(coordinator_state) {}

  void UnregisterInstance(std::string_view instance_name) override {
    auto status = coordinator_handler_.UnregisterReplicationInstance(instance_name);
    switch (status) {
      using enum memgraph::coordination::UnregisterInstanceCoordinatorStatus;
      case NO_INSTANCE_WITH_NAME:
        throw QueryRuntimeException("No instance with such name!");
      case IS_MAIN:
        throw QueryRuntimeException(
            "Alive main instance can't be unregistered! Shut it down to trigger failover and then unregister it!");
      case NOT_COORDINATOR:
        throw QueryRuntimeException("UNREGISTER INSTANCE query can only be run on a coordinator!");
      case NOT_LEADER: {
        auto const maybe_leader_coordinator = coordinator_handler_.GetLeaderCoordinatorData();
        auto const *common_message = "Couldn't unregister replica instance since coordinator is not a leader!";
        if (maybe_leader_coordinator) {
          throw QueryRuntimeException("{} Current leader is coordinator with id {} with bolt socket address {}",
                                      common_message, maybe_leader_coordinator->coordinator_id,
                                      maybe_leader_coordinator->bolt_server.SocketAddress());
        }
        throw QueryRuntimeException(
            "{} Try contacting other coordinators as there might be leader election happening or other coordinators "
            "are down.",
            common_message);
      }
      case RAFT_LOG_ERROR:
        throw QueryRuntimeException("Couldn't unregister replica instance since raft server couldn't append the log!");
      case RPC_FAILED:
        throw QueryRuntimeException(
            "Couldn't unregister replica instance because current main instance couldn't unregister replica!");
      case LOCK_OPENED:
        throw QueryRuntimeException("Couldn't unregister replica because the last action didn't finish successfully!");
      case FAILED_TO_OPEN_LOCK:
        throw QueryRuntimeException("Couldn't unregister instance as cluster didn't accept start of action!");
      case FAILED_TO_CLOSE_LOCK:
        throw QueryRuntimeException(
            "Couldn't unregister instance as cluster didn't accept successful finish of action!");
      case SUCCESS:
        break;
    }
  }

  void DemoteInstanceToReplica(std::string_view instance_name) override {
    auto status = coordinator_handler_.DemoteInstanceToReplica(instance_name);
    switch (status) {
      using enum memgraph::coordination::DemoteInstanceCoordinatorStatus;
      case NO_INSTANCE_WITH_NAME:
        throw QueryRuntimeException("No instance with such name!");
      case NOT_COORDINATOR:
        throw QueryRuntimeException("DEMOTE INSTANCE query can only be run on a coordinator!");
      case NOT_LEADER: {
        auto const maybe_leader_coordinator = coordinator_handler_.GetLeaderCoordinatorData();
        auto const *common_message = "Couldn't demote instance to replica since coordinator is not a leader!";
        if (maybe_leader_coordinator) {
          throw QueryRuntimeException("{} Current leader is coordinator with id {} with bolt socket address {}",
                                      common_message, maybe_leader_coordinator->coordinator_id,
                                      maybe_leader_coordinator->bolt_server.SocketAddress());
        }
        throw QueryRuntimeException(
            "{} Try contacting other coordinators as there might be leader election happening or other coordinators "
            "are down.",
            common_message);
      }
      case RAFT_LOG_ERROR:
        throw QueryRuntimeException(
            "Couldn't demote instance to replica since raft server couldn't append the log. "
            "Coordinator may not be leader anymore!");
      case RPC_FAILED:
        throw QueryRuntimeException(
            "Couldn't demote instance to replica because current main instance couldn't unregister replica!");
      case LOCK_OPENED:
        throw QueryRuntimeException(
            "Couldn't successfully finish action demote instance to replica because the last action didn't finish "
            "successfully!");
      case FAILED_TO_OPEN_LOCK:
        throw QueryRuntimeException("Couldn't demote instance to replica as cluster didn't accept start of action!");
      case FAILED_TO_CLOSE_LOCK:
        throw QueryRuntimeException(
            "Couldn't demote  instance to replica as cluster didn't accept successful finish of action!");
      case SUCCESS:
        break;
    }
  }

  void ForceResetClusterState() override {
    auto status = coordinator_handler_.ForceResetClusterState();
    using enum memgraph::coordination::ReconcileClusterStateStatus;
    switch (status) {
      case SHUTTING_DOWN:
        throw QueryRuntimeException("Couldn't finish force reset as cluster is shutting down!");
      case FAIL:
        throw QueryRuntimeException("Force reset failed, check logs for more details!");
      case SUCCESS:
        break;
    }
  }

  void RegisterReplicationInstance(std::string_view bolt_server, std::string_view management_server,
                                   std::string_view replication_server,
                                   std::chrono::seconds const &instance_check_frequency,
                                   std::chrono::seconds const &instance_down_timeout,
                                   std::chrono::seconds const &instance_get_uuid_frequency,
                                   std::string_view instance_name, CoordinatorQuery::SyncMode sync_mode) override {
    auto const maybe_bolt_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(bolt_server);
    if (!maybe_bolt_server) {
      throw QueryRuntimeException("Invalid bolt socket address!");
    }

    auto const maybe_management_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(management_server);
    if (!maybe_management_server) {
      throw QueryRuntimeException("Invalid management socket address!");
    }

    auto const maybe_replication_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(replication_server);
    if (!maybe_replication_server) {
      throw QueryRuntimeException("Invalid replication socket address!");
    }

    auto const repl_config =
        coordination::ReplicationClientInfo{.instance_name = std::string(instance_name),
                                            .replication_mode = convertFromCoordinatorToReplicationMode(sync_mode),
                                            .replication_server = *maybe_replication_server};

    auto coordinator_client_config =
        coordination::CoordinatorToReplicaConfig{.instance_name = std::string(instance_name),
                                                 .mgt_server = *maybe_management_server,
                                                 .bolt_server = *maybe_bolt_server,
                                                 .replication_client_info = repl_config,
                                                 .instance_health_check_frequency_sec = instance_check_frequency,
                                                 .instance_down_timeout_sec = instance_down_timeout,
                                                 .instance_get_uuid_frequency_sec = instance_get_uuid_frequency,
                                                 .ssl = std::nullopt};

    auto status = coordinator_handler_.RegisterReplicationInstance(coordinator_client_config);
    switch (status) {
      using enum memgraph::coordination::RegisterInstanceCoordinatorStatus;
      case NAME_EXISTS:
        throw QueryRuntimeException("Couldn't register replica instance since instance with such name already exists!");
      case MGMT_ENDPOINT_EXISTS:
        throw QueryRuntimeException(
            "Couldn't register replica instance since instance with such management server already exists!");
      case REPL_ENDPOINT_EXISTS:
        throw QueryRuntimeException(
            "Couldn't register replica instance since instance with such replication endpoint already exists!");
      case NOT_COORDINATOR:
        throw QueryRuntimeException("REGISTER INSTANCE query can only be run on a coordinator!");
      case NOT_LEADER: {
        auto const maybe_leader_coordinator = coordinator_handler_.GetLeaderCoordinatorData();
        auto const *common_message = "Couldn't register replica instance since coordinator is not a leader!";
        if (maybe_leader_coordinator) {
          throw QueryRuntimeException("{} Current leader is coordinator with id {} with bolt socket address {}",
                                      common_message, maybe_leader_coordinator->coordinator_id,
                                      maybe_leader_coordinator->bolt_server.SocketAddress());
        }
        throw QueryRuntimeException(
            "{} Try contacting other coordinators as there might be leader election happening or other coordinators "
            "are down.",
            common_message);
      }
      case RAFT_LOG_ERROR:
        throw QueryRuntimeException("Couldn't register replica instance since raft server couldn't append the log!");
      case RPC_FAILED:
        throw QueryRuntimeException(
            "Couldn't register replica instance because setting instance to replica failed! Check logs on replica to "
            "find out more info!");
      case LOCK_OPENED:
        throw QueryRuntimeException(
            "Couldn't register replica instance because because the last action didn't finish successfully!");
      case FAILED_TO_OPEN_LOCK:
        throw QueryRuntimeException("Couldn't register instance as cluster didn't accept start of action!");
      case FAILED_TO_CLOSE_LOCK:
        throw QueryRuntimeException("Couldn't register instance as cluster didn't accept successful finish of action!");
      case SUCCESS:
        break;
    }
  }

  auto AddCoordinatorInstance(uint32_t coordinator_id, std::string_view bolt_server,
                              std::string_view coordinator_server, std::string_view management_server)
      -> void override {
    auto const maybe_coordinator_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(coordinator_server);
    if (!maybe_coordinator_server) {
      throw QueryRuntimeException("Invalid coordinator socket address!");
    }

    auto const maybe_management_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(management_server);
    if (!maybe_management_server) {
      throw QueryRuntimeException("Invalid management socket address!");
    }

    auto const maybe_bolt_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(bolt_server);
    if (!maybe_bolt_server) {
      throw QueryRuntimeException("Invalid bolt socket address!");
    }

    auto const coord_coord_config =
        coordination::CoordinatorToCoordinatorConfig{.coordinator_id = coordinator_id,
                                                     .bolt_server = *maybe_bolt_server,
                                                     .coordinator_server = *maybe_coordinator_server,
                                                     .management_server = *maybe_management_server

        };

    auto const status = coordinator_handler_.AddCoordinatorInstance(coord_coord_config);
    switch (status) {
      using enum memgraph::coordination::AddCoordinatorInstanceStatus;  // NOLINT
      case ID_ALREADY_EXISTS:
        throw QueryRuntimeException("Couldn't add coordinator since instance with such id already exists!");
      case BOLT_ENDPOINT_ALREADY_EXISTS:
        throw QueryRuntimeException("Couldn't add coordinator since instance with such bolt endpoint already exists!");
      case COORDINATOR_ENDPOINT_ALREADY_EXISTS:
        throw QueryRuntimeException(
            "Couldn't add coordinator since instance with such coordinator server already exists!");
      case SUCCESS:
        break;
    }
    spdlog::info("Added instance on coordinator server {}", maybe_coordinator_server->SocketAddress());
  }

  void SetReplicationInstanceToMain(std::string_view instance_name) override {
    auto const status = coordinator_handler_.SetReplicationInstanceToMain(instance_name);
    switch (status) {
      using enum memgraph::coordination::SetInstanceToMainCoordinatorStatus;
      case NO_INSTANCE_WITH_NAME:
        throw QueryRuntimeException("No instance with such name!");
      case MAIN_ALREADY_EXISTS:
        throw QueryRuntimeException("Couldn't set instance to main since there is already a main instance in cluster!");
      case NOT_COORDINATOR:
        throw QueryRuntimeException("SET INSTANCE TO MAIN query can only be run on a coordinator!");
      case NOT_LEADER: {
        auto const maybe_leader_coordinator = coordinator_handler_.GetLeaderCoordinatorData();
        auto const *common_message = "Couldn't set instance to main since coordinator is not a leader!";
        if (maybe_leader_coordinator) {
          throw QueryRuntimeException("{} Current leader is coordinator with id {} with bolt socket address {}",
                                      common_message, maybe_leader_coordinator->coordinator_id,
                                      maybe_leader_coordinator->bolt_server.SocketAddress());
        }
        throw QueryRuntimeException(
            "{} Try contacting other coordinators as there might be leader election happening or other coordinators "
            "are down.",
            common_message);
      }
      case RAFT_LOG_ERROR:
        throw QueryRuntimeException("Couldn't promote instance since raft server couldn't append the log!");
      case COULD_NOT_PROMOTE_TO_MAIN:
        throw QueryRuntimeException(
            "Couldn't set replica instance to main! Check coordinator and replica for more logs");
      case SWAP_UUID_FAILED:
        throw QueryRuntimeException("Couldn't set replica instance to main. Replicas didn't swap uuid of new main.");
      case FAILED_TO_OPEN_LOCK:
        throw QueryRuntimeException("Couldn't set instance to main as cluster didn't accept start of action!");
      case LOCK_OPENED:
        throw QueryRuntimeException(
            "Couldn't set instance to main instance because because the last action didn't finish successfully!");
      case ENABLE_WRITING_FAILED:
        throw QueryRuntimeException("Instance promoted to MAIN, but couldn't enable writing to instance.");
      case FAILED_TO_CLOSE_LOCK:
        throw QueryRuntimeException(
            "Couldn't set instance to main as cluster didn't accept successful finish of action!");
      case SUCCESS:
        break;
    }
  }

  [[nodiscard]] std::vector<coordination::InstanceStatus> ShowInstances() const override {
    return coordinator_handler_.ShowInstances();
  }

 private:
  dbms::CoordinatorHandler coordinator_handler_;
};
#endif

/// returns false if the replication role can't be set
/// @throw QueryRuntimeException if an error occurred.

Callback HandleAuthQuery(AuthQuery *auth_query, InterpreterContext *interpreter_context, const Parameters &parameters,
                         Interpreter &interpreter) {
  AuthQueryHandler *auth = interpreter_context->auth;
#ifdef MG_ENTERPRISE
  auto *db_handler = interpreter_context->dbms_handler;
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
  const bool if_not_exists = auth_query->if_not_exists_;
  std::string database = auth_query->database_;
  std::vector<AuthQuery::Privilege> privileges = auth_query->privileges_;
#ifdef MG_ENTERPRISE
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> label_privileges =
      auth_query->label_privileges_;
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges =
      auth_query->edge_type_privileges_;
#endif
  auto password = EvaluateOptionalExpression(auth_query->password_, evaluator);

  auto oldPassword = EvaluateOptionalExpression(auth_query->old_password_, evaluator);
  auto newPassword = EvaluateOptionalExpression(auth_query->new_password_, evaluator);

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
                                                          AuthQuery::Action::DENY_DATABASE_FROM_USER,
                                                          AuthQuery::Action::REVOKE_DATABASE_FROM_USER,
                                                          AuthQuery::Action::SHOW_DATABASE_PRIVILEGES,
                                                          AuthQuery::Action::SET_MAIN_DATABASE};

  if (license_check_result.HasError() && enterprise_only_methods.contains(auth_query->action_)) {
    throw utils::BasicException(
        license::LicenseCheckErrorToString(license_check_result.GetError(), "advanced authentication features"));
  }

  const auto forbid_on_replica = [has_license = license_check_result.HasError(),
                                  is_replica = interpreter_context->repl_state->IsReplica()]() {
    if (is_replica) {
#if MG_ENTERPRISE
      if (has_license) {
        throw QueryException(
            "Query forbidden on the replica! Update on MAIN, as it is the only source of truth for authentication "
            "data. MAIN will then replicate the update to connected REPLICAs");
      }
      throw QueryException(
          "Query forbidden on the replica! Switch role to MAIN and update user data, then switch back to REPLICA.");
#else
      throw QueryException(
          "Query forbidden on the replica! Switch role to MAIN and update user data, then switch back to REPLICA.");
#endif
    }
  };

  switch (auth_query->action_) {
    case AuthQuery::Action::CREATE_USER:
      forbid_on_replica();
      callback.fn = [auth, username, password, if_not_exists,
                     valid_enterprise_license = !license_check_result.HasError(), interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        MG_ASSERT(password.IsString() || password.IsNull());
        if (!auth->CreateUser(
                username, password.IsString() ? std::make_optional(std::string(password.ValueString())) : std::nullopt,
                &*interpreter->system_transaction_)) {
          if (!if_not_exists) {
            throw UserAlreadyExistsException("User '{}' already exists.", username);
          }
          spdlog::warn("User '{}' already exists.", username);
        }

        // If the license is not valid we create users with admin access
        if (!valid_enterprise_license) {
          spdlog::warn("Granting all the privileges to {}.", username);
          auth->GrantPrivilege(
              username, kPrivilegesAll
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
              ,
              &*interpreter->system_transaction_);
        }

        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_USER:
      forbid_on_replica();
      callback.fn = [auth, username, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        if (!auth->DropUser(username, &*interpreter->system_transaction_)) {
          throw QueryRuntimeException("User '{}' doesn't exist.", username);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SET_PASSWORD:
      forbid_on_replica();
      callback.fn = [auth, username, password, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        MG_ASSERT(password.IsString() || password.IsNull());
        auth->SetPassword(username,
                          password.IsString() ? std::make_optional(std::string(password.ValueString())) : std::nullopt,
                          &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CHANGE_PASSWORD:
      forbid_on_replica();
      callback.fn = [auth, username, oldPassword, newPassword, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }
        const std::optional<std::string> username = interpreter->user_or_role_->username();
        if (!username) {
          throw QueryException("You need to be valid user to replace password");
        }

        MG_ASSERT(newPassword.IsString() || newPassword.IsNull());
        MG_ASSERT(oldPassword.IsString() || oldPassword.IsNull());
        auth->ChangePassword(
            *username,
            oldPassword.IsString() ? std::make_optional(std::string(oldPassword.ValueString())) : std::nullopt,
            newPassword.IsString() ? std::make_optional(std::string(newPassword.ValueString())) : std::nullopt,
            &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CREATE_ROLE:
      forbid_on_replica();
      callback.fn = [auth, rolename, if_not_exists, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        if (!auth->CreateRole(rolename, &*interpreter->system_transaction_)) {
          if (!if_not_exists) {
            throw QueryRuntimeException("Role '{}' already exists.", rolename);
          }
          spdlog::warn("Role '{}' already exists.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_ROLE:
      forbid_on_replica();
      callback.fn = [auth, rolename, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        if (!auth->DropRole(rolename, &*interpreter->system_transaction_)) {
          throw QueryRuntimeException("Role '{}' doesn't exist.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::SHOW_CURRENT_USER:
      callback.header = {"user"};
      callback.fn = [&interpreter] {
        const auto &username = interpreter.user_or_role_->username();
        return std::vector<std::vector<TypedValue>>{
            {username.has_value() ? TypedValue(username.value()) : TypedValue()}};
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
      forbid_on_replica();
      callback.fn = [auth, username, rolename, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        auth->SetRole(username, rolename, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CLEAR_ROLE:
      forbid_on_replica();
      callback.fn = [auth, username, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        auth->ClearRole(username, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::GRANT_PRIVILEGE:
      forbid_on_replica();
      callback.fn = [auth, user_or_role, privileges, interpreter = &interpreter
#ifdef MG_ENTERPRISE
                     ,
                     label_privileges, edge_type_privileges
#endif
      ] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        auth->GrantPrivilege(user_or_role, privileges
#ifdef MG_ENTERPRISE
                             ,
                             label_privileges, edge_type_privileges
#endif
                             ,
                             &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DENY_PRIVILEGE:
      forbid_on_replica();
      callback.fn = [auth, user_or_role, privileges, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        auth->DenyPrivilege(user_or_role, privileges, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::REVOKE_PRIVILEGE: {
      forbid_on_replica();
      callback.fn = [auth, user_or_role, privileges, interpreter = &interpreter
#ifdef MG_ENTERPRISE
                     ,
                     label_privileges, edge_type_privileges
#endif
      ] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        auth->RevokePrivilege(user_or_role, privileges
#ifdef MG_ENTERPRISE
                              ,
                              label_privileges, edge_type_privileges
#endif
                              ,
                              &*interpreter->system_transaction_);
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
      forbid_on_replica();
#ifdef MG_ENTERPRISE
      callback.fn = [auth, database, username, db_handler, interpreter = &interpreter] {  // NOLINT
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        try {
          std::optional<memgraph::dbms::DatabaseAccess> db =
              std::nullopt;  // Hold pointer to database to protect it until query is done
          if (database != memgraph::auth::kAllDatabases) {
            db = db_handler->Get(database);  // Will throw if databases doesn't exist and protect it during pull
          }
          auth->GrantDatabase(database, username, &*interpreter->system_transaction_);  // Can throws query exception
        } catch (memgraph::dbms::UnknownDatabaseException &e) {
          throw QueryRuntimeException(e.what());
        }
#else
      callback.fn = [] {
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DENY_DATABASE_FROM_USER:
      forbid_on_replica();
#ifdef MG_ENTERPRISE
      callback.fn = [auth, database, username, db_handler, interpreter = &interpreter] {  // NOLINT
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        try {
          std::optional<memgraph::dbms::DatabaseAccess> db =
              std::nullopt;  // Hold pointer to database to protect it until query is done
          if (database != memgraph::auth::kAllDatabases) {
            db = db_handler->Get(database);  // Will throw if databases doesn't exist and protect it during pull
          }
          auth->DenyDatabase(database, username, &*interpreter->system_transaction_);  // Can throws query exception
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
      forbid_on_replica();
#ifdef MG_ENTERPRISE
      callback.fn = [auth, database, username, db_handler, interpreter = &interpreter] {  // NOLINT
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        try {
          std::optional<memgraph::dbms::DatabaseAccess> db =
              std::nullopt;  // Hold pointer to database to protect it until query is done
          if (database != memgraph::auth::kAllDatabases) {
            db = db_handler->Get(database);  // Will throw if databases doesn't exist and protect it during pull
          }
          auth->RevokeDatabase(database, username, &*interpreter->system_transaction_);  // Can throws query exception
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
      forbid_on_replica();
#ifdef MG_ENTERPRISE
      callback.fn = [auth, database, username, db_handler, interpreter = &interpreter] {  // NOLINT
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        try {
          const auto db =
              db_handler->Get(database);  // Will throw if databases doesn't exist and protect it during pull
          auth->SetMainDatabase(database, username, &*interpreter->system_transaction_);  // Can throws query exception
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

Callback HandleReplicationQuery(ReplicationQuery *repl_query, const Parameters &parameters,
                                ReplicationQueryHandler &replication_query_handler,
                                const query::InterpreterConfig &config, std::vector<Notification> *notifications
#ifdef MG_ENTERPRISE
                                ,
                                std::optional<std::reference_wrapper<coordination::CoordinatorState>> coordinator_state
#endif
) {
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
  auto const is_managed_by_coordinator = [&]() {
#ifdef MG_ENTERPRISE
    return coordinator_state.has_value() && coordinator_state->get().IsDataInstance();
#else
    return false;
#endif
  }();  // iile
  Callback callback;
  switch (repl_query->action_) {
    case ReplicationQuery::Action::SET_REPLICATION_ROLE: {
#ifdef MG_ENTERPRISE
      if (is_managed_by_coordinator) {
        throw QueryRuntimeException("Can't set role manually on instance with coordinator server port.");
      }
#endif

      auto port = EvaluateOptionalExpression(repl_query->port_, evaluator);
      std::optional<int64_t> maybe_port;
      if (port.IsInt()) {
        maybe_port = port.ValueInt();
      }
      if (maybe_port == 7687 && repl_query->role_ == ReplicationQuery::ReplicationRole::REPLICA) {
        notifications->emplace_back(SeverityLevel::WARNING, NotificationCode::REPLICA_PORT_WARNING,
                                    "Be careful the replication port must be different from the memgraph port!");
      }
      callback.fn = [handler = ReplQueryHandler{replication_query_handler}, role = repl_query->role_,
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
      callback.fn = [handler = ReplQueryHandler{replication_query_handler}] {
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
#ifdef MG_ENTERPRISE
      if (is_managed_by_coordinator) {
        throw QueryRuntimeException("Can't register replica manually on instance with coordinator server port.");
      }
#endif
      const auto &name = repl_query->instance_name_;
      const auto &sync_mode = repl_query->sync_mode_;
      auto socket_address = repl_query->socket_address_->Accept(evaluator);
      const auto replica_check_frequency = config.replication_replica_check_frequency;

      callback.fn = [handler = ReplQueryHandler{replication_query_handler}, name, socket_address, sync_mode,
                     replica_check_frequency]() mutable {
        handler.RegisterReplica(name, std::string(socket_address.ValueString()), sync_mode, replica_check_frequency);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::REGISTER_REPLICA,
                                  fmt::format("Replica {} is registered.", repl_query->instance_name_));
      return callback;
    }

    case ReplicationQuery::Action::DROP_REPLICA: {
#ifdef MG_ENTERPRISE
      if (is_managed_by_coordinator) {
        throw QueryRuntimeException("Can't drop replica manually on instance with coordinator server port.");
      }
#endif
      const auto &name = repl_query->instance_name_;
      callback.fn = [handler = ReplQueryHandler{replication_query_handler}, name]() mutable {
        handler.DropReplica(name);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::DROP_REPLICA,
                                  fmt::format("Replica {} is dropped.", repl_query->instance_name_));
      return callback;
    }
    case ReplicationQuery::Action::SHOW_REPLICAS: {
      bool full_info = false;
#ifdef MG_ENTERPRISE
      full_info = license::global_license_checker.IsEnterpriseValidFast();
#endif

      callback.header = {"name", "socket_address", "sync_mode", "system_info", "data_info"};

      callback.fn = [handler = ReplQueryHandler{replication_query_handler}, replica_nfields = callback.header.size(),
                     full_info] {
        auto const sync_mode_to_tv = [](memgraph::replication_coordination_glue::ReplicationMode sync_mode) {
          using namespace std::string_view_literals;
          switch (sync_mode) {
            using enum memgraph::replication_coordination_glue::ReplicationMode;
            case SYNC:
              return TypedValue{"sync"sv};
            case ASYNC:
              return TypedValue{"async"sv};
          }
        };

        auto const replica_sys_state_to_tv = [](memgraph::replication::ReplicationClient::State state) {
          using namespace std::string_view_literals;
          switch (state) {
            using enum memgraph::replication::ReplicationClient::State;
            case BEHIND:
              return TypedValue{"invalid"sv};
            case READY:
              return TypedValue{"ready"sv};
            case RECOVERY:
              return TypedValue{"recovery"sv};
          }
        };

        auto const sys_info_to_tv = [&](ReplicaSystemInfoState orig) {
          auto info = std::map<std::string, TypedValue>{};
          info.emplace("ts", TypedValue{static_cast<int64_t>(orig.ts_)});
          // TODO: behind not implemented
          info.emplace("behind", TypedValue{/* static_cast<int64_t>(orig.behind_) */});
          info.emplace("status", replica_sys_state_to_tv(orig.state_));
          return TypedValue{std::move(info)};
        };

        auto const replica_state_to_tv = [](memgraph::storage::replication::ReplicaState state) {
          using namespace std::string_view_literals;
          switch (state) {
            using enum memgraph::storage::replication::ReplicaState;
            case READY:
              return TypedValue{"ready"sv};
            case REPLICATING:
              return TypedValue{"replicating"sv};
            case RECOVERY:
              return TypedValue{"recovery"sv};
            case MAYBE_BEHIND:
              return TypedValue{"invalid"sv};
            case DIVERGED_FROM_MAIN:
              return TypedValue{"diverged"sv};
          }
        };

        auto const info_to_tv = [&](ReplicaInfoState orig) {
          auto info = std::map<std::string, TypedValue>{};
          info.emplace("ts", TypedValue{static_cast<int64_t>(orig.ts_)});
          info.emplace("behind", TypedValue{static_cast<int64_t>(orig.behind_)});
          info.emplace("status", replica_state_to_tv(orig.state_));
          return TypedValue{std::move(info)};
        };

        auto const data_info_to_tv = [&](std::map<std::string, ReplicaInfoState> orig) {
          auto data_info = std::map<std::string, TypedValue>{};
          for (auto &[name, info] : orig) {
            data_info.emplace(name, info_to_tv(info));
          }
          return TypedValue{std::move(data_info)};
        };

        auto replicas = handler.ShowReplicas();
        auto typed_replicas = std::vector<std::vector<TypedValue>>{};
        typed_replicas.reserve(replicas.size());
        for (auto &replica : replicas) {
          std::vector<TypedValue> typed_replica;
          typed_replica.reserve(replica_nfields);

          typed_replica.emplace_back(replica.name_);
          typed_replica.emplace_back(replica.socket_address_);
          typed_replica.emplace_back(sync_mode_to_tv(replica.sync_mode_));
          if (full_info) {
            typed_replica.emplace_back(sys_info_to_tv(replica.system_info_));
          } else {
            // Set to NULL
            typed_replica.emplace_back(TypedValue{});
          }
          typed_replica.emplace_back(data_info_to_tv(replica.data_info_));

          typed_replicas.emplace_back(std::move(typed_replica));
        }
        return typed_replicas;
      };
      return callback;
    }
  }
}

#ifdef MG_ENTERPRISE

auto ParseConfigMap(std::unordered_map<Expression *, Expression *> const &config_map,
                    ExpressionVisitor<TypedValue> &evaluator)
    -> std::optional<std::map<std::string, std::string, std::less<>>> {
  if (std::ranges::any_of(config_map, [&evaluator](const auto &entry) {
        auto key_expr = entry.first->Accept(evaluator);
        auto value_expr = entry.second->Accept(evaluator);
        return !key_expr.IsString() || !value_expr.IsString();
      })) {
    spdlog::error("Config map must contain only string keys and values!");
    return std::nullopt;
  }

  return ranges::views::all(config_map) | ranges::views::transform([&evaluator](const auto &entry) {
           auto key_expr = entry.first->Accept(evaluator);
           auto value_expr = entry.second->Accept(evaluator);
           return std::pair{key_expr.ValueString(), value_expr.ValueString()};
         }) |
         ranges::to<std::map<std::string, std::string, std::less<>>>;
}

Callback HandleCoordinatorQuery(CoordinatorQuery *coordinator_query, const Parameters &parameters,
                                coordination::CoordinatorState *coordinator_state,
                                const query::InterpreterConfig &config, std::vector<Notification> *notifications) {
  using enum memgraph::flags::Experiments;

  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryRuntimeException("High availability is only available in Memgraph Enterprise.");
  }

  if (!flags::AreExperimentsEnabled(HIGH_AVAILABILITY)) {
    throw QueryRuntimeException(
        "High availability is experimental feature. If you want to use it, add high-availability option to the "
        "--experimental-enabled flag.");
  }

  Callback callback;
  switch (coordinator_query->action_) {
    case CoordinatorQuery::Action::ADD_COORDINATOR_INSTANCE: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can add coordinator instance!");
      }

      // TODO: MemoryResource for EvaluationContext, it should probably be passed as
      // the argument to Callback.
      EvaluationContext evaluation_context{.timestamp = QueryTimestamp(), .parameters = parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

      auto config_map = ParseConfigMap(coordinator_query->configs_, evaluator);
      if (!config_map) {
        throw QueryRuntimeException("Failed to parse config map!");
      }

      if (config_map->size() != 3) {
        throw QueryRuntimeException("Config map must contain exactly 3 entries: {}, {} and  {}!", kCoordinatorServer,
                                    kBoltServer, kManagementServer);
      }

      auto const &coordinator_server_it = config_map->find(kCoordinatorServer);
      if (coordinator_server_it == config_map->end()) {
        throw QueryRuntimeException("Config map must contain {} entry!", kCoordinatorServer);
      }

      auto const &bolt_server_it = config_map->find(kBoltServer);
      if (bolt_server_it == config_map->end()) {
        throw QueryRuntimeException("Config map must contain {} entry!", kBoltServer);
      }

      auto const &management_server_it = config_map->find(kManagementServer);
      if (management_server_it == config_map->end()) {
        throw QueryRuntimeException("Config map must contain {} entry!", kManagementServer);
      }

      auto coord_server_id = coordinator_query->coordinator_id_->Accept(evaluator).ValueInt();

      callback.fn = [handler = CoordQueryHandler{*coordinator_state}, coord_server_id,
                     bolt_server = bolt_server_it->second, coordinator_server = coordinator_server_it->second,
                     management_server = management_server_it->second]() mutable {
        handler.AddCoordinatorInstance(coord_server_id, bolt_server, coordinator_server, management_server);
        return std::vector<std::vector<TypedValue>>();
      };

      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::ADD_COORDINATOR_INSTANCE,
                                  fmt::format("Coordinator has added instance {} on coordinator server {}.",
                                              coordinator_query->instance_name_, coordinator_server_it->second));
      return callback;
    }
    case CoordinatorQuery::Action::REGISTER_INSTANCE: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can register coordinator server!");
      }
      // TODO: MemoryResource for EvaluationContext, it should probably be passed as
      // the argument to Callback.
      EvaluationContext evaluation_context{.timestamp = QueryTimestamp(), .parameters = parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
      auto config_map = ParseConfigMap(coordinator_query->configs_, evaluator);

      if (!config_map) {
        throw QueryRuntimeException("Failed to parse config map!");
      }

      if (config_map->size() != 3) {
        throw QueryRuntimeException("Config map must contain exactly 3 entries: {}, {} and {}!", kBoltServer,
                                    kManagementServer, kReplicationServer);
      }

      auto const &replication_server_it = config_map->find(kReplicationServer);
      if (replication_server_it == config_map->end()) {
        throw QueryRuntimeException("Config map must contain {} entry!", kReplicationServer);
      }

      auto const &management_server_it = config_map->find(kManagementServer);
      if (management_server_it == config_map->end()) {
        throw QueryRuntimeException("Config map must contain {} entry!", kManagementServer);
      }

      auto const &bolt_server_it = config_map->find(kBoltServer);
      if (bolt_server_it == config_map->end()) {
        throw QueryRuntimeException("Config map must contain {} entry!", kBoltServer);
      }

      callback.fn = [handler = CoordQueryHandler{*coordinator_state},
                     instance_health_check_frequency_sec = config.instance_health_check_frequency_sec,
                     bolt_server = bolt_server_it->second, management_server = management_server_it->second,
                     replication_server = replication_server_it->second,
                     instance_name = coordinator_query->instance_name_,
                     instance_down_timeout_sec = config.instance_down_timeout_sec,
                     instance_get_uuid_frequency_sec = config.instance_get_uuid_frequency_sec,
                     sync_mode = coordinator_query->sync_mode_]() mutable {
        handler.RegisterReplicationInstance(bolt_server, management_server, replication_server,
                                            instance_health_check_frequency_sec, instance_down_timeout_sec,
                                            instance_get_uuid_frequency_sec, instance_name, sync_mode);
        return std::vector<std::vector<TypedValue>>();
      };

      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::REGISTER_REPLICATION_INSTANCE,
                                  fmt::format("Coordinator has registered replication instance on {} for instance {}.",
                                              bolt_server_it->second, coordinator_query->instance_name_));
      return callback;
    }
    case CoordinatorQuery::Action::UNREGISTER_INSTANCE:
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can unregister instance!");
      }
      callback.fn = [handler = CoordQueryHandler{*coordinator_state},
                     instance_name = coordinator_query->instance_name_]() mutable {
        handler.UnregisterInstance(instance_name);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(
          SeverityLevel::INFO, NotificationCode::UNREGISTER_INSTANCE,
          fmt::format("Coordinator has unregistered instance {}.", coordinator_query->instance_name_));

      return callback;

    case CoordinatorQuery::Action::DEMOTE_INSTANCE:
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can demote instance!");
      }
      callback.fn = [handler = CoordQueryHandler{*coordinator_state},
                     instance_name = coordinator_query->instance_name_]() mutable {
        handler.DemoteInstanceToReplica(instance_name);
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(
          SeverityLevel::INFO, NotificationCode::DEMOTE_INSTANCE_TO_REPLICA,
          fmt::format("Coordinator has demoted instance to replica {}.", coordinator_query->instance_name_));

      return callback;

    case CoordinatorQuery::Action::FORCE_RESET_CLUSTER_STATE:
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can force reset cluster!");
      }
      callback.fn = [handler = CoordQueryHandler{*coordinator_state}]() mutable {
        handler.ForceResetClusterState();
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::FORCE_RESET_CLUSTER_STATE,
                                  fmt::format("Coordinator has force reset cluster state."));

      return callback;

    case CoordinatorQuery::Action::SET_INSTANCE_TO_MAIN: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can register coordinator server!");
      }
      // TODO: MemoryResource for EvaluationContext, it should probably be passed as
      // the argument to Callback.
      EvaluationContext evaluation_context{.timestamp = QueryTimestamp(), .parameters = parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

      callback.fn = [handler = CoordQueryHandler{*coordinator_state},
                     instance_name = coordinator_query->instance_name_]() mutable {
        handler.SetReplicationInstanceToMain(instance_name);
        return std::vector<std::vector<TypedValue>>();
      };

      return callback;
    }
    case CoordinatorQuery::Action::SHOW_INSTANCES: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can run SHOW INSTANCES.");
      }

      callback.header = {"name",   "bolt_server", "coordinator_server", "management_server",
                         "health", "role",        "last_succ_resp_ms"};
      callback.fn = [handler = CoordQueryHandler{*coordinator_state},
                     replica_nfields = callback.header.size()]() mutable {
        auto const instances = handler.ShowInstances();
        auto const converter = [](const auto &status) -> std::vector<TypedValue> {
          return {TypedValue{status.instance_name},
                  TypedValue{status.bolt_server},
                  TypedValue{status.coordinator_server},
                  TypedValue{status.management_server},
                  TypedValue{status.health},
                  TypedValue{status.cluster_role},
                  TypedValue{status.last_succ_resp_ms}};
        };

        return utils::fmap(instances, converter);
      };
      return callback;
    }
  }
}
#endif

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
                                                  std::shared_ptr<QueryUserOrRole> user_or_role) {
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

  // Make a copy of the user and pass it to the subsystem
  auto owner = interpreter_context->auth_checker->GenQueryUser(user_or_role->username(), user_or_role->rolename());

  return [db_acc = std::move(db_acc), interpreter_context, stream_name = stream_query->stream_name_,
          topic_names = EvaluateTopicNames(evaluator, stream_query->topic_names_),
          consumer_group = std::move(consumer_group), common_stream_info = std::move(common_stream_info),
          bootstrap_servers = std::move(bootstrap), owner = std::move(owner),
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
                                                   std::shared_ptr<QueryUserOrRole> user_or_role) {
  auto service_url = GetOptionalStringValue(stream_query->service_url_, evaluator);
  if (service_url && service_url->empty()) {
    throw SemanticException("Service URL must not be an empty string!");
  }
  auto common_stream_info = GetCommonStreamInfo(stream_query, evaluator);
  memgraph::metrics::IncrementCounter(memgraph::metrics::StreamsCreated);

  // Make a copy of the user and pass it to the subsystem
  auto owner = interpreter_context->auth_checker->GenQueryUser(user_or_role->username(), user_or_role->rolename());

  return [db = std::move(db), interpreter_context, stream_name = stream_query->stream_name_,
          topic_names = EvaluateTopicNames(evaluator, stream_query->topic_names_),
          common_stream_info = std::move(common_stream_info), service_url = std::move(service_url),
          owner = std::move(owner),
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
                           std::shared_ptr<QueryUserOrRole> user_or_role, std::vector<Notification> *notifications) {
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
          callback.fn =
              GetKafkaCreateCallback(stream_query, evaluator, db_acc, interpreter_context, std::move(user_or_role));
          break;
        case StreamQuery::Type::PULSAR:
          callback.fn =
              GetPulsarCreateCallback(stream_query, evaluator, db_acc, interpreter_context, std::move(user_or_role));
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

        callback.fn = [db_acc, streams = db_acc->streams(), stream_name = stream_query->stream_name_, batch_limit,
                       timeout]() {
          if (db_acc.is_deleting()) {
            throw QueryException("Can not start stream while database is being dropped.");
          }
          streams->StartWithLimit(stream_name, static_cast<uint64_t>(batch_limit.value()), timeout);
          return std::vector<std::vector<TypedValue>>{};
        };
      } else {
        callback.fn = [db_acc, streams = db_acc->streams(), stream_name = stream_query->stream_name_]() {
          if (db_acc.is_deleting()) {
            throw QueryException("Can not start stream while database is being dropped.");
          }
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
  explicit PullPlan(std::shared_ptr<PlanWrapper> plan, const Parameters &parameters, bool is_profile_query,
                    DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                    std::shared_ptr<QueryUserOrRole> user_or_role, std::atomic<TransactionStatus> *transaction_status,
                    std::shared_ptr<utils::AsyncTimer> tx_timer, DatabaseAccessProtector db_acc,
                    std::optional<QueryLogger> &query_logger,
                    TriggerContextCollector *trigger_context_collector = nullptr,
                    std::optional<size_t> memory_limit = {}, FrameChangeCollector *frame_change_collector_ = nullptr,
                    std::optional<int64_t> hops_limit = {});

  std::optional<plan::ProfilingStatsWithTotalTime> Pull(AnyStream *stream, std::optional<int> n,
                                                        const std::vector<Symbol> &output_symbols,
                                                        std::map<std::string, TypedValue> *summary);

 private:
  std::shared_ptr<PlanWrapper> plan_ = nullptr;
  plan::UniqueCursorPtr cursor_ = nullptr;
  Frame frame_;
  ExecutionContext ctx_;
  std::optional<size_t> memory_limit_;
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  std::optional<QueryLogger> &query_logger_;

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

PullPlan::PullPlan(const std::shared_ptr<PlanWrapper> plan, const Parameters &parameters, const bool is_profile_query,
                   DbAccessor *dba, InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
                   std::shared_ptr<QueryUserOrRole> user_or_role, std::atomic<TransactionStatus> *transaction_status,
                   std::shared_ptr<utils::AsyncTimer> tx_timer, DatabaseAccessProtector db_acc,
                   std::optional<QueryLogger> &query_logger, TriggerContextCollector *trigger_context_collector,
                   const std::optional<size_t> memory_limit, FrameChangeCollector *frame_change_collector,
                   const std::optional<int64_t> hops_limit)
    : plan_(plan),
      cursor_(plan->plan().MakeCursor(execution_memory)),
      frame_(plan->symbol_table().max_position(), execution_memory),
      memory_limit_(memory_limit),
      query_logger_(query_logger) {
  ctx_.hops_limit = query::HopsLimit{hops_limit};
  ctx_.db_accessor = dba;
  ctx_.symbol_table = plan->symbol_table();
  ctx_.evaluation_context.timestamp = QueryTimestamp();
  ctx_.evaluation_context.parameters = parameters;
  ctx_.evaluation_context.properties = NamesToProperties(plan->ast_storage().properties_, dba);
  ctx_.evaluation_context.labels = NamesToLabels(plan->ast_storage().labels_, dba);
  ctx_.user_or_role = user_or_role;
#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast() && user_or_role && *user_or_role && dba) {
    // Create only if an explicit user is defined
    auto auth_checker = interpreter_context->auth_checker->GetFineGrainedAuthChecker(user_or_role, dba);

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
  ctx_.evaluation_context.memory = execution_memory;
  ctx_.db_acc = std::move(db_acc);
}

std::optional<plan::ProfilingStatsWithTotalTime> PullPlan::Pull(AnyStream *stream, std::optional<int> n,
                                                                const std::vector<Symbol> &output_symbols,
                                                                std::map<std::string, TypedValue> *summary) {
  std::optional<uint64_t> transaction_id = ctx_.db_accessor->GetTransactionId();
  MG_ASSERT(transaction_id.has_value());

  if (memory_limit_) {
    memgraph::memory::TryStartTrackingOnTransaction(*transaction_id, *memory_limit_);
    memgraph::memory::StartTrackingCurrentThreadTransaction(*transaction_id);
  }
  utils::OnScopeExit<std::function<void()>> reset_query_limit{
      [memory_limit = memory_limit_, transaction_id = *transaction_id]() {
        if (memory_limit) {
          // Stopping tracking of transaction occurs in interpreter::pull
          // Exception can occur so we need to handle that case there.
          // We can't stop tracking here as there can be multiple pulls
          // so we need to take care of that after everything was pulled
          memgraph::memory::StopTrackingCurrentThreadTransaction(transaction_id);
        }
      }};

  // Returns true if a result was pulled.
  const auto pull_result = [&]() -> bool { return cursor_->Pull(frame_, ctx_); };

  auto values = std::vector<TypedValue>(output_symbols.size());
  const auto stream_values = [&] {
    for (auto const i : ranges::views::iota(0UL, output_symbols.size())) {
      values[i] = frame_[output_symbols[i]];
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
  summary->insert_or_assign("number_of_hops", ctx_.number_of_hops);

  if (query_logger_) {
    query_logger_->trace(fmt::format("Query execution time: {}", execution_time_.count()));
  }

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
      auto key = ExecutionStatsKeyToString(ExecutionStats::Key(i));
      stats.emplace(key, ctx_.execution_stats.counters[i]);
      if (query_logger_) {
        query_logger_->trace(fmt::format("{}: {}", key, ctx_.execution_stats.counters[i]));
      }
    }
    summary->insert_or_assign("stats", std::move(stats));
  }
  cursor_->Shutdown();
  ctx_.profile_execution_time = execution_time_;

  if (!flags::run_time::GetHopsLimitPartialResults() && ctx_.hops_limit.IsLimitReached()) {
    throw QueryException("Query exceeded the maximum number of hops.");
  }

  auto stats_and_total_time = GetStatsWithTotalTime(ctx_);

  if (query_logger_) {
    query_logger_->trace(fmt::format("Profile plan\n{}", ProfilingStatsToJson(stats_and_total_time).dump()));
  }

  return stats_and_total_time;
}

using RWType = plan::ReadWriteTypeChecker::RWType;

bool IsQueryWrite(const query::plan::ReadWriteTypeChecker::RWType query_type) {
  return query_type == RWType::W || query_type == RWType::RW;
}

}  // namespace

Interpreter::Interpreter(InterpreterContext *interpreter_context) : interpreter_context_(interpreter_context) {
  MG_ASSERT(interpreter_context_, "Interpreter context must not be NULL");
}

Interpreter::Interpreter(InterpreterContext *interpreter_context, memgraph::dbms::DatabaseAccess db)
    : current_db_{std::move(db)}, interpreter_context_(interpreter_context) {
  MG_ASSERT(current_db_.db_acc_, "Database accessor needs to be valid");
  MG_ASSERT(interpreter_context_, "Interpreter context must not be NULL");
}

auto DetermineTxTimeout(std::optional<int64_t> tx_timeout_ms, InterpreterConfig const &config) -> TxTimeout {
  using double_seconds = std::chrono::duration<double>;

  auto const global_tx_timeout = double_seconds{flags::run_time::GetExecutionTimeout()};
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
  }
}

PreparedQuery PrepareCypherQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
                                 InterpreterContext *interpreter_context, CurrentDB &current_db,
                                 utils::MemoryResource *execution_memory, std::vector<Notification> *notifications,
                                 std::shared_ptr<QueryUserOrRole> user_or_role,
                                 std::atomic<TransactionStatus> *transaction_status,
                                 std::shared_ptr<utils::AsyncTimer> tx_timer, Interpreter &interpreter,
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

  const auto hops_limit = EvaluateHopsLimit(evaluator, cypher_query->pre_query_directives_.hops_limit_);
  if (hops_limit) {
    spdlog::debug("Running query with hops limit of {}", *hops_limit);
  }

  auto clauses = cypher_query->single_query_->clauses_;
  if (std::any_of(clauses.begin(), clauses.end(),
                  [](const auto *clause) { return clause->GetTypeInfo() == LoadCsv::kType; })) {
    notifications->emplace_back(
        SeverityLevel::INFO, NotificationCode::LOAD_CSV_TIP,
        "It's important to note that the parser parses the values as strings. It's up to the user to "
        "convert the parsed row values to the appropriate type. This can be done using the built-in "
        "conversion functions such as ToInteger, ToFloat, ToBoolean etc.");
  }

  MG_ASSERT(current_db.execution_db_accessor_, "Cypher query expects a current DB transaction");
  auto *dba =
      &*current_db
            .execution_db_accessor_;  // todo pass the full current_db into planner...make plan optimisation optional

  const auto is_cacheable = parsed_query.is_cacheable;
  auto *plan_cache = is_cacheable ? current_db.db_acc_->get()->plan_cache() : nullptr;

  auto plan = CypherQueryToPlan(parsed_query.stripped_query.hash(), std::move(parsed_query.ast_storage), cypher_query,
                                parsed_query.parameters, plan_cache, dba);

  auto hints = plan::ProvidePlanHints(&plan->plan(), plan->symbol_table());
  for (const auto &hint : hints) {
    notifications->emplace_back(SeverityLevel::INFO, NotificationCode::PLAN_HINTING, hint);
    interpreter.LogQueryMessage(hint);
  }

  if (interpreter.IsQueryLoggingActive()) {
    std::stringstream printed_plan;
    plan::PrettyPrint(*dba, &plan->plan(), &printed_plan);
    interpreter.LogQueryMessage(fmt::format("Explain plan:\n{}", printed_plan.str()));
  }

  TryCaching(plan->ast_storage(), frame_change_collector);
  summary->insert_or_assign("cost_estimate", plan->cost());
  interpreter.LogQueryMessage(fmt::format("Plan cost: {}", plan->cost()));
  bool is_profile_query = false;
  if (interpreter.IsQueryLoggingActive()) {
    is_profile_query = true;
  }

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
      plan, parsed_query.parameters, is_profile_query, dba, interpreter_context, execution_memory,
      std::move(user_or_role), transaction_status, std::move(tx_timer), current_db.db_acc_, interpreter.query_logger_,
      trigger_context_collector, memory_limit,
      frame_change_collector->IsTrackingValues() ? frame_change_collector : nullptr, hops_limit);
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
                                  std::vector<Notification> *notifications, InterpreterContext *interpreter_context,
                                  Interpreter &interpreter, CurrentDB &current_db) {
  const std::string kExplainQueryStart = "explain ";
  MG_ASSERT(utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.query()), kExplainQueryStart),
            "Expected stripped query to start with '{}'", kExplainQueryStart);

  // Parse and cache the inner query separately (as if it was a standalone
  // query), producing a fresh AST. Note that currently we cannot just reuse
  // part of the already produced AST because the parameters within ASTs are
  // looked up using their positions within the string that was parsed. These
  // wouldn't match up if if we were to reuse the AST (produced by parsing the
  // full query string) when given just the inner query to execute.
  auto inner_query = parsed_query.query_string.substr(kExplainQueryStart.size());
  ParsedQuery parsed_inner_query = ParseQuery(inner_query, parsed_query.user_parameters,
                                              &interpreter_context->ast_cache, interpreter_context->config.query);

  auto *cypher_query = utils::Downcast<CypherQuery>(parsed_inner_query.query);
  MG_ASSERT(cypher_query, "Cypher grammar should not allow other queries in EXPLAIN");

  MG_ASSERT(current_db.execution_db_accessor_, "Explain query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *plan_cache = parsed_inner_query.is_cacheable ? current_db.db_acc_->get()->plan_cache() : nullptr;

  auto cypher_query_plan =
      CypherQueryToPlan(parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage),
                        cypher_query, parsed_inner_query.parameters, plan_cache, dba);

  auto hints = plan::ProvidePlanHints(&cypher_query_plan->plan(), cypher_query_plan->symbol_table());
  for (const auto &hint : hints) {
    notifications->emplace_back(SeverityLevel::INFO, NotificationCode::PLAN_HINTING, hint);
    interpreter.LogQueryMessage(hint);
  }

  std::stringstream printed_plan;
  plan::PrettyPrint(*dba, &cypher_query_plan->plan(), &printed_plan);
  interpreter.LogQueryMessage(fmt::format("Explain plan:\n{}", printed_plan.str()));

  std::vector<std::vector<TypedValue>> printed_plan_rows;
  for (const auto &row : utils::Split(utils::RTrim(printed_plan.str()), "\n")) {
    printed_plan_rows.push_back(std::vector<TypedValue>{TypedValue(row)});
  }

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
                                  std::map<std::string, TypedValue> *summary, std::vector<Notification> *notifications,
                                  InterpreterContext *interpreter_context, Interpreter &interpreter,
                                  CurrentDB &current_db, utils::MemoryResource *execution_memory,
                                  std::shared_ptr<QueryUserOrRole> user_or_role,
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

  MG_ASSERT(cypher_query, "Cypher grammar should not allow other queries in PROFILE");
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parsed_inner_query.parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
  const auto memory_limit = EvaluateMemoryLimit(evaluator, cypher_query->memory_limit_, cypher_query->memory_scale_);

  const auto hops_limit = EvaluateHopsLimit(evaluator, cypher_query->pre_query_directives_.hops_limit_);

  MG_ASSERT(current_db.execution_db_accessor_, "Profile query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *plan_cache = parsed_inner_query.is_cacheable ? current_db.db_acc_->get()->plan_cache() : nullptr;
  auto cypher_query_plan =
      CypherQueryToPlan(parsed_inner_query.stripped_query.hash(), std::move(parsed_inner_query.ast_storage),
                        cypher_query, parsed_inner_query.parameters, plan_cache, dba);
  TryCaching(cypher_query_plan->ast_storage(), frame_change_collector);

  auto hints = plan::ProvidePlanHints(&cypher_query_plan->plan(), cypher_query_plan->symbol_table());
  for (const auto &hint : hints) {
    notifications->emplace_back(SeverityLevel::INFO, NotificationCode::PLAN_HINTING, hint);
    interpreter.LogQueryMessage(hint);
  }

  auto rw_type_checker = plan::ReadWriteTypeChecker();

  rw_type_checker.InferRWType(const_cast<plan::LogicalOperator &>(cypher_query_plan->plan()));

  return PreparedQuery{
      {"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"},
      std::move(parsed_query.required_privileges),
      [plan = std::move(cypher_query_plan), parameters = std::move(parsed_inner_query.parameters), summary, dba,
       interpreter_context, execution_memory, memory_limit, user_or_role = std::move(user_or_role),
       // We want to execute the query we are profiling lazily, so we delay
       // the construction of the corresponding context.
       stats_and_total_time = std::optional<plan::ProfilingStatsWithTotalTime>{},
       pull_plan = std::shared_ptr<PullPlanVector>(nullptr), transaction_status, frame_change_collector,
       tx_timer = std::move(tx_timer), db_acc = current_db.db_acc_, hops_limit,
       &query_logger = interpreter.query_logger_](AnyStream *stream,
                                                  std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        // No output symbols are given so that nothing is streamed.
        if (!stats_and_total_time) {
          stats_and_total_time =
              PullPlan(plan, parameters, true, dba, interpreter_context, execution_memory, std::move(user_or_role),
                       transaction_status, std::move(tx_timer), db_acc, query_logger, nullptr, memory_limit,
                       frame_change_collector->IsTrackingValues() ? frame_change_collector : nullptr, hops_limit)
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
  auto plan = std::make_shared<PullPlanDump>(dba, *current_db.db_acc_);
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
    result.emplace_back();
    result.emplace_back(static_cast<int64_t>(stat_entry.second.count));
    result.emplace_back();
    result.emplace_back();
    result.emplace_back();
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
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
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
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
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

  if (properties.size() > 1) {
    throw utils::NotYetImplemented("index on multiple properties");
  }

  auto properties_stringified = utils::Join(properties_string, ", ");

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

PreparedQuery PrepareEdgeIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                    std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  auto *index_query = utils::Downcast<EdgeIndexQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  MG_ASSERT(current_db.db_acc_, "Index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  auto *storage = db_acc->storage();
  auto edge_type = storage->NameToEdgeType(index_query->edge_type_.name);

  std::vector<storage::PropertyId> properties;
  std::vector<std::string> properties_string;
  properties.reserve(index_query->properties_.size());
  properties_string.reserve(index_query->properties_.size());
  for (const auto &prop : index_query->properties_) {
    properties.push_back(storage->NameToProperty(prop.name));
    properties_string.push_back(prop.name);
  }

  if (properties.size() > 1) {
    throw utils::NotYetImplemented("index on multiple properties");
  }

  auto properties_stringified = utils::Join(properties_string, ", ");

  Notification index_notification(SeverityLevel::INFO);
  switch (index_query->action_) {
    case EdgeIndexQuery::Action::CREATE: {
      index_notification.code = NotificationCode::CREATE_INDEX;
      const auto &ix_properties = index_query->properties_;
      if (ix_properties.empty()) {
        index_notification.title = fmt::format("Created index on edge-type {}.", index_query->edge_type_.name);
      } else {
        index_notification.title = fmt::format("Created index on edge-type {} on property {}.",
                                               index_query->edge_type_.name, ix_properties.front().name);
      }

      handler = [dba, edge_type, label_name = index_query->edge_type_.name,
                 properties_stringified = std::move(properties_stringified), properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        MG_ASSERT(properties.size() <= 1U);
        auto maybe_index_error =
            properties.empty() ? dba->CreateIndex(edge_type) : dba->CreateIndex(edge_type, properties[0]);
        utils::OnScopeExit invalidator(invalidate_plan_cache);

        if (maybe_index_error.HasError()) {
          index_notification.code = NotificationCode::EXISTENT_INDEX;
          index_notification.title =
              fmt::format("Index on edge-type {} on properties {} already exists.", label_name, properties_stringified);
        }
      };
      break;
    }
    case EdgeIndexQuery::Action::DROP: {
      index_notification.code = NotificationCode::DROP_INDEX;
      index_notification.title = fmt::format("Dropped index on edge-type {}.", index_query->edge_type_.name);
      handler = [dba, edge_type, label_name = index_query->edge_type_.name,
                 properties_stringified = std::move(properties_stringified), properties = std::move(properties),
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        MG_ASSERT(properties.size() <= 1U);
        auto maybe_index_error =
            properties.empty() ? dba->DropIndex(edge_type) : dba->DropIndex(edge_type, properties[0]);
        utils::OnScopeExit invalidator(invalidate_plan_cache);

        if (maybe_index_error.HasError()) {
          index_notification.code = NotificationCode::NONEXISTENT_INDEX;
          index_notification.title =
              fmt::format("Index on edge-type {} on {} doesn't exist.", label_name, properties_stringified);
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
        return QueryHandlerResult::COMMIT;
      },
      RWType::W};
}

PreparedQuery PreparePointIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                     std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  auto *index_query = utils::Downcast<PointIndexQuery>(parsed_query.query);
  std::function<Notification(void)> handler;

  MG_ASSERT(current_db.db_acc_, "Index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto const invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  auto label_name = index_query->label_.name;
  auto prop_name = index_query->property_.name;
  auto *storage = db_acc->storage();

  switch (index_query->action_) {
    case PointIndexQuery::Action::CREATE: {
      handler = [label_name = std::move(label_name), prop_name = std::move(prop_name), dba, storage,
                 invalidate_plan_cache = std::move(invalidate_plan_cache)]() {
        Notification index_notification(SeverityLevel::INFO);
        index_notification.code = NotificationCode::CREATE_INDEX;
        index_notification.title = fmt::format("Created point index on label {}, property {}.", label_name, prop_name);

        auto label_id = storage->NameToLabel(label_name);
        auto prop_id = storage->NameToProperty(prop_name);

        auto maybe_index_error = dba->CreatePointIndex(label_id, prop_id);
        utils::OnScopeExit const invalidator(invalidate_plan_cache);

        if (maybe_index_error.HasError()) {
          index_notification.code = NotificationCode::EXISTENT_INDEX;
          index_notification.title =
              fmt::format("Point index on label {} and property {} already exists.", label_name, prop_name);
        }
        return index_notification;
      };
      break;
    }
    case PointIndexQuery::Action::DROP: {
      handler = [label_name = std::move(label_name), prop_name = std::move(prop_name), dba, storage,
                 invalidate_plan_cache = std::move(invalidate_plan_cache)]() {
        Notification index_notification(SeverityLevel::INFO);
        index_notification.code = NotificationCode::DROP_INDEX;
        index_notification.title = fmt::format("Dropped point index on label {}, property {}.", label_name, prop_name);

        auto label_id = storage->NameToLabel(label_name);
        auto prop_id = storage->NameToProperty(prop_name);

        auto maybe_index_error = dba->DropPointIndex(label_id, prop_id);
        utils::OnScopeExit const invalidator(invalidate_plan_cache);

        if (maybe_index_error.HasError()) {
          index_notification.code = NotificationCode::NONEXISTENT_INDEX;
          index_notification.title =
              fmt::format("Point index on label {} and property {} doesn't exist.", label_name, prop_name);
        }
        return index_notification;
      };
      break;
    }
  }

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [handler = std::move(handler), notifications](AnyStream * /*stream*/, std::optional<int> /*unused*/) mutable {
        notifications->push_back(handler());
        return QueryHandlerResult::COMMIT;
      },
      RWType::W};
}

PreparedQuery PrepareTextIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                    std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  auto *text_index_query = utils::Downcast<TextIndexQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  // TODO: we will need transaction for replication
  MG_ASSERT(current_db.db_acc_, "Text index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Text index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  // Creating an index influences computed plan costs.
  auto invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  auto *storage = db_acc->storage();
  auto label = storage->NameToLabel(text_index_query->label_.name);
  auto &index_name = text_index_query->index_name_;

  Notification index_notification(SeverityLevel::INFO);
  switch (text_index_query->action_) {
    case TextIndexQuery::Action::CREATE: {
      index_notification.code = NotificationCode::CREATE_INDEX;
      index_notification.title = fmt::format("Created text index on label {}.", text_index_query->label_.name);
      // TODO: not just storage + invalidate_plan_cache. Need a DB transaction (for replication)
      handler = [dba, label, index_name,
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
          throw TextSearchDisabledException();
        }
        dba->CreateTextIndex(index_name, label);
        utils::OnScopeExit invalidator(invalidate_plan_cache);
      };
      break;
    }
    case TextIndexQuery::Action::DROP: {
      index_notification.code = NotificationCode::DROP_INDEX;
      index_notification.title = fmt::format("Dropped text index on label {}.", text_index_query->label_.name);
      // TODO: not just storage + invalidate_plan_cache. Need a DB transaction (for replication)
      handler = [dba, index_name,
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &index_notification) {
        if (!flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
          throw TextSearchDisabledException();
        }
        dba->DropTextIndex(index_name);
        utils::OnScopeExit invalidator(invalidate_plan_cache);
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

#ifdef MG_ENTERPRISE
PreparedQuery PrepareTtlQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                              std::vector<Notification> *notifications, CurrentDB &current_db,
                              InterpreterContext *interpreter_context) {
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryException("Trying to use enterprise feature without a valid license.");
  }

  if (in_explicit_transaction) {
    throw TtlInMulticommandTxException();
  }

  auto *ttl_query = utils::Downcast<TtlQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  MG_ASSERT(current_db.db_acc_, "Time to live query expects a current DB");
  auto db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Time to live query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *storage = db_acc->storage();
  auto label = storage->NameToLabel("TTL");
  auto prop = storage->NameToProperty("ttl");

  auto invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  Notification notification(SeverityLevel::INFO);
  switch (ttl_query->type_) {
    case TtlQuery::Type::ENABLE: {
      auto evaluation_context = EvaluationContext{.timestamp = QueryTimestamp(), .parameters = parsed_query.parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
      try {
        std::string info;
        std::string period;
        if (ttl_query->period_) {
          period = ttl_query->period_->Accept(evaluator).ValueString();
        }
        std::string start_time;
        if (ttl_query->specific_time_) {
          start_time = ttl_query->specific_time_->Accept(evaluator).ValueString();
        }
        auto ttl_info = ttl::TtlInfo{period, start_time};
        if (interpreter_context->repl_state->IsReplica()) {
          // Special case for REPLICA
          info = "TTL configured. Background job will not run, since instance is REPLICA.";
        } else {
          // TTL could already be configured; use the present config if no user-defined config
          info = "Starting time-to-live worker. Will be executed";
          if (ttl_info)
            info += ttl_info.ToString();
          else if (db_acc->ttl().Config())
            info += db_acc->ttl().Config().ToString();
        }
        handler = [db_acc = std::move(db_acc), dba, label, prop, ttl_info, interpreter_context, info = std::move(info),
                   invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &notification) mutable {
          auto &ttl = db_acc->ttl();

          if (!ttl.Enabled()) {
            (void)dba->CreateIndex(label, prop);  // Only way to fail is to try to create an already existant index
            ttl.Enable();
            std::invoke(invalidate_plan_cache);
          }
          if (ttl_info) ttl.Configure(ttl_info);
          ttl.Setup(std::move(db_acc), interpreter_context);

          notification.code = NotificationCode::ENABLE_TTL;
          notification.title = info;
        };
      } catch (const ttl::TtlException &e) {
        throw utils::BasicException(e.what());
      }
      break;
    }
    case TtlQuery::Type::DISABLE: {
      // TODO: not just storage + invalidate_plan_cache. Need a DB transaction (for replication)
      handler = [db_acc = std::move(db_acc), dba, label, prop,
                 invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification &notification) mutable {
        (void)dba->DropIndex(label, prop);  // Only way to fail is to try to drop a non-existant index
        const utils::OnScopeExit invalidator(invalidate_plan_cache);
        db_acc->ttl().Disable();
        notification.code = NotificationCode::DISABLE_TTL;
        notification.title = fmt::format("Disabled time-to-live feature.");
      };
      break;
    }
    case TtlQuery::Type::STOP: {
      handler = [db_acc = std::move(db_acc)](Notification &notification) mutable {
        db_acc->ttl().Stop();
        notification.code = NotificationCode::STOP_TTL;
        notification.title = fmt::format("Stopped time-to-live worker.");
      };
      break;
    }
    default: {
      DMG_ASSERT(false, "Unknown ttl query type");
    }
  }

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [handler = std::move(handler), notifications, notification = std::move(notification)](
                           AnyStream * /*stream*/, std::optional<int> /*unused*/) mutable {
                         handler(notification);
                         notifications->push_back(notification);
                         return QueryHandlerResult::COMMIT;  // TODO: Will need to become COMMIT when we fix replication
                       },
                       RWType::NONE};
}
#endif

PreparedQuery PrepareAuthQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                               InterpreterContext *interpreter_context, Interpreter &interpreter) {
  if (in_explicit_transaction) {
    throw UserModificationInMulticommandTxException();
  }

  auto *auth_query = utils::Downcast<AuthQuery>(parsed_query.query);

  auto callback = HandleAuthQuery(auth_query, interpreter_context, parsed_query.parameters, interpreter);

  return PreparedQuery{
      std::move(callback.header), std::move(parsed_query.required_privileges),
      [handler = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](  // NOLINT
          AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        if (!pull_plan) {
          // Run the specific query
          auto results = handler();
          pull_plan = std::make_shared<PullPlanVector>(std::move(results));
        }

        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      },
      RWType::NONE};
}

PreparedQuery PrepareReplicationQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction, std::vector<Notification> *notifications,
    ReplicationQueryHandler &replication_query_handler, CurrentDB & /*current_db*/, const InterpreterConfig &config
#ifdef MG_ENTERPRISE
    ,
    std::optional<std::reference_wrapper<coordination::CoordinatorState>> coordinator_state
#endif
) {
  if (in_explicit_transaction) {
    throw ReplicationModificationInMulticommandTxException();
  }

  auto *replication_query = utils::Downcast<ReplicationQuery>(parsed_query.query);
  auto callback = HandleReplicationQuery(replication_query, parsed_query.parameters, replication_query_handler, config,
                                         notifications
#ifdef MG_ENTERPRISE
                                         ,
                                         coordinator_state
#endif
  );

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

#ifdef MG_ENTERPRISE
PreparedQuery PrepareCoordinatorQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                      std::vector<Notification> *notifications,
                                      coordination::CoordinatorState &coordinator_state,
                                      const InterpreterConfig &config) {
  if (in_explicit_transaction) {
    throw CoordinatorModificationInMulticommandTxException();
  }

  auto *coordinator_query = utils::Downcast<CoordinatorQuery>(parsed_query.query);
  auto callback =
      HandleCoordinatorQuery(coordinator_query, parsed_query.parameters, &coordinator_state, config, notifications);

  return PreparedQuery{callback.header, std::move(parsed_query.required_privileges),
                       [callback_fn = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (UNLIKELY(!pull_plan)) {
                           pull_plan = std::make_shared<PullPlanVector>(callback_fn());
                         }

                         if (pull_plan->Pull(stream, n)) [[likely]] {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}
#endif

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

Callback CreateTrigger(TriggerQuery *trigger_query, const storage::PropertyValue::map_t &user_parameters,
                       TriggerStore *trigger_store, InterpreterContext *interpreter_context, DbAccessor *dba,
                       std::shared_ptr<QueryUserOrRole> user_or_role) {
  // Make a copy of the user and pass it to the subsystem
  auto owner = interpreter_context->auth_checker->GenQueryUser(user_or_role->username(), user_or_role->rolename());
  return {{},
          [trigger_name = std::move(trigger_query->trigger_name_),
           trigger_statement = std::move(trigger_query->statement_), event_type = trigger_query->event_type_,
           before_commit = trigger_query->before_commit_, trigger_store, interpreter_context, dba, user_parameters,
           owner = std::move(owner)]() mutable -> std::vector<std::vector<TypedValue>> {
            trigger_store->AddTrigger(
                std::move(trigger_name), trigger_statement, user_parameters, ToTriggerEventType(event_type),
                before_commit ? TriggerPhase::BEFORE_COMMIT : TriggerPhase::AFTER_COMMIT,
                &interpreter_context->ast_cache, dba, interpreter_context->config.query, std::move(owner));
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
                                  std::shared_ptr<QueryUserOrRole> user_or_role) {
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

  auto callback = std::invoke([trigger_query, trigger_store, interpreter_context, dba,
                               user_parameters = parsed_query.user_parameters, owner = std::move(user_or_role),
                               &trigger_notification]() mutable {
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
                                 InterpreterContext *interpreter_context,
                                 std::shared_ptr<QueryUserOrRole> user_or_role) {
  if (in_explicit_transaction) {
    throw StreamQueryInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Stream query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  auto *stream_query = utils::Downcast<StreamQuery>(parsed_query.query);
  MG_ASSERT(stream_query);
  auto callback = HandleStreamQuery(stream_query, parsed_query.parameters, db_acc, interpreter_context,
                                    std::move(user_or_role), notifications);

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
  MG_ASSERT(current_db.db_acc_, "Storage Isolation Level query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();
  if (storage->GetStorageMode() == storage::StorageMode::IN_MEMORY_ANALYTICAL) {
    throw IsolationLevelModificationInAnalyticsException();
  }
  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL &&
      isolation_level != storage::IsolationLevel::SNAPSHOT_ISOLATION) {
    throw IsolationLevelModificationInDiskTransactionalException();
  }

  std::function<void()> callback;
  switch (isolation_level_query->isolation_level_scope_) {
    case IsolationLevelQuery::IsolationLevelScope::GLOBAL: {
      callback = [storage, isolation_level] {
        if (auto res = storage->SetIsolationLevel(isolation_level); res.HasError()) {
          throw utils::BasicException("Failed setting global isolation level");
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

  return PreparedQuery{{},
                       std::move(parsed_query.required_privileges),
                       [callback = std::move(callback)](AnyStream * /*stream*/,
                                                        std::optional<int> /*n*/) -> std::optional<QueryHandlerResult> {
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
            if (auto vertex_cnt_approx = in.storage()->GetBaseInfo().vertex_count; vertex_cnt_approx > 0) {
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

Callback DropGraph(memgraph::dbms::DatabaseAccess &db, DbAccessor *dba) {
  Callback callback;
  callback.fn = [&db, dba]() mutable {
    auto storage_mode = db->GetStorageMode();
    if (storage_mode != storage::StorageMode::IN_MEMORY_ANALYTICAL) {
      throw utils::BasicException(
          "Drop graph can only be used in the analytical mode. Switch to analytical mode by executing 'STORAGE MODE "
          "IN_MEMORY_ANALYTICAL'");
    }
    dba->DropGraph();

    auto *trigger_store = db->trigger_store();
    trigger_store->DropAll();

    auto *streams = db->streams();
    streams->DropAll();

    auto &ttl = db->ttl();
    ttl.Stop();

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

std::vector<Interpreter::SessionInfo> GetActiveUsersInfo(InterpreterContext *interpreter_context) {
  std::vector<Interpreter::SessionInfo> active_users =
      interpreter_context->interpreters.WithLock([](const auto &interpreters_) {
        std::vector<Interpreter::SessionInfo> info;
        info.reserve(interpreters_.size());
        for (const auto &interpreter : interpreters_) {
          info.push_back(interpreter->session_info_);
        }

        return info;
      });

  return active_users;
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

    callback = [requested_mode,
                storage = static_cast<storage::InMemoryStorage *>(db_acc->storage())]() -> std::function<void()> {
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

PreparedQuery PrepareDropGraphQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
  MG_ASSERT(current_db.db_acc_, "Drop graph query expects a current DB");
  memgraph::dbms::DatabaseAccess &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Drop graph query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *drop_graph_query = utils::Downcast<DropGraphQuery>(parsed_query.query);
  MG_ASSERT(drop_graph_query);

  std::function<void()> callback = DropGraph(db_acc, dba).fn;

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

PreparedQuery PrepareCreateSnapshotQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db,
                                         replication_coordination_glue::ReplicationRole replication_role) {
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
      [storage, replication_role](AnyStream * /*stream*/,
                                  std::optional<int> /*n*/) -> std::optional<QueryHandlerResult> {
        auto *mem_storage = static_cast<storage::InMemoryStorage *>(storage);
        if (auto maybe_error = mem_storage->CreateSnapshot(replication_role); maybe_error.HasError()) {
          switch (maybe_error.GetError()) {
            case storage::InMemoryStorage::CreateSnapshotError::DisabledForReplica:
              throw utils::BasicException(
                  "Failed to create a snapshot. Replica instances are not allowed to create them.");
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
auto ShowTransactions(const std::unordered_set<Interpreter *> &interpreters, QueryUserOrRole *user_or_role,
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
      return interpreter->current_db_.db_acc_ ? interpreter->current_db_.db_acc_->get()->name() : all;
    };

    auto same_user = [](const auto &lv, const auto &rv) {
      if (lv.get() == rv) return true;
      if (lv && rv) return *lv == *rv;
      return false;
    };

    if (transaction_id.has_value() && (same_user(interpreter->user_or_role_, user_or_role) ||
                                       privilege_checker(user_or_role, get_interpreter_db_name()))) {
      const auto &typed_queries = interpreter->GetQueries();
      results.push_back(
          {TypedValue(interpreter->user_or_role_
                          ? (interpreter->user_or_role_->username() ? *interpreter->user_or_role_->username() : "")
                          : ""),
           TypedValue(std::to_string(transaction_id.value())), TypedValue(typed_queries)});
      // Handle user-defined metadata
      std::map<std::string, TypedValue> metadata_tv;
      if (interpreter->metadata_) {
        for (const auto &md : *(interpreter->metadata_)) {
          metadata_tv.emplace(md.first, TypedValue(md.second));
        }
      }
      results.back().emplace_back(metadata_tv);
    }
  }
  return results;
}

Callback HandleTransactionQueueQuery(TransactionQueueQuery *transaction_query,
                                     std::shared_ptr<QueryUserOrRole> user_or_role, const Parameters &parameters,
                                     InterpreterContext *interpreter_context) {
  auto privilege_checker = [](QueryUserOrRole *user_or_role, std::string const &db_name) {
    return user_or_role && user_or_role->IsAuthorized({query::AuthQuery::Privilege::TRANSACTION_MANAGEMENT}, db_name,
                                                      &query::up_to_date_policy);
  };

  Callback callback;
  switch (transaction_query->action_) {
    case TransactionQueueQuery::Action::SHOW_TRANSACTIONS: {
      auto show_transactions = [user_or_role = std::move(user_or_role),
                                privilege_checker = std::move(privilege_checker)](const auto &interpreters) {
        return ShowTransactions(interpreters, user_or_role.get(), privilege_checker);
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
      callback.fn = [interpreter_context, maybe_kill_transaction_ids = std::move(maybe_kill_transaction_ids),
                     user_or_role = std::move(user_or_role),
                     privilege_checker = std::move(privilege_checker)]() mutable {
        return interpreter_context->TerminateTransactions(std::move(maybe_kill_transaction_ids), user_or_role.get(),
                                                          std::move(privilege_checker));
      };
      break;
    }
  }

  return callback;
}

PreparedQuery PrepareTransactionQueueQuery(ParsedQuery parsed_query, std::shared_ptr<QueryUserOrRole> user_or_role,
                                           InterpreterContext *interpreter_context) {
  auto *transaction_queue_query = utils::Downcast<TransactionQueueQuery>(parsed_query.query);
  MG_ASSERT(transaction_queue_query);
  auto callback = HandleTransactionQueueQuery(transaction_queue_query, std::move(user_or_role), parsed_query.parameters,
                                              interpreter_context);

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
  MG_ASSERT(current_db.db_transactional_accessor_, "Database info query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *info_query = utils::Downcast<DatabaseInfoQuery>(parsed_query.query);
  std::vector<std::string> header;
  std::function<std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()> handler;
  auto *database = current_db.db_acc_->get();
  switch (info_query->info_type_) {
    case DatabaseInfoQuery::InfoType::INDEX: {
      header = {"index type", "label", "property", "count"};
      handler = [database, dba] {
        auto *storage = database->storage();
        const std::string_view label_index_mark{"label"};
        const std::string_view label_property_index_mark{"label+property"};
        const std::string_view edge_type_index_mark{"edge-type"};
        const std::string_view edge_type_property_index_mark{"edge-type+property"};
        const std::string_view text_index_mark{"text"};
        const std::string_view point_label_property_index_mark{"point"};
        auto info = dba->ListAllIndices();
        auto storage_acc = database->Access();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.label.size() + info.label_property.size() + info.text_indices.size());
        for (const auto &item : info.label) {
          results.push_back({TypedValue(label_index_mark), TypedValue(storage->LabelToName(item)), TypedValue(),
                             TypedValue(static_cast<int>(storage_acc->ApproximateVertexCount(item)))});
        }
        for (const auto &item : info.label_property) {
          results.push_back(
              {TypedValue(label_property_index_mark), TypedValue(storage->LabelToName(item.first)),
               TypedValue(storage->PropertyToName(item.second)),
               TypedValue(static_cast<int>(storage_acc->ApproximateVertexCount(item.first, item.second)))});
        }
        for (const auto &item : info.edge_type) {
          results.push_back({TypedValue(edge_type_index_mark), TypedValue(storage->EdgeTypeToName(item)), TypedValue(),
                             TypedValue(static_cast<int>(storage_acc->ApproximateEdgeCount(item)))});
        }
        for (const auto &item : info.edge_type_property) {
          results.push_back({TypedValue(edge_type_property_index_mark), TypedValue(storage->EdgeTypeToName(item.first)),
                             TypedValue(storage->PropertyToName(item.second)),
                             TypedValue(static_cast<int>(storage_acc->ApproximateEdgeCount(item.first, item.second)))});
        }
        for (const auto &[index_name, label] : info.text_indices) {
          results.push_back({TypedValue(fmt::format("{} (name: {})", text_index_mark, index_name)),
                             TypedValue(storage->LabelToName(label)), TypedValue(), TypedValue()});
        }
        for (const auto &[label_id, prop_id] : info.point_label_property) {
          results.push_back({TypedValue(point_label_property_index_mark), TypedValue(storage->LabelToName(label_id)),
                             TypedValue(storage->PropertyToName(prop_id)),
                             TypedValue(static_cast<int>(storage_acc->ApproximatePointCount(label_id, prop_id)))});
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
      header = {"constraint type", "label", "properties", "data_type"};
      handler = [storage = current_db.db_acc_->get()->storage(), dba] {
        auto info = dba->ListAllConstraints();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.existence.size() + info.unique.size() + info.type.size());
        for (const auto &item : info.existence) {
          results.push_back({TypedValue("exists"), TypedValue(storage->LabelToName(item.first)),
                             TypedValue(storage->PropertyToName(item.second)), TypedValue("")});
        }
        for (const auto &item : info.unique) {
          std::vector<TypedValue> properties;
          properties.reserve(item.second.size());
          for (const auto &property : item.second) {
            properties.emplace_back(storage->PropertyToName(property));
          }
          results.push_back({TypedValue("unique"), TypedValue(storage->LabelToName(item.first)),
                             TypedValue(std::move(properties)), TypedValue("")});
        }
        for (const auto &[label, property, type] : info.type) {
          results.push_back({TypedValue("data_type"), TypedValue(storage->LabelToName(label)),
                             TypedValue(storage->PropertyToName(property)),
                             TypedValue(storage::TypeConstraintKindToString(type))});
        }
        return std::pair{results, QueryHandlerResult::COMMIT};
      };
      break;
    }
    case DatabaseInfoQuery::InfoType::EDGE_TYPES: {
      header = {"edge types"};
      handler = [storage = current_db.db_acc_->get()->storage(), dba] {
        if (!storage->config_.salient.items.enable_schema_metadata) {
          throw QueryRuntimeException(
              "The metadata collection for edge-types is disabled. To enable it, restart your instance and set the "
              "storage-enable-schema-metadata flag to True.");
        }
        auto edge_types = dba->ListAllPossiblyPresentEdgeTypes();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(edge_types.size());
        for (auto &edge_type : edge_types) {
          results.push_back({TypedValue(storage->EdgeTypeToName(edge_type))});
        }

        return std::pair{results, QueryHandlerResult::COMMIT};
      };

      break;
    }
    case DatabaseInfoQuery::InfoType::NODE_LABELS: {
      header = {"node labels"};
      handler = [storage = current_db.db_acc_->get()->storage(), dba] {
        if (!storage->config_.salient.items.enable_schema_metadata) {
          throw QueryRuntimeException(
              "The metadata collection for node-labels is disabled. To enable it, restart your instance and set the "
              "storage-enable-schema-metadata flag to True.");
        }
        auto node_labels = dba->ListAllPossiblyPresentVertexLabels();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(node_labels.size());
        for (auto &node_label : node_labels) {
          results.push_back({TypedValue(storage->LabelToName(node_label))});
        }

        return std::pair{results, QueryHandlerResult::COMMIT};
      };

      break;
    }
    case DatabaseInfoQuery::InfoType::METRICS: {
#ifdef MG_ENTERPRISE
      if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
        throw QueryRuntimeException("SHOW METRICS INFO command is only available with a valid Enterprise License!");
      }
#else
      throw QueryRuntimeException("SHOW METRICS INFO command is only available in Memgraph Enterprise build!");
#endif
      header = {"name", "type", "metric type", "value"};
      handler = [storage = current_db.db_acc_->get()->storage()] {
        auto const info = storage->GetBaseInfo();
        auto const metrics_info = memgraph::storage::Storage::GetMetrics();
        std::vector<std::vector<TypedValue>> results;
        results.push_back({TypedValue("VertexCount"), TypedValue("General"), TypedValue("Gauge"),
                           TypedValue(static_cast<int64_t>(info.vertex_count))});
        results.push_back({TypedValue("EdgeCount"), TypedValue("General"), TypedValue("Gauge"),
                           TypedValue(static_cast<int64_t>(info.edge_count))});
        results.push_back(
            {TypedValue("AverageDegree"), TypedValue("General"), TypedValue("Gauge"), TypedValue(info.average_degree)});
        results.push_back({TypedValue("MemoryRes"), TypedValue("Memory"), TypedValue("Gauge"),
                           TypedValue(static_cast<int64_t>(info.memory_res))});
        results.push_back({TypedValue("DiskUsage"), TypedValue("Memory"), TypedValue("Gauge"),
                           TypedValue(static_cast<int64_t>(info.disk_usage))});
        for (const auto &metric : metrics_info) {
          results.push_back({TypedValue(metric.name), TypedValue(metric.type), TypedValue(metric.event_type),
                             TypedValue(static_cast<int64_t>(metric.value))});
        }
        std::ranges::sort(results, [](auto const &record_1, auto const &record_2) {
          auto const key_1 = std::tie(record_1[1].ValueString(), record_1[2].ValueString(), record_1[0].ValueString());
          auto const key_2 = std::tie(record_2[1].ValueString(), record_2[2].ValueString(), record_2[0].ValueString());
          return key_1 < key_2;
        });
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
                                     std::optional<storage::IsolationLevel> next_transaction_isolation_level,
                                     InterpreterContext *interpreter_context) {
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
        auto info = storage->GetBaseInfo();
        const auto vm_max_map_count = utils::GetVmMaxMapCount();
        const int64_t vm_max_map_count_storage_info =
            vm_max_map_count.has_value() ? vm_max_map_count.value() : memgraph::utils::VM_MAX_MAP_COUNT_DEFAULT;
        std::vector<std::vector<TypedValue>> results{
            {TypedValue("name"), TypedValue(storage->name())},
            {TypedValue("vertex_count"), TypedValue(static_cast<int64_t>(info.vertex_count))},
            {TypedValue("edge_count"), TypedValue(static_cast<int64_t>(info.edge_count))},
            {TypedValue("average_degree"), TypedValue(info.average_degree)},
            {TypedValue("vm_max_map_count"), TypedValue(vm_max_map_count_storage_info)},
            {TypedValue("memory_res"), TypedValue(utils::GetReadableSize(static_cast<double>(info.memory_res)))},
            {TypedValue("peak_memory_res"),
             TypedValue(utils::GetReadableSize(static_cast<double>(info.peak_memory_res)))},
            {TypedValue("unreleased_delta_objects"), TypedValue(static_cast<int64_t>(info.unreleased_delta_objects))},
            {TypedValue("disk_usage"), TypedValue(utils::GetReadableSize(static_cast<double>(info.disk_usage)))},
            {TypedValue("memory_tracked"),
             TypedValue(utils::GetReadableSize(static_cast<double>(utils::total_memory_tracker.Amount())))},
            {TypedValue("allocation_limit"),
             TypedValue(utils::GetReadableSize(static_cast<double>(utils::total_memory_tracker.HardLimit())))},
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
    case SystemInfoQuery::InfoType::ACTIVE_USERS: {
      header = {"username", "session uuid", "login timestamp"};
      handler = [interpreter_context] {
        std::vector<std::vector<TypedValue>> results;
        for (const auto &result : GetActiveUsersInfo(interpreter_context)) {
          results.push_back({TypedValue(result.username), TypedValue(result.uuid), TypedValue(result.login_timestamp)});
        }
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
        case Constraint::Type::NODE_KEY: {
          throw utils::NotYetImplemented("Node key constraints");
        }
        case Constraint::Type::EXISTS: {
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
        }
        case Constraint::Type::UNIQUE: {
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
        case Constraint::Type::TYPE: {
          auto const maybe_constraint_type = constraint_query->constraint_.type_constraint;
          MG_ASSERT(maybe_constraint_type.has_value());
          auto const constraint_type = *maybe_constraint_type;

          constraint_notification.title = fmt::format("Created IS TYPED {} constraint on label {} on property {}.",
                                                      storage::TypeConstraintKindToString(constraint_type),
                                                      constraint_query->constraint_.label.name, properties_stringified);
          handler = [storage, dba, label, label_name = constraint_query->constraint_.label.name, constraint_type,
                     properties_stringified = std::move(properties_stringified),
                     properties = std::move(properties)](Notification & /**/) {
            auto maybe_constraint_error = dba->CreateTypeConstraint(label, properties[0], constraint_type);

            if (maybe_constraint_error.HasError()) {
              const auto &error = maybe_constraint_error.GetError();
              std::visit(
                  [storage, &label_name, &properties_stringified,
                   constraint_type]<typename T>(T const &arg) {  // TODO: using universal reference gives clang tidy
                                                                 // error but it used above with no problem?
                    using ErrorType = std::remove_cvref_t<T>;
                    if constexpr (std::is_same_v<ErrorType, storage::ConstraintViolation>) {
                      auto &violation = arg;
                      MG_ASSERT(violation.properties.size() == 1U);
                      auto property_name = storage->PropertyToName(*violation.properties.begin());
                      throw QueryRuntimeException(
                          "Unable to create IS TYPED {} constraint on :{}({}), because an "
                          "existing node violates it.",
                          storage::TypeConstraintKindToString(constraint_type), label_name, property_name);
                    } else if constexpr (std::is_same_v<ErrorType, storage::ConstraintDefinitionError>) {
                      throw QueryRuntimeException("Constraint IS TYPED {} on :{}({}) already exists",
                                                  storage::TypeConstraintKindToString(constraint_type), label_name,
                                                  properties_stringified);
                    } else {
                      static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
                    }
                  },
                  error);
            }
          };
          break;
        }
      }
    } break;
    case ConstraintQuery::ActionType::DROP: {
      constraint_notification.code = NotificationCode::DROP_CONSTRAINT;

      switch (constraint_query->constraint_.type) {
        case Constraint::Type::NODE_KEY:
          throw utils::NotYetImplemented("Node key constraints");
        case Constraint::Type::EXISTS: {
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
        }
        case Constraint::Type::UNIQUE: {
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
          break;
        }
        case Constraint::Type::TYPE: {
          auto const maybe_constraint_type = constraint_query->constraint_.type_constraint;
          MG_ASSERT(maybe_constraint_type.has_value());
          auto const constraint_type = *maybe_constraint_type;

          constraint_notification.title =
              fmt::format("Dropped IS TYPED {} constraint on label {} on properties {}.",
                          storage::TypeConstraintKindToString(constraint_type),
                          constraint_query->constraint_.label.name, utils::Join(properties_string, ", "));
          handler = [dba, label, label_name = constraint_query->constraint_.label.name, constraint_type,
                     properties_stringified = std::move(properties_stringified),
                     properties = std::move(properties)](Notification & /**/) {
            auto maybe_constraint_error = dba->DropTypeConstraint(label, properties[0], constraint_type);
            if (maybe_constraint_error.HasError()) {
              throw QueryRuntimeException("Constraint IS TYPED {} on :{}({}) doesn't exist",
                                          storage::TypeConstraintKindToString(constraint_type), label_name,
                                          properties_stringified);
            }
            return std::vector<std::vector<TypedValue>>();
          };
          break;
        }
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
                                        std::optional<std::function<void(std::string_view)>> on_change_cb,
                                        Interpreter &interpreter) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryException("Trying to use enterprise feature without a valid license.");
  }

  auto *query = utils::Downcast<MultiDatabaseQuery>(parsed_query.query);
  auto *db_handler = interpreter_context->dbms_handler;

  const bool is_replica = interpreter_context->repl_state->IsReplica();

  switch (query->action_) {
    case MultiDatabaseQuery::Action::CREATE: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }
      return PreparedQuery{
          {"STATUS"},
          std::move(parsed_query.required_privileges),
          [db_name = query->db_name_, db_handler, interpreter = &interpreter](
              AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
            if (!interpreter->system_transaction_) {
              throw QueryException("Expected to be in a system transaction");
            }

            std::vector<std::vector<TypedValue>> status;
            std::string res;

            const auto success = db_handler->New(db_name, &*interpreter->system_transaction_);
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
    }
    case MultiDatabaseQuery::Action::USE: {
      if (current_db.in_explicit_db_) {
        throw QueryException("Database switching is prohibited if session explicitly defines the used database");
      }
      return PreparedQuery{{"STATUS"},
                           std::move(parsed_query.required_privileges),
                           [db_name = query->db_name_, db_handler, &current_db, on_change = std::move(on_change_cb)](
                               AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                             std::vector<std::vector<TypedValue>> status;
                             std::string res;

                             try {
                               if (current_db.db_acc_ && db_name == current_db.db_acc_->get()->name()) {
                                 res = "Already using " + db_name;
                               } else {
                                 auto tmp = db_handler->Get(db_name);
                                 if (on_change) (*on_change)(db_name);  // Will trow if cb fails
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
    }
    case MultiDatabaseQuery::Action::DROP: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }

      return PreparedQuery{
          {"STATUS"},
          std::move(parsed_query.required_privileges),
          [db_name = query->db_name_, db_handler, auth = interpreter_context->auth, interpreter = &interpreter](
              AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
            if (!interpreter->system_transaction_) {
              throw QueryException("Expected to be in a system transaction");
            }

            std::vector<std::vector<TypedValue>> status;

            try {
              // Remove database
              auto success = db_handler->TryDelete(db_name, &*interpreter->system_transaction_);
              if (!success.HasError()) {
                // Remove from auth
                if (auth) auth->DeleteDatabase(db_name, &*interpreter->system_transaction_);
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
    case MultiDatabaseQuery::Action::SHOW: {
      return PreparedQuery{
          {"Current"},
          std::move(parsed_query.required_privileges),
          [db_acc = current_db.db_acc_, pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
              AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
            if (!pull_plan) {
              std::vector<std::vector<TypedValue>> results;
              auto db_name = db_acc ? TypedValue{db_acc->get()->storage()->name()} : TypedValue{};
              results.push_back({std::move(db_name)});
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
    }
  };
#else
  throw QueryException("Query not supported.");
#endif
}

PreparedQuery PrepareShowDatabasesQuery(ParsedQuery parsed_query, InterpreterContext *interpreter_context,
                                        std::shared_ptr<QueryUserOrRole> user_or_role) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryException("Trying to use enterprise feature without a valid license.");
  }

  auto *db_handler = interpreter_context->dbms_handler;
  AuthQueryHandler *auth = interpreter_context->auth;

  Callback callback;
  callback.header = {"Name"};
  callback.fn = [auth, db_handler,
                 user_or_role = std::move(user_or_role)]() mutable -> std::vector<std::vector<TypedValue>> {
    std::vector<std::vector<TypedValue>> status;
    auto gen_status = [&]<typename T, typename K>(T all, K denied) {
      Sort(all);
      Sort(denied);

      status.reserve(all.size());
      for (const auto &name : all) {
        status.push_back({TypedValue(name)});
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

    if (!user_or_role || !*user_or_role) {
      // No user, return all
      gen_status(db_handler->All(), std::vector<TypedValue>{});
    } else {
      // User has a subset of accessible dbs; this is synched with the SessionContextHandler
      const auto &db_priv = auth->GetDatabasePrivileges(user_or_role->key());
      const auto &allowed = db_priv[0][0];
      const auto &denied = db_priv[0][1].ValueList();
      if (allowed.IsString() && allowed.ValueString() == auth::kAllDatabases) {
        // All databases are allowed
        gen_status(db_handler->All(), denied);
      } else {
        gen_status(allowed.ValueList(), denied);
      }
    }

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

PreparedQuery PrepareCreateEnumQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
  MG_ASSERT(current_db.db_acc_, "Create Enum query expects a current DB");

  auto *create_enum_query = utils::Downcast<CreateEnumQuery>(parsed_query.query);
  MG_ASSERT(create_enum_query);

  return {{},
          std::move(parsed_query.required_privileges),
          [dba = *current_db.execution_db_accessor_, enum_name = std::move(create_enum_query->enum_name_),
           enum_values = std::move(create_enum_query->enum_values_)](
              AnyStream * /*stream*/, std::optional<int> /*unused*/) mutable -> std::optional<QueryHandlerResult> {
            auto res = dba.CreateEnum(enum_name, enum_values);
            if (res.HasError()) {
              switch (res.GetError()) {
                case storage::EnumStorageError::EnumExists:
                  throw QueryRuntimeException("Enum already exists.");
                case storage::EnumStorageError::InvalidValue:
                  throw QueryRuntimeException("Enum value has duplicate.");
                default:
                  // Should not happen
                  throw QueryRuntimeException("Enum could not be created.");
              }
            }
            return QueryHandlerResult::COMMIT;
          },
          RWType::W};
}

PreparedQuery PrepareShowEnumsQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
  return PreparedQuery{
      {"Enum Name", "Enum Values"},
      std::move(parsed_query.required_privileges),
      [dba = *current_db.execution_db_accessor_, pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
          AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        if (!pull_plan) {
          auto enums = dba.ShowEnums();
          auto to_row = [](auto &&p) { return std::vector{TypedValue{p.first}, TypedValue{p.second}}; };
          pull_plan = std::make_shared<PullPlanVector>(enums | rv::transform(to_row) | r::to_vector);
        }

        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      },
      RWType::NONE,
      ""  // No target DB
  };
}

PreparedQuery PrepareEnumAlterAddQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
  MG_ASSERT(current_db.db_acc_, "Alter Enum query expects a current DB");

  auto *alter_enum_add_query = utils::Downcast<AlterEnumAddValueQuery>(parsed_query.query);
  MG_ASSERT(alter_enum_add_query);

  return {{},
          std::move(parsed_query.required_privileges),
          [dba = *current_db.execution_db_accessor_, enum_name = std::move(alter_enum_add_query->enum_name_),
           enum_value = std::move(alter_enum_add_query->enum_value_)](
              AnyStream * /*stream*/, std::optional<int> /*unused*/) mutable -> std::optional<QueryHandlerResult> {
            auto res = dba.EnumAlterAdd(enum_name, enum_value);
            if (res.HasError()) {
              switch (res.GetError()) {
                case storage::EnumStorageError::InvalidValue:
                  throw QueryRuntimeException("Enum value already exists.");
                case storage::EnumStorageError::UnknownEnumType:
                  throw QueryRuntimeException("Unknown Enum type.");
                default:
                  // Should not happen
                  throw QueryRuntimeException("Enum could not be altered.");
              }
            }
            return QueryHandlerResult::COMMIT;
          },
          RWType::W};
}

PreparedQuery PrepareEnumAlterUpdateQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
  MG_ASSERT(current_db.db_acc_, "Alter Enum query expects a current DB");

  auto *alter_enum_update_query = utils::Downcast<AlterEnumUpdateValueQuery>(parsed_query.query);
  MG_ASSERT(alter_enum_update_query);

  return {{},
          std::move(parsed_query.required_privileges),
          [dba = *current_db.execution_db_accessor_, enum_name = std::move(alter_enum_update_query->enum_name_),
           enum_value_old = std::move(alter_enum_update_query->old_enum_value_),
           enum_value_new = std::move(alter_enum_update_query->new_enum_value_)](
              AnyStream * /*stream*/, std::optional<int> /*unused*/) mutable -> std::optional<QueryHandlerResult> {
            auto res = dba.EnumAlterUpdate(enum_name, enum_value_old, enum_value_new);
            if (res.HasError()) {
              switch (res.GetError()) {
                case storage::EnumStorageError::InvalidValue:
                  throw QueryRuntimeException("Enum value {}::{} already exists.", enum_name, enum_value_new);
                case storage::EnumStorageError::UnknownEnumType:
                  throw QueryRuntimeException("Unknown Enum name {}.", enum_name);
                case storage::EnumStorageError::UnknownEnumValue:
                  throw QueryRuntimeException("Unknown Enum value {}::{}.", enum_name, enum_value_old);
                default:
                  // Should not happen
                  throw QueryRuntimeException("Enum could not be altered.");
              }
            }
            return QueryHandlerResult::COMMIT;
          },
          RWType::W};
}

PreparedQuery PrepareSessionTraceQuery(ParsedQuery parsed_query, CurrentDB &current_db, Interpreter *interpreter) {
  MG_ASSERT(current_db.db_acc_, "Session trace query expects a current DB");

  auto *session_trace_query = utils::Downcast<SessionTraceQuery>(parsed_query.query);
  MG_ASSERT(session_trace_query);

  std::function<std::pair<std::vector<std::vector<TypedValue>>, QueryHandlerResult>()> handler;
  handler = [interpreter, enabled = session_trace_query->enabled_] {
    std::vector<std::vector<TypedValue>> results;

    auto query_log_directory = flags::run_time::GetQueryLogDirectory();

    if (query_log_directory.empty()) {
      throw QueryException("The flag --query-log-directory has to be present in order to enable session trace.");
    }

    if (enabled) {
      interpreter->query_logger_.emplace(fmt::format("{}/{}.log", query_log_directory, interpreter->session_info_.uuid),
                                         interpreter->session_info_.uuid, interpreter->session_info_.username);
      interpreter->LogQueryMessage("Session initialized!");
    } else {
      interpreter->query_logger_.reset();
    }

    results.emplace_back(std::vector<TypedValue>{TypedValue(interpreter->session_info_.uuid)});
    return std::pair{results, QueryHandlerResult::NOTHING};
  };

  return PreparedQuery{{"session uuid"},
                       std::move(parsed_query.required_privileges),
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

PreparedQuery PrepareShowSchemaInfoQuery(const ParsedQuery &parsed_query, CurrentDB &current_db) {
  if (current_db.db_acc_->get()->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw ShowSchemaInfoOnDiskException();
  }

  Callback callback;
  callback.header = {"schema"};
  callback.fn = [db = *current_db.db_acc_, db_acc = current_db.execution_db_accessor_,
                 storage_acc =
                     current_db.db_transactional_accessor_.get()]() mutable -> std::vector<std::vector<TypedValue>> {
    memgraph::metrics::IncrementCounter(memgraph::metrics::ShowSchema);

    std::vector<std::vector<TypedValue>> schema;
    auto *storage = db->storage();
    if (storage->config_.salient.items.enable_schema_info) {
      // SCHEMA INFO
      auto json = storage->SchemaInfoReadAccessor().ToJson(*storage->name_id_mapper_, storage->enum_store_);

      // INDICES
      auto node_indexes = nlohmann::json::array();
      auto edge_indexes = nlohmann::json::array();
      auto index_info = db_acc->ListAllIndices();
      // Vertex label indices
      for (const auto label_id : index_info.label) {
        node_indexes.push_back(nlohmann::json::object({{"labels", {storage->LabelToName(label_id)}},
                                                       {"properties", nlohmann::json::array()},
                                                       {"count", storage_acc->ApproximateVertexCount(label_id)}}));
      }
      // Vertex label property indices
      for (const auto &[label_id, property] : index_info.label_property) {
        node_indexes.push_back(
            nlohmann::json::object({{"labels", {storage->LabelToName(label_id)}},
                                    {"properties", {storage->PropertyToName(property)}},
                                    {"count", storage_acc->ApproximateVertexCount(label_id, property)}}));
      }
      // Vertex label text
      for (const auto &[str, label_id] : index_info.text_indices) {
        node_indexes.push_back(nlohmann::json::object({{"labels", {storage->LabelToName(label_id)}},
                                                       {"properties", nlohmann::json::array()},
                                                       {"count", -1},
                                                       {"type", "label_text"}}));
      }
      // Vertex label property_point
      for (const auto &[label_id, property] : index_info.point_label_property) {
        node_indexes.push_back(
            nlohmann::json::object({{"labels", {storage->LabelToName(label_id)}},
                                    {"properties", {storage->PropertyToName(property)}},
                                    {"count", storage_acc->ApproximatePointCount(label_id, property)},
                                    {"type", "label+property_point"}}));
      }
      // Edge type indices
      for (const auto type : index_info.edge_type) {
        edge_indexes.push_back(nlohmann::json::object({{"edge_type", {storage->EdgeTypeToName(type)}},
                                                       {"properties", nlohmann::json::array()},
                                                       {"count", storage_acc->ApproximateEdgeCount(type)}}));
      }
      // Edge type property indices
      for (const auto &[type, property] : index_info.edge_type_property) {
        edge_indexes.push_back(nlohmann::json::object({{"edge_type", {storage->EdgeTypeToName(type)}},
                                                       {"properties", {storage->PropertyToName(property)}},
                                                       {"count", storage_acc->ApproximateEdgeCount(type, property)}}));
      }
      json.emplace("node_indexes", std::move(node_indexes));
      json.emplace("edge_indexes", std::move(edge_indexes));

      // CONSTRAINTS
      auto node_constraints = nlohmann::json::array();
      auto constraint_info = db_acc->ListAllConstraints();
      // Existence
      for (const auto &[label_id, property] : constraint_info.existence) {
        node_constraints.push_back(nlohmann::json::object({{"type", "existence"},
                                                           {"labels", {storage->LabelToName(label_id)}},
                                                           {"properties", {storage->PropertyToName(property)}}}));
      }
      // Unique
      for (const auto &[label_id, properties] : constraint_info.unique) {
        auto json_properties = nlohmann::json::array();
        for (const auto property : properties) {
          json_properties.emplace_back(storage->PropertyToName(property));
        }
        node_constraints.push_back(nlohmann::json::object({{"type", "unique"},
                                                           {"labels", {storage->LabelToName(label_id)}},
                                                           {"properties", std::move(json_properties)}}));
      }
      // Type
      for (const auto &[label_id, property, constraint_kind] : constraint_info.type) {
        node_constraints.push_back(
            nlohmann::json::object({{"type", "data_type"},
                                    {"labels", {storage->LabelToName(label_id)}},
                                    {"properties", {storage->PropertyToName(property)}},
                                    {"data_type", TypeConstraintKindToString(constraint_kind)}}));
      }
      json.emplace("node_constraints", std::move(node_constraints));

      // ENUMS
      auto enums = nlohmann::json::array();
      for (auto [type, values] : storage->enum_store_.AllRegistered()) {
        auto json_values = nlohmann::json::array();
        for (const auto &val : values) json_values.push_back(val);
        enums.push_back(nlohmann::json::object({{"name", type}, {"values", std::move(json_values)}}));
      }
      json.emplace("enums", std::move(enums));

      // Pack json into query result
      schema.push_back(std::vector<TypedValue>{TypedValue(json.dump())});
    } else {
      throw QueryException("SchemaInfo disabled.");
    }
    return schema;
  };

  return PreparedQuery{std::move(callback.header), parsed_query.required_privileges,
                       [handler = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) {
                           auto results = handler();
                           pull_plan = std::make_shared<PullPlanVector>(std::move(results));
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::R, current_db.db_acc_->get()->name()};
}

std::optional<uint64_t> Interpreter::GetTransactionId() const { return current_transaction_; }

void Interpreter::BeginTransaction(QueryExtras const &extras) {
  ResetInterpreter();
  const auto prepared_query = PrepareTransactionQuery("BEGIN", extras);
  prepared_query.query_handler(nullptr, {});
}

void Interpreter::CommitTransaction() {
  const auto prepared_query = PrepareTransactionQuery("COMMIT");
  prepared_query.query_handler(nullptr, {});
  ResetInterpreter();
}

void Interpreter::RollbackTransaction() {
  const auto prepared_query = PrepareTransactionQuery("ROLLBACK");
  prepared_query.query_handler(nullptr, {});
  ResetInterpreter();
}

#ifdef MG_ENTERPRISE
auto Interpreter::Route(std::map<std::string, std::string> const &routing) -> RouteResult {
  if (!interpreter_context_->coordinator_state_) {
    throw QueryException("You cannot fetch routing table from an instance which is not part of a cluster.");
  }
  if (interpreter_context_->coordinator_state_.has_value() &&
      interpreter_context_->coordinator_state_->get().IsDataInstance()) {
    auto const &address = routing.find("address");
    if (address == routing.end()) {
      throw QueryException("Routing table must contain address field.");
    }

    auto result = RouteResult{};
    if (interpreter_context_->repl_state->IsMain()) {
      result.servers.emplace_back(std::vector<std::string>{address->second}, "WRITE");
    } else {
      result.servers.emplace_back(std::vector<std::string>{address->second}, "READ");
    }
    return result;
  }

  return RouteResult{.servers = interpreter_context_->coordinator_state_->get().GetRoutingTable()};
}
#endif

#if MG_ENTERPRISE
// Before Prepare or during Prepare, but single-threaded.
// TODO: Is there any cleanup?
void Interpreter::SetCurrentDB(std::string_view db_name, bool in_explicit_db) {
  // Can throw
  // do we lock here?
  current_db_.SetCurrentDB(interpreter_context_->dbms_handler->Get(db_name), in_explicit_db);
}
#else
// Default database only
void Interpreter::SetCurrentDB() { current_db_.SetCurrentDB(interpreter_context_->dbms_handler->Get(), false); }
#endif

Interpreter::PrepareResult Interpreter::Prepare(const std::string &query_string, UserParameters_fn params_getter,
                                                QueryExtras const &extras) {
  LogQueryMessage(fmt::format("Accepted query: {}", query_string));
  MG_ASSERT(user_or_role_, "Trying to prepare a query without a query user.");
  // Handle transaction control queries.
  const auto upper_case_query = utils::ToUpperCase(query_string);
  const auto trimmed_query = utils::Trim(upper_case_query);
  if (trimmed_query == "BEGIN" || trimmed_query == "COMMIT" || trimmed_query == "ROLLBACK") {
    if (trimmed_query == "BEGIN") {
      ResetInterpreter();
    }
    auto &query_execution = query_executions_.emplace_back(QueryExecution::Create());
    query_execution->prepared_query = PrepareTransactionQuery(trimmed_query, extras);
    auto qid = in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};
    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid, {}};
  }

  // NOTE: query_string is not BEGIN, COMMIT or ROLLBACK

  bool const is_schema_assert_query{upper_case_query.find(kSchemaAssert) != std::string::npos};

  // All queries other than transaction control queries advance the command in
  // an explicit transaction block.
  if (in_explicit_transaction_) {
    if (is_schema_assert_query) {
      throw SchemaAssertInMulticommandTxException();
    }

    transaction_queries_->push_back(query_string);
    AdvanceCommand();
  } else {
    ResetInterpreter();
    transaction_queries_->push_back(query_string);
    if (current_db_.db_transactional_accessor_ /* && !in_explicit_transaction_*/) {
      // If we're not in an explicit transaction block and we have an open
      // transaction, abort it since we're about to prepare a new query.
      AbortCommand(nullptr);
    }

    SetupInterpreterTransaction(extras);
    LogQueryMessage(fmt::format("Query [{}] associated with transaction [{}]", query_string, *current_transaction_));
  }

  std::unique_ptr<QueryExecution> *query_execution_ptr = nullptr;
  try {
    utils::Timer parsing_timer;
    LogQueryMessage("Query parsing started.");
    ParsedQuery parsed_query = ParseQuery(query_string, params_getter(nullptr), &interpreter_context_->ast_cache,
                                          interpreter_context_->config.query);
    auto parsing_time = parsing_timer.Elapsed().count();
    LogQueryMessage("Query parsing ended.");

    // Setup QueryExecution
    query_executions_.emplace_back(QueryExecution::Create());
    auto &query_execution = query_executions_.back();
    query_execution_ptr = &query_execution;

    std::optional<int> qid =
        in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};

    query_execution->summary["parsing_time"] = parsing_time;
    LogQueryMessage(fmt::format("Query parsing time: {}", parsing_time));

    // Set a default cost estimate of 0. Individual queries can overwrite this
    // field with an improved estimate.
    query_execution->summary["cost_estimate"] = 0.0;

    // System queries require strict ordering; since there is no MVCC-like thing, we allow single queries
    bool system_queries = utils::Downcast<AuthQuery>(parsed_query.query) ||
                          utils::Downcast<MultiDatabaseQuery>(parsed_query.query) ||
                          utils::Downcast<ShowDatabasesQuery>(parsed_query.query) ||
                          utils::Downcast<ReplicationQuery>(parsed_query.query);

    // TODO Split SHOW REPLICAS (which needs the db) and other replication queries
    auto system_transaction = std::invoke([&]() -> std::optional<memgraph::system::Transaction> {
      if (!system_queries) return std::nullopt;

      // TODO: Ordering between system and data queries
      auto system_txn = interpreter_context_->system_->TryCreateTransaction(std::chrono::milliseconds(kSystemTxTryMS));
      if (!system_txn) {
        throw ConcurrentSystemQueriesException("Multiple concurrent system queries are not supported.");
      }
      return system_txn;
    });

    // Some queries do not require a database to be executed (current_db_ won't be passed on to the Prepare*; special
    // case for use database which overwrites the current database)
    bool no_db_required = system_queries || utils::Downcast<ShowConfigQuery>(parsed_query.query) ||
                          utils::Downcast<SettingQuery>(parsed_query.query) ||
                          utils::Downcast<VersionQuery>(parsed_query.query) ||
                          utils::Downcast<TransactionQueueQuery>(parsed_query.query);
    if (!no_db_required && !current_db_.db_acc_) {
      throw DatabaseContextRequiredException("Database required for the query.");
    }

    // Some queries require an active transaction in order to be prepared.
    // TODO: make a better analysis visitor over the `parsed_query.query`
    bool const unique_db_transaction =
        utils::Downcast<IndexQuery>(parsed_query.query) || utils::Downcast<EdgeIndexQuery>(parsed_query.query) ||
        utils::Downcast<PointIndexQuery>(parsed_query.query) || utils::Downcast<TextIndexQuery>(parsed_query.query) ||
        utils::Downcast<ConstraintQuery>(parsed_query.query) || utils::Downcast<DropGraphQuery>(parsed_query.query) ||
        utils::Downcast<CreateEnumQuery>(parsed_query.query) ||
        utils::Downcast<AlterEnumAddValueQuery>(parsed_query.query) ||
        utils::Downcast<AlterEnumUpdateValueQuery>(parsed_query.query) || utils::Downcast<TtlQuery>(parsed_query.query);

    bool const requires_db_transaction =
        unique_db_transaction || utils::Downcast<CypherQuery>(parsed_query.query) ||
        utils::Downcast<ExplainQuery>(parsed_query.query) || utils::Downcast<ProfileQuery>(parsed_query.query) ||
        utils::Downcast<DumpQuery>(parsed_query.query) || utils::Downcast<TriggerQuery>(parsed_query.query) ||
        utils::Downcast<AnalyzeGraphQuery>(parsed_query.query) ||
        utils::Downcast<DatabaseInfoQuery>(parsed_query.query) || utils::Downcast<ShowEnumsQuery>(parsed_query.query) ||
        utils::Downcast<ShowSchemaInfoQuery>(parsed_query.query);

    if (!in_explicit_transaction_ && requires_db_transaction) {
      // TODO: ATM only a single database, will change when we have multiple database transactions
      bool could_commit = utils::Downcast<CypherQuery>(parsed_query.query) != nullptr;
      bool const unique = unique_db_transaction || is_schema_assert_query;
      SetupDatabaseTransaction(could_commit, unique);
    }

    if (current_db_.db_acc_) {
      // fix parameters, enums requires storage to map to correct enum value
      parsed_query.user_parameters = params_getter(current_db_.db_acc_->get()->storage());
      parsed_query.parameters = PrepareQueryParameters(parsed_query.stripped_query, parsed_query.user_parameters);
    }

#ifdef MG_ENTERPRISE
    // TODO(antoniofilipovic) extend to cover Lab queries
    if (interpreter_context_->coordinator_state_ && interpreter_context_->coordinator_state_->get().IsCoordinator() &&
        !utils::Downcast<CoordinatorQuery>(parsed_query.query) && !utils::Downcast<SettingQuery>(parsed_query.query)) {
      throw QueryRuntimeException("Coordinator can run only coordinator queries!");
    }
#endif

    utils::Timer planning_timer;
    LogQueryMessage("Query planning started!");
    PreparedQuery prepared_query;
    utils::MemoryResource *memory_resource = query_execution->execution_memory.resource();
    frame_change_collector_.reset();
    frame_change_collector_.emplace();

    if (utils::Downcast<CypherQuery>(parsed_query.query)) {
      prepared_query =
          PrepareCypherQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_, current_db_,
                             memory_resource, &query_execution->notifications, user_or_role_, &transaction_status_,
                             current_timeout_timer_, *this, &*frame_change_collector_);
    } else if (utils::Downcast<ExplainQuery>(parsed_query.query)) {
      prepared_query = PrepareExplainQuery(std::move(parsed_query), &query_execution->summary,
                                           &query_execution->notifications, interpreter_context_, *this, current_db_);
    } else if (utils::Downcast<ProfileQuery>(parsed_query.query)) {
      prepared_query = PrepareProfileQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                                           &query_execution->notifications, interpreter_context_, *this, current_db_,
                                           memory_resource, user_or_role_, &transaction_status_, current_timeout_timer_,
                                           &*frame_change_collector_);
    } else if (utils::Downcast<DumpQuery>(parsed_query.query)) {
      prepared_query = PrepareDumpQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<IndexQuery>(parsed_query.query)) {
      prepared_query = PrepareIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                         &query_execution->notifications, current_db_);
    } else if (utils::Downcast<EdgeIndexQuery>(parsed_query.query)) {
      prepared_query = PrepareEdgeIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                             &query_execution->notifications, current_db_);
    } else if (utils::Downcast<PointIndexQuery>(parsed_query.query)) {
      prepared_query = PreparePointIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                              &query_execution->notifications, current_db_);
    } else if (utils::Downcast<TextIndexQuery>(parsed_query.query)) {
      prepared_query = PrepareTextIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                             &query_execution->notifications, current_db_);
    } else if (utils::Downcast<TtlQuery>(parsed_query.query)) {
#ifdef MG_ENTERPRISE
      prepared_query = PrepareTtlQuery(std::move(parsed_query), in_explicit_transaction_,
                                       &query_execution->notifications, current_db_, interpreter_context_);
#else
      throw QueryException("Query not supported.");
#endif  // MG_ENTERPRISE
    } else if (utils::Downcast<AnalyzeGraphQuery>(parsed_query.query)) {
      prepared_query = PrepareAnalyzeGraphQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<AuthQuery>(parsed_query.query)) {
      /// SYSTEM (Replication) PURE
      prepared_query = PrepareAuthQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_, *this);
    } else if (utils::Downcast<DatabaseInfoQuery>(parsed_query.query)) {
      prepared_query = PrepareDatabaseInfoQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<SystemInfoQuery>(parsed_query.query)) {
      prepared_query =
          PrepareSystemInfoQuery(std::move(parsed_query), in_explicit_transaction_, current_db_,
                                 interpreter_isolation_level, next_transaction_isolation_level, interpreter_context_);
    } else if (utils::Downcast<ConstraintQuery>(parsed_query.query)) {
      prepared_query = PrepareConstraintQuery(std::move(parsed_query), in_explicit_transaction_,
                                              &query_execution->notifications, current_db_);
    } else if (utils::Downcast<ReplicationQuery>(parsed_query.query)) {
      /// TODO: make replication DB agnostic
      prepared_query =
          PrepareReplicationQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                                  *interpreter_context_->replication_handler_, current_db_, interpreter_context_->config
#ifdef MG_ENTERPRISE
                                  ,
                                  interpreter_context_->coordinator_state_
#endif
          );

    } else if (utils::Downcast<CoordinatorQuery>(parsed_query.query)) {
#ifdef MG_ENTERPRISE
      if (!interpreter_context_->coordinator_state_.has_value()) {
        throw QueryRuntimeException(
            "Coordinator was not initialized as coordinator port and coordinator id or management port where not "
            "set.");
      }
      prepared_query =
          PrepareCoordinatorQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                                  *interpreter_context_->coordinator_state_, interpreter_context_->config);
#else
      throw QueryRuntimeException("Coordinator queries are not part of community edition");
#endif
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
                              current_db_, interpreter_context_, user_or_role_);
    } else if (utils::Downcast<StreamQuery>(parsed_query.query)) {
      prepared_query =
          PrepareStreamQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                             current_db_, interpreter_context_, user_or_role_);
    } else if (utils::Downcast<IsolationLevelQuery>(parsed_query.query)) {
      prepared_query = PrepareIsolationLevelQuery(std::move(parsed_query), in_explicit_transaction_, current_db_, this);
    } else if (utils::Downcast<CreateSnapshotQuery>(parsed_query.query)) {
      auto const replication_role = interpreter_context_->repl_state->GetRole();
      prepared_query =
          PrepareCreateSnapshotQuery(std::move(parsed_query), in_explicit_transaction_, current_db_, replication_role);
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
      prepared_query = PrepareTransactionQueueQuery(std::move(parsed_query), user_or_role_, interpreter_context_);
    } else if (utils::Downcast<MultiDatabaseQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw MultiDatabaseQueryInMulticommandTxException();
      }
      /// SYSTEM (Replication) + INTERPRETER
      // DMG_ASSERT(system_guard);
      prepared_query =
          PrepareMultiDatabaseQuery(std::move(parsed_query), current_db_, interpreter_context_, on_change_, *this);
    } else if (utils::Downcast<ShowDatabasesQuery>(parsed_query.query)) {
      prepared_query = PrepareShowDatabasesQuery(std::move(parsed_query), interpreter_context_, user_or_role_);
    } else if (utils::Downcast<EdgeImportModeQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw EdgeImportModeModificationInMulticommandTxException();
      }
      prepared_query = PrepareEdgeImportModeQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<DropGraphQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw DropGraphInMulticommandTxException();
      }
      prepared_query = PrepareDropGraphQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<CreateEnumQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw EnumModificationInMulticommandTxException();
      }
      prepared_query = PrepareCreateEnumQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<ShowEnumsQuery>(parsed_query.query)) {
      prepared_query = PrepareShowEnumsQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<AlterEnumAddValueQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw EnumModificationInMulticommandTxException();
      }
      prepared_query = PrepareEnumAlterAddQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<AlterEnumUpdateValueQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw EnumModificationInMulticommandTxException();
      }
      prepared_query = PrepareEnumAlterUpdateQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<AlterEnumRemoveValueQuery>(parsed_query.query)) {
      throw utils::NotYetImplemented("Alter enum remove value");
    } else if (utils::Downcast<DropEnumQuery>(parsed_query.query)) {
      throw utils::NotYetImplemented("Drop enum");
    } else if (utils::Downcast<ShowSchemaInfoQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw ShowSchemaInfoInMulticommandTxException();
      }
      prepared_query = PrepareShowSchemaInfoQuery(parsed_query, current_db_);
    } else if (utils::Downcast<SessionTraceQuery>(parsed_query.query)) {
      prepared_query = PrepareSessionTraceQuery(std::move(parsed_query), current_db_, this);
    } else {
      LOG_FATAL("Should not get here -- unknown query type!");
    }

    auto planning_time = planning_timer.Elapsed().count();
    query_execution->summary["planning_time"] = planning_time;
    query_execution->prepared_query.emplace(std::move(prepared_query));
    LogQueryMessage("Query planning ended.");
    LogQueryMessage(fmt::format("Query planning time: {}", planning_time));

    const auto rw_type = query_execution->prepared_query->rw_type;
    query_execution->summary["type"] = plan::ReadWriteTypeChecker::TypeToString(rw_type);

    UpdateTypeCount(rw_type);

    bool const write_query = IsQueryWrite(rw_type);
    if (write_query) {
      if (interpreter_context_->repl_state->IsReplica()) {
        query_execution = nullptr;
        throw WriteQueryOnReplicaException();
      }
#ifdef MG_ENTERPRISE
      if (interpreter_context_->coordinator_state_.has_value() &&
          interpreter_context_->coordinator_state_->get().IsDataInstance() &&
          !interpreter_context_->repl_state->IsMainWriteable()) {
        query_execution = nullptr;
        throw WriteQueryOnMainException();
      }
#endif
    }

    // Set the target db to the current db (some queries have different target from the current db)
    if (!query_execution->prepared_query->db) {
      if (current_db_.db_acc_) {
        query_execution->prepared_query->db = current_db_.db_acc_->get()->name();
      } else {
        query_execution->prepared_query->db = "";
      }
    }
    query_execution->summary["db"] = *query_execution->prepared_query->db;

    // prepare is done, move system txn guard to be owned by interpreter
    system_transaction_ = std::move(system_transaction);
    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid,
            query_execution->prepared_query->db};
  } catch (const utils::BasicException &e) {
    LogQueryMessage(fmt::format("Failed query: {}", e.what()));
    // Trigger first failed query
    metrics::FirstFailedQuery();
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedQuery);
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedPrepare);
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
  if (query_logger_) {
    query_logger_->SetTransactionId(std::to_string(*current_transaction_));
  }
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
  LogQueryMessage("Query abort started.");
  utils::OnScopeExit const abort_end([this]() {
    this->LogQueryMessage("Query abort ended.");
    if (query_logger_) {
      query_logger_->ResetTransactionId();
    }
  });

  bool decrement = true;

  // System tx
  // TODO Implement system transaction scope and the ability to abort
  system_transaction_.reset();

  // Data tx
  auto expected = TransactionStatus::ACTIVE;
  while (!transaction_status_.compare_exchange_weak(expected, TransactionStatus::STARTED_ROLLBACK)) {
    if (expected == TransactionStatus::TERMINATED || expected == TransactionStatus::IDLE) {
      transaction_status_.store(TransactionStatus::STARTED_ROLLBACK);
      decrement = false;
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

  if (decrement) {
    // Decrement only if the transaction was active when we started to Abort
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveTransactions);
  }

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
    QueryAllocator execution_memory{};

    // create a new transaction for each trigger
    auto tx_acc = db_acc->Access();
    DbAccessor db_accessor{tx_acc.get()};

    // On-disk storage removes all Vertex/Edge Accessors because previous trigger tx finished.
    // So we need to adapt TriggerContext based on user transaction which is still alive.
    auto trigger_context = original_trigger_context;
    trigger_context.AdaptForAccessor(&db_accessor);
    try {
      trigger.Execute(&db_accessor, db_acc, execution_memory.resource(), flags::run_time::GetExecutionTimeout(),
                      &interpreter_context->is_shutting_down, transaction_status, trigger_context);
    } catch (const utils::BasicException &exception) {
      spdlog::warn("Trigger '{}' failed with exception:\n{}", trigger.Name(), exception.what());
      db_accessor.Abort();
      continue;
    }

    bool is_main = interpreter_context->repl_state->IsMain();
    auto maybe_commit_error = db_accessor.Commit({.is_main = is_main}, db_acc);

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
                  break;
                }
                case storage::ConstraintViolation::Type::UNIQUE: {
                  const auto &label_name = db_accessor.LabelToName(constraint_violation.label);
                  std::stringstream property_names_stream;
                  utils::PrintIterable(
                      property_names_stream, constraint_violation.properties, ", ",
                      [&](auto &stream, const auto &prop) { stream << db_accessor.PropertyToName(prop); });
                  spdlog::warn("Trigger '{}' failed to commit due to unique constraint violation on :{}({})",
                               trigger.Name(), label_name, property_names_stream.str());
                  break;
                }
                case storage::ConstraintViolation::Type::TYPE: {
                  MG_ASSERT(constraint_violation.properties.size() == 1U);
                  const auto &property_name = db_accessor.PropertyToName(*constraint_violation.properties.begin());
                  const auto &label_name = db_accessor.LabelToName(constraint_violation.label);
                  spdlog::warn("Trigger '{}' failed to commit due to type constraint violation on: {}({}) IS TYPED {}",
                               trigger.Name(), label_name, property_name,
                               storage::TypeConstraintKindToString(*constraint_violation.constraint_kind));

                  break;
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
  LogQueryMessage("Query commit started.");
  utils::OnScopeExit const commit_end([this]() {
    this->LogQueryMessage("Query commit ended.");
    if (query_logger_) {
      query_logger_->ResetTransactionId();
    }
  });

  // It's possible that some queries did not finish because the user did
  // not pull all of the results from the query.
  // For now, we will not check if there are some unfinished queries.
  // We should document clearly that all results should be pulled to complete
  // a query.
  current_transaction_.reset();
  if (!current_db_.db_transactional_accessor_ || !current_db_.db_acc_) {
    // No database nor db transaction; check for system transaction
    if (!system_transaction_) return;

    // TODO Distinguish between data and system transaction state
    // Think about updating the status to a struct with bitfield
    // Clean transaction status on exit
    utils::OnScopeExit clean_status([this]() {
      system_transaction_.reset();
      // System transactions are not terminable
      // Durability has happened at time of PULL
      // Commit is doing replication and timestamp update
      // The DBMS does not support MVCC, so doing durability here doesn't change the overall logic; we cannot abort!
      // What we are trying to do is set the transaction back to IDLE
      // We cannot simply put it to IDLE, since the status is used as a syncronization method and we have to follow
      // its logic. There are 2 states when we could update to IDLE (ACTIVE and TERMINATED).
      auto expected = TransactionStatus::ACTIVE;
      while (!transaction_status_.compare_exchange_weak(expected, TransactionStatus::IDLE)) {
        if (expected == TransactionStatus::TERMINATED) {
          continue;
        }
        expected = TransactionStatus::ACTIVE;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });

    auto const main_commit = [&](replication::RoleMainData &mainData) {
    // Only enterprise can do system replication
#ifdef MG_ENTERPRISE
      if (license::global_license_checker.IsEnterpriseValidFast()) {
        return system_transaction_->Commit(memgraph::system::DoReplication{mainData});
      }
#endif
      return system_transaction_->Commit(memgraph::system::DoNothing{});
    };

    auto const replica_commit = [&](replication::RoleReplicaData &) {
      return system_transaction_->Commit(memgraph::system::DoNothing{});
    };

    auto const commit_method = utils::Overloaded{main_commit, replica_commit};
    [[maybe_unused]] auto sync_result = std::visit(commit_method, interpreter_context_->repl_state->ReplicationData());
    // TODO: something with sync_result
    return;
  }
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
      QueryAllocator execution_memory{};
      AdvanceCommand();
      try {
        trigger.Execute(&*current_db_.execution_db_accessor_, current_db_.db_acc_, execution_memory.resource(),
                        flags::run_time::GetExecutionTimeout(), &interpreter_context_->is_shutting_down,
                        &transaction_status_, *trigger_context);
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

  auto commit_confirmed_by_all_sync_replicas = true;

  bool is_main = interpreter_context_->repl_state->IsMain();
  auto maybe_commit_error = current_db_.db_transactional_accessor_->Commit({.is_main = is_main}, current_db_.db_acc_);
  if (maybe_commit_error.HasError()) {
    const auto &error = maybe_commit_error.GetError();

    std::visit(
        [&execution_db_accessor = current_db_.execution_db_accessor_,
         &commit_confirmed_by_all_sync_replicas]<typename T>(const T &arg) {
          using ErrorType = std::remove_cvref_t<T>;
          if constexpr (std::is_same_v<ErrorType, storage::ReplicationError>) {
            commit_confirmed_by_all_sync_replicas = false;
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
              case storage::ConstraintViolation::Type::TYPE: {
                // This should never get triggered since type constraints get checked immediately and not at
                // commit time
                MG_ASSERT(false, "Encountered type constraint violation while commiting which should never happen.");
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
      RunTriggersAfterCommit(*current_db_.db_acc_, interpreter_context_, std::move(trigger_context),
                             &this->transaction_status_);
      user_transaction->FinalizeTransaction();
      SPDLOG_DEBUG("Finished executing after commit triggers");  // NOLINT(bugprone-lambda-function-name)
    });
  }

  SPDLOG_DEBUG("Finished committing the transaction");
  if (!commit_confirmed_by_all_sync_replicas) {
    throw ReplicationException("At least one SYNC replica has not confirmed committing last transaction.");
  }

  if (IsQueryLoggingActive()) {
    query_logger_->trace("Commit successfully finished!");
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

void Interpreter::SetUser(std::shared_ptr<QueryUserOrRole> user_or_role) {
  user_or_role_ = std::move(user_or_role);
  if (query_logger_) {
    query_logger_->SetUser(user_or_role_->key());
  }
}

void Interpreter::SetSessionInfo(std::string uuid, std::string username, std::string login_timestamp) {
  session_info_ = {.uuid = uuid, .username = username, .login_timestamp = login_timestamp};
  if (query_logger_) {
    query_logger_->SetSessionId(uuid);
  }
}

void Interpreter::ResetUser() {
  user_or_role_.reset();
  if (query_logger_) {
    query_logger_->ResetUser();
  }
}

bool Interpreter::IsQueryLoggingActive() const { return query_logger_.has_value(); }

void Interpreter::LogQueryMessage(std::string message) {
  if (query_logger_.has_value()) {
    (*query_logger_).trace(message);
  }
}

}  // namespace memgraph::query
