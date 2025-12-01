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
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <usearch/index_plugins.hpp>
#include <utility>
#include <variant>
#include <vector>

#include "auth/auth.hpp"
#include "auth/exceptions.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "coordination/constants.hpp"
#include "coordination/coordinator_ops_status.hpp"
#include "coordination/coordinator_state.hpp"
#include "dbms/constants.hpp"
#include "dbms/coordinator_handler.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/global.hpp"
#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "flags/storage_access.hpp"
#include "frontend/ast/query/user_profile.hpp"
#include "frontend/semantic/rw_checker.hpp"
#include "io/network/endpoint.hpp"
#include "license/license.hpp"
#include "memory/global_memory_control.hpp"
#include "memory/query_memory_control.hpp"
#include "query/auth_query_handler.hpp"
#include "query/config.hpp"
#include "query/constants.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/dependant_symbol_visitor.hpp"
#include "query/dump.hpp"
#include "query/exceptions.hpp"
#include "query/frame_change_caching.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/hops_limit.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/interpreter_context.hpp"
#include "query/metadata.hpp"
#include "query/parameters.hpp"
#include "query/plan/fmt.hpp"
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
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "replication/config.hpp"
#include "replication/state.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/edge_import_mode.hpp"
#include "storage/v2/fmt.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/algorithm.hpp"
#include "utils/build_info.hpp"
#include "utils/compile_time.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_histogram.hpp"
#include "utils/exceptions.hpp"
#include "utils/functional.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/priority_thread_pool.hpp"
#include "utils/query_memory_tracker.hpp"
#include "utils/readable_size.hpp"
#include "utils/resource_monitoring.hpp"
#include "utils/settings.hpp"
#include "utils/stat.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"
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

// Accessors need to be able to throw if unable to gain access. Otherwise there could be a deadlock, where some queries
// gained access during prepare, but can't execute (in PULL) because other queries are still preparing/waiting for
// access
void memgraph::query::CurrentDB::SetupDatabaseTransaction(
    std::optional<storage::IsolationLevel> override_isolation_level, bool could_commit,
    storage::StorageAccessType acc_type) {
  if (!db_acc_) {
    throw DatabaseContextRequiredException("Database required for the transaction setup.");
  }
  auto &db_acc = *db_acc_;
  const auto timeout = std::chrono::seconds{FLAGS_storage_access_timeout_sec};
  switch (acc_type) {
    case storage::StorageAccessType::READ:
      [[fallthrough]];
    case storage::StorageAccessType::WRITE:
      db_transactional_accessor_ = db_acc->Access(acc_type, override_isolation_level,
                                                  /*allow timeout*/ timeout);
      break;
    case storage::StorageAccessType::UNIQUE:
      db_transactional_accessor_ = db_acc->UniqueAccess(override_isolation_level, /*allow timeout*/ timeout);
      break;
    case storage::StorageAccessType::READ_ONLY:
      db_transactional_accessor_ = db_acc->ReadOnlyAccess(override_isolation_level, /*allow timeout*/ timeout);
      break;
    default:
      spdlog::error("Unknown accessor type: {}", static_cast<int>(acc_type));
      throw QueryRuntimeException("Failed to gain storage access! Unknown accessor type.");
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

struct QueryLogWrapper {
  std::string_view query;
  const memgraph::storage::ExternalPropertyValue::map_t *metadata;
  std::string_view db_name;
};

#if FMT_VERSION > 90000
template <>
class fmt::formatter<QueryLogWrapper> : public fmt::ostream_formatter {};
#endif

std::ostream &operator<<(std::ostream &os, const QueryLogWrapper &qlw) {
  auto final_query = memgraph::utils::NoCopyStr{qlw.query};
#if MG_ENTERPRISE
  os << "[Run - " << qlw.db_name << "] ";
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    final_query = memgraph::logging::MaskSensitiveInformation(final_query.view());
  }
#else
  os << "[Run] ";
#endif
  os << "'" << final_query.view() << "'";
  if (!qlw.metadata->empty()) {
    os << " - {";
    std::string header;
    for (const auto &[key, val] : *qlw.metadata) {
      os << header << key << ":" << val;
      if (header.empty()) header = ", ";
    }
    os << "}";
  }
  return os;
}

namespace memgraph::query {

constexpr std::string_view kSchemaAssert = "SCHEMA.ASSERT";
constexpr int kSystemTxTryMS = 100;  //!< Duration of the unique try_lock_for

template <typename>
constexpr auto kAlwaysFalse = false;

namespace {
constexpr std::string_view kSocketErrorExplanation =
    "The socket address must be a string defining the address and port, delimited by a "
    "single colon. The address must be valid and the port must be an integer.";

#ifdef MG_ENTERPRISE
void EnsureMainInstance(InterpreterContext *interpreter_context, const std::string &operation_name) {
  if (interpreter_context->repl_state.ReadLock()->IsReplica()) {
    throw QueryException(
        fmt::format("{} forbidden on REPLICA! This operation must be executed on the MAIN instance.", operation_name));
  }
}
#endif

template <typename T, typename K>
void Sort(std::vector<T, K> &vec) {
  std::sort(vec.begin(), vec.end());
}

template <typename K>
void Sort(std::vector<TypedValue, K> &vec) {
  std::sort(vec.begin(), vec.end(),
            [](const TypedValue &lv, const TypedValue &rv) { return lv.ValueString() < rv.ValueString(); });
}

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
std::optional<T> GenOptional(const T &in) {
  return in.empty() ? std::nullopt : std::make_optional<T>(in);
}

struct Callback {
  std::vector<std::string> header;
  using CallbackFunction = std::function<std::vector<std::vector<TypedValue>>()>;
  CallbackFunction fn;
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

#ifdef MG_ENTERPRISE
constexpr auto convertFromCoordinatorToReplicationMode(const CoordinatorQuery::SyncMode &sync_mode)
    -> replication_coordination_glue::ReplicationMode {
  switch (sync_mode) {
    case CoordinatorQuery::SyncMode::ASYNC: {
      return replication_coordination_glue::ReplicationMode::ASYNC;
    }
    case CoordinatorQuery::SyncMode::SYNC: {
      return replication_coordination_glue::ReplicationMode::SYNC;
    }
    case CoordinatorQuery::SyncMode::STRICT_SYNC: {
      return replication_coordination_glue::ReplicationMode::STRICT_SYNC;
    }
  }
  // TODO: C++23 std::unreachable()
  return replication_coordination_glue::ReplicationMode::ASYNC;
}
#endif

constexpr auto convertToReplicationMode(const ReplicationQuery::SyncMode &sync_mode)
    -> replication_coordination_glue::ReplicationMode {
  switch (sync_mode) {
    case ReplicationQuery::SyncMode::ASYNC: {
      return replication_coordination_glue::ReplicationMode::ASYNC;
    }
    case ReplicationQuery::SyncMode::SYNC: {
      return replication_coordination_glue::ReplicationMode::SYNC;
    }
    case ReplicationQuery::SyncMode::STRICT_SYNC: {
      return replication_coordination_glue::ReplicationMode::STRICT_SYNC;
    }
  }
  // TODO: C++23 std::unreachable()
  return replication_coordination_glue::ReplicationMode::ASYNC;
}

std::string PropertyPathToName(auto &&context, storage::PropertyPath const &property_path) {
  return property_path |
         rv::transform([&](storage::PropertyId property_id) { return context->PropertyToName(property_id); }) |
         rv::join('.') | r::to<std::string>();
};

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

      if (!handler_->TrySetReplicationRoleReplica(config)) {
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
    const auto repl_mode = convertToReplicationMode(sync_mode);

    auto maybe_endpoint = io::network::Endpoint::ParseAndCreateSocketOrAddress(
        socket_address, memgraph::replication::kDefaultReplicationPort);
    if (!maybe_endpoint) {
      throw QueryRuntimeException("Invalid socket address. {}", kSocketErrorExplanation);
    }

    const auto replication_config =
        replication::ReplicationClientConfig{.name = name,
                                             .mode = repl_mode,
                                             .repl_server_endpoint = std::move(*maybe_endpoint),  // don't resolve early
                                             .replica_check_frequency = replica_check_frequency,
                                             .ssl = std::nullopt};

    const auto error = handler_->TryRegisterReplica(replication_config);

    if (error.HasError()) {
      if (error.GetError() == RegisterReplicaError::NOT_MAIN) {
        throw QueryRuntimeException("Replica can't register another replica!");
      }

      throw QueryRuntimeException("Couldn't register replica {}. Error: {}", name,
                                  static_cast<uint8_t>(error.GetError()));
    }
  }

  /// @throw QueryRuntimeException if an error occurred.
  void DropReplica(std::string_view replica_name) {
    auto const result = handler_->UnregisterReplica(replica_name);
    switch (result) {
      using enum memgraph::query::UnregisterReplicaResult;
      case NO_ACCESS:
        throw QueryRuntimeException(
            "Failed to unregister replica due to lack of unique access over the cluster state. Please try again later "
            "on.");
      case NOT_MAIN:
        throw QueryRuntimeException(
            "Replica can't unregister a replica! Please rerun the query on main in order to unregister a replica from "
            "the cluster.");
      case COULD_NOT_BE_PERSISTED:
        [[fallthrough]];
      case CANNOT_UNREGISTER:
        throw QueryRuntimeException("Failed to unregister the replica {}.", replica_name);
      case SUCCESS:
        break;
    }
  }

  std::vector<ReplicasInfo> ShowReplicas() const {
    auto info = handler_->ShowReplicas();
    if (info.HasError()) {
      switch (info.GetError()) {
        case ShowReplicaError::NOT_MAIN:
          throw QueryRuntimeException("Show replicas query should only be run on the main instance.");
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
      case NO_MAIN:
        throw QueryRuntimeException(
            "The replica cannot be unregisted because the current main is down. Retry when the cluster has an active "
            "leader!");
      case NOT_COORDINATOR:
        throw QueryRuntimeException("UNREGISTER INSTANCE query can only be run on a coordinator!");
      case NOT_LEADER: {
        auto const maybe_leader_coordinator = coordinator_handler_.GetLeaderCoordinatorData();
        constexpr std::string_view common_message =
            "Couldn't unregister replica instance since coordinator is not a leader!";
        if (maybe_leader_coordinator) {
          throw QueryRuntimeException("{} Current leader is coordinator with id {} with bolt socket address {}",
                                      common_message, maybe_leader_coordinator->id,
                                      maybe_leader_coordinator->bolt_server);
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
        constexpr std::string_view common_message =
            "Couldn't demote instance to replica since coordinator is not a leader!";
        if (maybe_leader_coordinator) {
          throw QueryRuntimeException("{} Current leader is coordinator with id {} with bolt socket address {}",
                                      common_message, maybe_leader_coordinator->id,
                                      maybe_leader_coordinator->bolt_server);
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
      case NOT_LEADER_ANYMORE:
        throw QueryRuntimeException("Force reset failed since the instance is not leader anymore!");
      case SUCCESS:
        break;
    }
  }

  void YieldLeadership() override {
    switch (coordinator_handler_.YieldLeadership()) {
      case coordination::YieldLeadershipStatus::SUCCESS: {
        spdlog::info(
            "The request for yielding leadership was submitted successfully. Please monitor the cluster state with "
            "'SHOW INSTANCES' to see changes applied.");
        break;
      }
      case coordination::YieldLeadershipStatus::NOT_LEADER: {
        throw QueryRuntimeException("Only the current leader can yield the leadership!");
      }
    }
  }

  void SetCoordinatorSetting(std::string_view const setting_name, std::string_view const setting_value) override {
    switch (coordinator_handler_.SetCoordinatorSetting(setting_name, setting_value)) {
      case coordination::SetCoordinatorSettingStatus::SUCCESS: {
        spdlog::info("The request for updating coordinator setting was accepted by Raft storage.");
        break;
      }
      case coordination::SetCoordinatorSettingStatus::RAFT_LOG_ERROR: {
        throw QueryRuntimeException(
            "Raft storage didn't accept a configuration change. The most probable reason is that coordinators cannot "
            "form a consensus or that the currently active instance is not the leader.");
      }
      case coordination::SetCoordinatorSettingStatus::UNKNOWN_SETTING: {
        throw QueryRuntimeException("Setting {} doesn't exist on coordinators.", setting_name);
      }
      case coordination::SetCoordinatorSettingStatus::INVALID_ARGUMENT: {
        throw QueryRuntimeException("Invalid argument detected while trying to update setting {}", setting_name);
      }
    }
  }

  std::vector<std::pair<std::string, std::string>> ShowCoordinatorSettings() override {
    return coordinator_handler_.ShowCoordinatorSettings();
  }

  std::map<std::string, std::map<std::string, coordination::ReplicaDBLagData>> ShowReplicationLag() override {
    return coordinator_handler_.ShowReplicationLag();
  }

  void RegisterReplicationInstance(std::string_view bolt_server, std::string_view management_server,
                                   std::string_view replication_server, std::string_view instance_name,
                                   CoordinatorQuery::SyncMode sync_mode) override {
    auto const maybe_bolt_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(bolt_server);
    if (!maybe_bolt_server) {
      throw QueryRuntimeException("Invalid bolt socket address. {}", kSocketErrorExplanation);
    }

    auto const maybe_management_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(management_server);
    if (!maybe_management_server) {
      throw QueryRuntimeException("Invalid management socket address. {}", kSocketErrorExplanation);
    }

    auto const maybe_replication_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(replication_server);
    if (!maybe_replication_server) {
      throw QueryRuntimeException("Invalid replication socket address. {}", kSocketErrorExplanation);
    }

    auto const repl_config =
        coordination::ReplicationClientInfo{.instance_name = std::string(instance_name),
                                            .replication_mode = convertFromCoordinatorToReplicationMode(sync_mode),
                                            .replication_server = *maybe_replication_server};

    auto coordinator_client_config = coordination::DataInstanceConfig{.instance_name = std::string(instance_name),
                                                                      .mgt_server = *maybe_management_server,
                                                                      .bolt_server = *maybe_bolt_server,
                                                                      .replication_client_info = repl_config};

    switch (coordinator_handler_.RegisterReplicationInstance(coordinator_client_config)) {
      using enum coordination::RegisterInstanceCoordinatorStatus;
      case STRICT_SYNC_AND_SYNC_FORBIDDEN:
        throw QueryRuntimeException(
            "Cluster cannot consists of both STRICT_SYNC and SYNC replicas. The valid cluster consists of either "
            "STRICT_SYNC and ASYNC or SYNC and ASYNC replicas.");
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
                                      common_message, maybe_leader_coordinator->id,
                                      maybe_leader_coordinator->bolt_server);
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
      case SUCCESS:
        break;
    }
  }

  void RemoveCoordinatorInstance(int32_t coordinator_id) override {
    switch (coordinator_handler_.RemoveCoordinatorInstance(coordinator_id)) {
      using enum memgraph::coordination::RemoveCoordinatorInstanceStatus;  // NOLINT
      case NO_SUCH_ID:
        throw QueryRuntimeException(
            "Couldn't remove coordinator instance because coordinator with id {} doesn't exist!", coordinator_id);
      case SUCCESS:
        break;
    }
    spdlog::info("Removed coordinator {}.", coordinator_id);
  }

  auto AddCoordinatorInstance(int32_t coordinator_id, std::string_view bolt_server, std::string_view coordinator_server,
                              std::string_view management_server) -> void override {
    auto const maybe_coordinator_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(coordinator_server);
    if (!maybe_coordinator_server) {
      throw QueryRuntimeException("Invalid coordinator socket address. {}", kSocketErrorExplanation);
    }

    auto const maybe_management_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(management_server);
    if (!maybe_management_server) {
      throw QueryRuntimeException("Invalid management socket address. {}", kSocketErrorExplanation);
    }

    auto const maybe_bolt_server = io::network::Endpoint::ParseAndCreateSocketOrAddress(bolt_server);
    if (!maybe_bolt_server) {
      throw QueryRuntimeException("Invalid bolt socket address. {}", kSocketErrorExplanation);
    }

    auto const coord_coord_config =
        coordination::CoordinatorInstanceConfig{.coordinator_id = coordinator_id,
                                                .bolt_server = *maybe_bolt_server,
                                                .coordinator_server = *maybe_coordinator_server,
                                                .management_server = *maybe_management_server

        };

    switch (coordinator_handler_.AddCoordinatorInstance(coord_coord_config)) {
      using enum memgraph::coordination::AddCoordinatorInstanceStatus;  // NOLINT
      case ID_ALREADY_EXISTS:
        throw QueryRuntimeException("Couldn't add coordinator since instance with such id already exists!");
      case MGMT_ENDPOINT_ALREADY_EXISTS:
        throw QueryRuntimeException(
            "Couldn't add coordinator since instance with such management endpoint already exists!");
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
        constexpr std::string_view common_message = "Couldn't set instance to main since coordinator is not a leader!";
        if (maybe_leader_coordinator) {
          throw QueryRuntimeException("{} Current leader is coordinator with id {} with bolt socket address {}",
                                      common_message, maybe_leader_coordinator->id,
                                      maybe_leader_coordinator->bolt_server);
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
      case ENABLE_WRITING_FAILED:
        throw QueryRuntimeException("Instance promoted to MAIN, but couldn't enable writing to instance.");
      case SUCCESS:
        break;
    }
  }

  [[nodiscard]] coordination::InstanceStatus ShowInstance() const override {
    return coordinator_handler_.ShowInstance();
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
                         Interpreter &interpreter, std::optional<dbms::DatabaseAccess> db_acc) {
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
  std::string user_or_role = auth_query->user_or_role_;
  const bool if_not_exists = auth_query->if_not_exists_;
  std::string database = auth_query->database_;
  std::vector<AuthQuery::Privilege> privileges = auth_query->privileges_;
  auto role_databases = auth_query->role_databases_;
#ifdef MG_ENTERPRISE
  auto label_privileges = auth_query->label_privileges_;
  auto label_matching_modes = auth_query->label_matching_modes_;
  auto edge_type_privileges = auth_query->edge_type_privileges_;
  auto impersonation_targets = auth_query->impersonation_targets_;
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
                                  is_replica = interpreter_context->repl_state.ReadLock()->IsReplica()]() {
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
            throw UserAlreadyExistsException(
                "User or role with name '{}' already exists. Use the SHOW USERS or SHOW ROLES query to list all users "
                "and roles. In addition you can rerun the current query with IF NOT EXISTS.",
                username);
          }
          spdlog::warn("User '{}' already exists.", username);
        }

        // If the license is not valid we create users with admin access
        if (!valid_enterprise_license) {
          spdlog::warn(
              "Granting all the privileges to {}. You're currently using Memgraph Community License and all the users "
              "will have full privileges to access Memgraph database. If you want to ensure privileges are applied, "
              "please add Memgraph Enterprise License and restart Memgraph for the configuration to apply.",
              username);
          auth->GrantPrivilege(
              username, kPrivilegesAll
#ifdef MG_ENTERPRISE
              ,
              {{{AuthQuery::FineGrainedPrivilege::CREATE, {query::kAsterisk}},
                {AuthQuery::FineGrainedPrivilege::DELETE, {query::kAsterisk}}}},
              {AuthQuery::LabelMatchingMode::ANY},  // matching mode for label privileges
              {
                {
                  {AuthQuery::FineGrainedPrivilege::CREATE, {query::kAsterisk}}, {
                    AuthQuery::FineGrainedPrivilege::DELETE, { query::kAsterisk }
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
          throw QueryRuntimeException(
              "User with username '{}' doesn't exist. A new user can be created via the CREATE USER query.", username);
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
      callback.fn = [auth, roles = std::move(auth_query->roles_), if_not_exists, interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        if (roles.empty()) {
          throw QueryRuntimeException("No role name provided for CREATE ROLE");
        }
        const std::string &rolename = roles[0];

        if (!auth->CreateRole(rolename, &*interpreter->system_transaction_)) {
          if (!if_not_exists) {
            throw QueryRuntimeException(
                "Role or user with name '{}' already exists. Use the SHOW ROLES or SHOW USERS query to list all roles "
                "and users. In addition you can rerun the current query with IF NOT EXISTS.",
                rolename);
          }
          spdlog::warn("Role '{}' already exists.", rolename);
        }
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DROP_ROLE:
      forbid_on_replica();
      callback.fn = [auth, roles = std::move(auth_query->roles_), interpreter = &interpreter] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        if (roles.empty()) {
          throw QueryRuntimeException("No role name provided for DROP ROLE");
        }
        const std::string &rolename = roles[0];

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
    case AuthQuery::Action::SHOW_CURRENT_ROLE:
      callback.header = {"role"};
      callback.fn = [&interpreter
#ifdef MG_ENTERPRISE
                     ,
                     db_acc = std::move(db_acc)
#endif
      ] {
#ifdef MG_ENTERPRISE
        if (db_acc && db_acc.value()->name() != dbms::kDefaultDB &&
            !license::global_license_checker.IsEnterpriseValidFast()) {
          throw QueryRuntimeException("Multi-database queries are only available in enterprise edition");
        }
        const auto &rolenames =
            interpreter.user_or_role_->GetRolenames(db_acc ? std::make_optional(db_acc.value()->name()) : std::nullopt);
#else
        const auto &rolenames = interpreter.user_or_role_->GetRolenames(std::nullopt);
#endif
        if (rolenames.empty()) {
          return std::vector<std::vector<TypedValue>>{{TypedValue()}};
        }
        std::vector<std::vector<TypedValue>> rows;
        rows.reserve(rolenames.size());
        for (const auto &rolename : rolenames) {
          rows.emplace_back(std::vector<TypedValue>{TypedValue{rolename}});
        }
        return rows;
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

      callback.fn = [auth, username, roles = std::move(auth_query->roles_), interpreter = &interpreter,
                     role_databases = std::move(role_databases)] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }
#ifdef MG_ENTERPRISE
        auth->SetRoles(username, roles, role_databases, &*interpreter->system_transaction_);
#else
        if (!role_databases.empty()) {
          throw QueryException("Database specification is only available in the enterprise edition");
        }
        auth->SetRoles(username, roles, std::unordered_set<std::string>{}, &*interpreter->system_transaction_);
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::CLEAR_ROLE:
      forbid_on_replica();
      callback.fn = [auth, username, interpreter = &interpreter, role_databases = std::move(role_databases)] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

#ifdef MG_ENTERPRISE
        auth->ClearRoles(username, role_databases, &*interpreter->system_transaction_);
#else
        if (!role_databases.empty()) {
          throw QueryException("Database specification is only available in the enterprise edition");
        }
        auth->ClearRoles(username, std::unordered_set<std::string>{}, &*interpreter->system_transaction_);
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::GRANT_PRIVILEGE:
      forbid_on_replica();
      callback.fn = [auth, user_or_role, privileges, interpreter = &interpreter
#ifdef MG_ENTERPRISE
                     ,
                     label_privileges, label_matching_modes, edge_type_privileges
#endif
      ] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        auth->GrantPrivilege(user_or_role, privileges
#ifdef MG_ENTERPRISE
                             ,
                             label_privileges, label_matching_modes, edge_type_privileges
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
                     label_privileges, label_matching_modes, edge_type_privileges
#endif
      ] {
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }

        auth->RevokePrivilege(user_or_role, privileges
#ifdef MG_ENTERPRISE
                              ,
                              label_privileges, label_matching_modes, edge_type_privileges
#endif
                              ,
                              &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case AuthQuery::Action::SHOW_PRIVILEGES:
      callback.header = {"privilege", "effective", "description"};
      callback.fn = [auth, user_or_role, database_specification = auth_query->database_specification_
#ifdef MG_ENTERPRISE
                     ,
                     db_acc = std::move(db_acc), database_name = auth_query->database_, db_handler
#endif
      ] {
#ifdef MG_ENTERPRISE
        if (!license::global_license_checker.IsEnterpriseValidFast()) {
          if (database_specification != AuthQuery::DatabaseSpecification::NONE) {
            throw QueryRuntimeException("Multi-database queries are only available in enterprise edition");
          }
          return auth->GetPrivileges(user_or_role, std::nullopt);
        }
        std::optional<std::string> target_db;
        switch (database_specification) {
          case AuthQuery::DatabaseSpecification::NONE:
            // Allow only if there are no other databases (or for roles)
            // Roles themselves cannot have MT specializations, so no need to filter for them (nullopt)
            // Users can have MT specializations, so we force them to specify the database
            if (db_handler->Count() > 1 && !auth->HasRole(user_or_role)) {
              throw QueryRuntimeException(
                  "In a multi-tenant environment, SHOW PRIVILEGES query requires database specification. Use ON MAIN, "
                  "ON CURRENT or ON DATABASE db_name.");
            }
            target_db = dbms::kDefaultDB;  // HOTFIX: REMOVE ONCE MASTER IS FIXED
            break;
          case AuthQuery::DatabaseSpecification::MAIN: {
            auto main_db = auth->GetMainDatabase(user_or_role);
            if (!main_db) {
              throw QueryRuntimeException("No user found for SHOW PRIVILEGES ON MAIN");
            }
            target_db = main_db.value();
          } break;
          case AuthQuery::DatabaseSpecification::CURRENT:
            if (!db_acc) {
              throw QueryRuntimeException("No current database for SHOW PRIVILEGES ON CURRENT");
            }
            target_db = db_acc.value()->name();
            break;
          case AuthQuery::DatabaseSpecification::DATABASE:
            target_db = database_name;
            break;
        }
        std::optional<dbms::DatabaseAccess> target_db_acc;
        if (target_db) {
          // Check that the db exists
          target_db_acc = db_handler->Get(*target_db);
        }
        return auth->GetPrivileges(user_or_role, target_db);
#else
        if (database_specification != AuthQuery::DatabaseSpecification::NONE) {
          throw QueryRuntimeException("Multi-database queries are only available in enterprise edition");
        }
        return auth->GetPrivileges(user_or_role, std::nullopt);
#endif
      };
      return callback;
    case AuthQuery::Action::SHOW_ROLE_FOR_USER:
      callback.header = std::vector<std::string>{"role"};
      callback.fn = [auth, username, database_specification = auth_query->database_specification_
#ifdef MG_ENTERPRISE
                     ,
                     db_acc = std::move(db_acc), database_name = auth_query->database_, db_handler
#endif
      ] {
#ifdef MG_ENTERPRISE
        if (database_specification != AuthQuery::DatabaseSpecification::NONE &&
            !license::global_license_checker.IsEnterpriseValidFast()) {
          throw QueryRuntimeException("Multi-database queries are only available in enterprise edition");
        }
        std::optional<std::string> target_db;
        switch (database_specification) {
          case AuthQuery::DatabaseSpecification::NONE:
            break;
          case AuthQuery::DatabaseSpecification::MAIN: {
            auto main_db = auth->GetMainDatabase(username);
            if (!main_db) {
              throw QueryRuntimeException("No user found!");
            }
            target_db = main_db.value();
          } break;
          case AuthQuery::DatabaseSpecification::CURRENT:
            if (!db_acc) {
              throw QueryRuntimeException("No current database!");
            }
            target_db = db_acc.value()->name();
            break;
          case AuthQuery::DatabaseSpecification::DATABASE:
            target_db = database_name;
            break;
        }
        std::optional<dbms::DatabaseAccess> target_db_acc;
        if (target_db) {
          // Check that the db exists
          target_db_acc = db_handler->Get(target_db.value());
        }
        auto rolenames = auth->GetRolenamesForUser(username, target_db);
#else
        if (database_specification != AuthQuery::DatabaseSpecification::NONE) {
          throw QueryRuntimeException("Multi-database queries are only available in enterprise edition");
        }
        auto rolenames = auth->GetRolenamesForUser(username, std::nullopt);
#endif
        if (rolenames.empty()) {
          return std::vector<std::vector<TypedValue>>{std::vector<TypedValue>{TypedValue("null")}};
        }
        std::vector<std::vector<TypedValue>> rows;
        rows.reserve(rolenames.size());
        for (auto &&rolename : rolenames) {
          rows.emplace_back(std::vector<TypedValue>{TypedValue{rolename}});
        }
        return rows;
      };
      return callback;
    case AuthQuery::Action::SHOW_USERS_FOR_ROLE:
      callback.header = {"users"};
      callback.fn = [auth, roles = std::move(auth_query->roles_)] {
        if (roles.empty()) {
          throw QueryRuntimeException("No role name provided for SHOW USERS FOR ROLE");
        }
        const std::string &rolename = roles[0];

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
          auth->GrantDatabase(database, username,
                              &*interpreter->system_transaction_);  // Can throws query exception
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
          auth->RevokeDatabase(database, username,
                               &*interpreter->system_transaction_);  // Can throws query exception
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
          auth->SetMainDatabase(database, username,
                                &*interpreter->system_transaction_);  // Can throws query exception
        } catch (memgraph::dbms::UnknownDatabaseException &e) {
          throw QueryRuntimeException(e.what());
        }
#else
      callback.fn = [] {
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::GRANT_IMPERSONATE_USER:
      if (!license::global_license_checker.IsEnterpriseValidFast()) {
        throw QueryRuntimeException(
            license::LicenseCheckErrorToString(license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "impersonate user"));
      }
      forbid_on_replica();
#ifdef MG_ENTERPRISE
      callback.fn = [auth, user_or_role = std::move(user_or_role), targets = std::move(impersonation_targets),
                     interpreter = &interpreter] {  // NOLINT
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }
        try {
          auth->GrantImpersonateUser(user_or_role, targets,
                                     &*interpreter->system_transaction_);  // Can throws query exception
        } catch (memgraph::dbms::UnknownDatabaseException &e) {
          throw QueryRuntimeException(e.what());
        }
#else
      callback.fn = [] {
#endif
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    case AuthQuery::Action::DENY_IMPERSONATE_USER:
      if (!license::global_license_checker.IsEnterpriseValidFast()) {
        throw QueryRuntimeException(
            license::LicenseCheckErrorToString(license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "impersonate user"));
      }
      forbid_on_replica();
#ifdef MG_ENTERPRISE
      callback.fn = [auth, user_or_role = std::move(user_or_role), targets = std::move(impersonation_targets),
                     interpreter = &interpreter] {  // NOLINT
        if (!interpreter->system_transaction_) {
          throw QueryException("Expected to be in a system transaction");
        }
        try {
          auth->DenyImpersonateUser(user_or_role, targets,
                                    &*interpreter->system_transaction_);  // Can throws query exception
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
  }
}

Callback HandleReplicationInfoQuery(ReplicationInfoQuery *repl_query,
                                    ReplicationQueryHandler &replication_query_handler) {
  Callback callback;
  switch (repl_query->action_) {
    case ReplicationInfoQuery::Action::SHOW_REPLICATION_ROLE: {
      callback.header = {"replication role"};
      callback.fn = [handler = ReplQueryHandler{replication_query_handler}] {
        const auto mode = handler.ShowReplicationRole();
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
    case ReplicationInfoQuery::Action::SHOW_REPLICAS: {
      bool full_info = false;
#ifdef MG_ENTERPRISE
      full_info = license::global_license_checker.IsEnterpriseValidFast();
#endif

      callback.header = {"name", "socket_address", "sync_mode", "system_info", "data_info"};

      callback.fn = [handler = ReplQueryHandler{replication_query_handler}, replica_nfields = callback.header.size(),
                     full_info] {
        auto const sync_mode_to_tv = [](replication_coordination_glue::ReplicationMode sync_mode) {
          using namespace std::string_view_literals;
          switch (sync_mode) {
            using enum replication_coordination_glue::ReplicationMode;
            case SYNC:
              return TypedValue{"sync"sv};
            case ASYNC:
              return TypedValue{"async"sv};
            case STRICT_SYNC:
              return TypedValue{"strict_sync"sv};
          }
        };

        auto const replica_sys_state_to_tv = [](replication::ReplicationClient::State state) {
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
          info.emplace("behind", TypedValue{/*orig.behind_*/});
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
          info.emplace("behind", TypedValue{orig.behind_});
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

        const auto replicas = handler.ShowReplicas();
        auto typed_replicas = std::vector<std::vector<TypedValue>>{};
        typed_replicas.reserve(replicas.size());
        for (const auto &replica : replicas) {
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

  return rv::all(config_map) | rv::transform([&evaluator](const auto &entry) {
           auto key_expr = entry.first->Accept(evaluator);
           auto value_expr = entry.second->Accept(evaluator);
           return std::pair{key_expr.ValueString(), value_expr.ValueString()};
         }) |
         ranges::to<std::map<std::string, std::string, std::less<>>>;
}

Callback HandleCoordinatorQuery(CoordinatorQuery *coordinator_query, const Parameters &parameters,
                                coordination::CoordinatorState *coordinator_state,
                                const query::InterpreterConfig &config, std::vector<Notification> *notifications) {
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryRuntimeException(
        license::LicenseCheckErrorToString(license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "high availability"));
  }

  Callback callback;
  switch (coordinator_query->action_) {
    case CoordinatorQuery::Action::REMOVE_COORDINATOR_INSTANCE: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can remove coordinator instance!");
      }

      // TODO: MemoryResource for EvaluationContext, it should probably be passed as
      // the argument to Callback.
      EvaluationContext const evaluation_context{.timestamp = QueryTimestamp(), .parameters = parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

      auto coord_server_id = coordinator_query->coordinator_id_->Accept(evaluator).ValueInt();

      callback.fn = [handler = CoordQueryHandler{*coordinator_state}, coord_server_id]() mutable {
        handler.RemoveCoordinatorInstance(static_cast<int>(coord_server_id));
        return std::vector<std::vector<TypedValue>>();
      };

      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::REMOVE_COORDINATOR_INSTANCE,
                                  fmt::format("Coordinator {} has been removed.", coord_server_id));
      return callback;
    }

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

      callback.fn =
          [handler = CoordQueryHandler{*coordinator_state}, bolt_server = bolt_server_it->second,
           management_server = management_server_it->second, replication_server = replication_server_it->second,
           instance_name = coordinator_query->instance_name_, sync_mode = coordinator_query->sync_mode_]() mutable {
            handler.RegisterReplicationInstance(bolt_server, management_server, replication_server, instance_name,
                                                sync_mode);
            return std::vector<std::vector<TypedValue>>();
          };

      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::REGISTER_REPLICATION_INSTANCE,
                                  fmt::format("Coordinator has registered replication instance on {} for instance {}.",
                                              bolt_server_it->second, coordinator_query->instance_name_));
      return callback;
    }
    case CoordinatorQuery::Action::UNREGISTER_INSTANCE: {
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
    }
    case CoordinatorQuery::Action::DEMOTE_INSTANCE: {
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
    }
    case CoordinatorQuery::Action::FORCE_RESET_CLUSTER_STATE: {
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
    }
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
      callback.fn = [handler = CoordQueryHandler{*coordinator_state}]() mutable {
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
    case CoordinatorQuery::Action::SHOW_INSTANCE: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can run SHOW INSTANCE query.");
      }

      callback.header = {"name", "bolt_server", "coordinator_server", "management_server", "role"};
      callback.fn = [handler = CoordQueryHandler{*coordinator_state}]() mutable {
        auto const instance = handler.ShowInstance();
        std::vector<std::vector<TypedValue>> results;
        auto instance_result = std::vector{
            TypedValue{instance.instance_name},      TypedValue{instance.bolt_server},
            TypedValue{instance.coordinator_server}, TypedValue{instance.management_server},
            TypedValue{instance.cluster_role},

        };
        results.push_back(std::move(instance_result));
        return results;
      };
      return callback;
    }
    case CoordinatorQuery::Action::YIELD_LEADERSHIP: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can run YIELD LEADERSHIP query.");
      }
      callback.fn = [handler = CoordQueryHandler{*coordinator_state}]() mutable {
        handler.YieldLeadership();
        return std::vector<std::vector<TypedValue>>();
      };
      notifications->emplace_back(SeverityLevel::INFO, NotificationCode::YIELD_LEADERSHIP,
                                  fmt::format("The coordinator has tried to yield the current leadership."));

      return callback;
    }
    case CoordinatorQuery::Action::SET_COORDINATOR_SETTING: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can run SET COORDINATOR SETTING query.");
      }
      EvaluationContext evaluation_context{.timestamp = QueryTimestamp(), .parameters = parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
      const auto setting_name = EvaluateOptionalExpression(coordinator_query->setting_name_, evaluator);

      if (!setting_name.IsString()) {
        throw utils::BasicException("Setting name should be a string literal");
      }

      const auto setting_value = EvaluateOptionalExpression(coordinator_query->setting_value_, evaluator);
      if (!setting_value.IsString()) {
        throw utils::BasicException("Setting value should be a string literal");
      }

      callback.fn = [handler = CoordQueryHandler{*coordinator_state},
                     setting_name = std::string{setting_name.ValueString()},
                     setting_value = std::string{setting_value.ValueString()}]() mutable {
        handler.SetCoordinatorSetting(setting_name, setting_value);
        return std::vector<std::vector<TypedValue>>();
      };
      return callback;
    }
    case CoordinatorQuery::Action::SHOW_COORDINATOR_SETTINGS: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can run SHOW COORDINATOR SETTINGS query.");
      }
      callback.header = {"setting_name", "setting_value"};

      callback.fn = [handler = CoordQueryHandler{*coordinator_state}]() mutable {
        auto const coord_settings = handler.ShowCoordinatorSettings();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(coord_settings.size());

        for (const auto &[k, v] : coord_settings) {
          spdlog::info("Setting name: {} Setting value: {}", k, v);
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
    case CoordinatorQuery::Action::SHOW_REPLICATION_LAG: {
      if (!coordinator_state->IsCoordinator()) {
        throw QueryRuntimeException("Only coordinator can run SHOW REPLICATION LAG query.");
      }
      callback.header = {"instance_name", "data_info"};
      callback.fn = [handler = CoordQueryHandler{*coordinator_state}]() mutable {
        auto const lag_info = handler.ShowReplicationLag();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(lag_info.size());

        auto const db_lag_data_to_tv = [](coordination::ReplicaDBLagData orig) {
          auto info = std::map<std::string, TypedValue>{};
          info.emplace("num_committed_txns", TypedValue{static_cast<int64_t>(orig.num_committed_txns_)});
          info.emplace("num_txns_behind_main", TypedValue{static_cast<int64_t>(orig.num_txns_behind_main_)});
          return TypedValue{std::move(info)};
        };

        auto const instance_info_to_tv =
            [&db_lag_data_to_tv](std::map<std::string, coordination::ReplicaDBLagData> const &instance_info)
            -> std::map<std::string, TypedValue> {
          auto info = std::map<std::string, TypedValue>{};
          for (auto const &[db_name, db_lag_data] : instance_info) {
            info.emplace(db_name, db_lag_data_to_tv(db_lag_data));
          }
          return info;
        };

        for (auto const &[instance_name, data_info] : lag_info) {
          std::vector<TypedValue> instance_out_info;
          instance_out_info.reserve(2);
          instance_out_info.emplace_back(instance_name);
          instance_out_info.emplace_back(instance_info_to_tv(data_info));
          results.push_back(std::move(instance_out_info));
        }
        return results;
      };
      return callback;
    }
  }
}
#endif

auto ParseVectorIndexConfigMap(std::unordered_map<query::Expression *, query::Expression *> const &config_map,
                               ExpressionVisitor<TypedValue> &evaluator) -> storage::VectorIndexConfigMap {
  if (config_map.empty()) {
    throw std::invalid_argument(
        "Vector index config map is empty. Please provide mandatory fields: dimension and capacity.");
  }

  auto transformed_map = rv::all(config_map) | rv::transform([&evaluator](const auto &pair) {
                           auto key_expr = pair.first->Accept(evaluator);
                           auto value_expr = pair.second->Accept(evaluator);
                           return std::pair{key_expr.ValueString(), value_expr};
                         }) |
                         ranges::to<std::map<std::string, query::TypedValue, std::less<>>>;

  auto metric_kind_it = transformed_map.find(kMetric);
  auto metric_kind = storage::MetricFromName(
      metric_kind_it != transformed_map.end() ? metric_kind_it->second.ValueString() : kDefaultMetric);
  auto dimension = transformed_map.find(kDimension);
  if (dimension == transformed_map.end()) {
    throw std::invalid_argument("Vector index spec must have a 'dimension' field.");
  }
  auto dimension_value = static_cast<std::uint16_t>(dimension->second.ValueInt());

  auto capacity = transformed_map.find(kCapacity);
  if (capacity == transformed_map.end()) {
    throw std::invalid_argument("Vector index spec must have a 'capacity' field.");
  }
  auto capacity_value = static_cast<std::size_t>(capacity->second.ValueInt());

  auto resize_coefficient_it = transformed_map.find(kResizeCoefficient);
  auto resize_coefficient =
      resize_coefficient_it != transformed_map.end() && resize_coefficient_it->second.ValueInt() > 0
          ? static_cast<std::uint16_t>(resize_coefficient_it->second.ValueInt())
          : kDefaultResizeCoefficient;
  auto scalar_kind_it = transformed_map.find(kScalarKind);
  auto scalar_kind = storage::ScalarFromName(
      scalar_kind_it != transformed_map.end() ? scalar_kind_it->second.ValueString() : kDefaultScalarKind);
  return storage::VectorIndexConfigMap{metric_kind, dimension_value, capacity_value, resize_coefficient, scalar_kind};
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
  auto owner = interpreter_context->auth_checker->GenQueryUser(user_or_role->username(), user_or_role->rolenames());

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
  auto owner = interpreter_context->auth_checker->GenQueryUser(user_or_role->username(), user_or_role->rolenames());

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
          if (db_acc.is_marked_for_deletion()) {
            throw QueryException("Can not start stream while database is being dropped.");
          }
          streams->StartWithLimit(stream_name, static_cast<uint64_t>(batch_limit.value()), timeout);
          return std::vector<std::vector<TypedValue>>{};
        };
      } else {
        callback.fn = [db_acc, streams = db_acc->streams(), stream_name = stream_query->stream_name_]() {
          if (db_acc.is_marked_for_deletion()) {
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
  explicit PullPlan(
      std::shared_ptr<PlanWrapper> plan, const Parameters &parameters, bool is_profile_query, DbAccessor *dba,
      InterpreterContext *interpreter_context, utils::MemoryResource *execution_memory,
      std::shared_ptr<QueryUserOrRole> user_or_role, StoppingContext stopping_context,
      storage::DatabaseProtectorPtr protector, std::optional<QueryLogger> &query_logger,
      TriggerContextCollector *trigger_context_collector = nullptr, std::optional<size_t> memory_limit = {},
      FrameChangeCollector *frame_change_collector_ = nullptr, std::optional<int64_t> hops_limit = {}
#ifdef MG_ENTERPRISE
      ,
      std::shared_ptr<utils::UserResources> user_resource = {}
#endif
  );

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
#ifdef MG_ENTERPRISE
  std::shared_ptr<utils::UserResources> user_resource_{};
#endif
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
                   std::shared_ptr<QueryUserOrRole> user_or_role, StoppingContext stopping_context,
                   storage::DatabaseProtectorPtr protector, std::optional<QueryLogger> &query_logger,
                   TriggerContextCollector *trigger_context_collector, const std::optional<size_t> memory_limit,
                   FrameChangeCollector *frame_change_collector, const std::optional<int64_t> hops_limit
#ifdef MG_ENTERPRISE
                   ,
                   std::shared_ptr<utils::UserResources> user_resource
#endif
                   )
    : plan_(plan),
      cursor_(plan->plan().MakeCursor(execution_memory)),
      frame_(plan->symbol_table().max_position(), execution_memory),
      memory_limit_(memory_limit),
      query_logger_(query_logger)
#ifdef MG_ENTERPRISE
      ,
      user_resource_ {
  std::move(user_resource)
}
#endif
{
  ctx_.hops_limit = query::HopsLimit{hops_limit};
  ctx_.db_accessor = dba;
  ctx_.symbol_table = plan->symbol_table();
  ctx_.evaluation_context.timestamp = QueryTimestamp();
  ctx_.evaluation_context.parameters = parameters;
  ctx_.evaluation_context.properties = NamesToProperties(plan->ast_storage().properties_, dba);
  ctx_.evaluation_context.labels = NamesToLabels(plan->ast_storage().labels_, dba);
  ctx_.evaluation_context.edgetypes = NamesToEdgeTypes(plan->ast_storage().edge_types_, dba);
  ctx_.user_or_role = user_or_role;
#ifdef MG_ENTERPRISE
  if (license::global_license_checker.IsEnterpriseValidFast() && user_or_role && *user_or_role && dba) {
    // Create only if an explicit user is defined
    auto auth_checker = interpreter_context->auth_checker->GetFineGrainedAuthChecker(user_or_role, dba);

    // if the user has global privileges to read, edit and write anything, we don't need to perform authorization
    // otherwise, we do assign the auth checker to check for label access control
    if (!auth_checker->HasAllGlobalPrivilegesOnVertices() || !auth_checker->HasAllGlobalPrivilegesOnEdges()) {
      ctx_.auth_checker = std::move(auth_checker);
    }
  }
#endif
  ctx_.stopping_context = std::move(stopping_context);
  ctx_.is_profile_query = is_profile_query;
  ctx_.trigger_context_collector = trigger_context_collector;
  ctx_.frame_change_collector = frame_change_collector;
  ctx_.evaluation_context.memory = execution_memory;
  ctx_.protector = std::move(protector);
  ctx_.is_main = interpreter_context->repl_state.ReadLock()->IsMain();
}

std::optional<plan::ProfilingStatsWithTotalTime> PullPlan::Pull(AnyStream *stream, std::optional<int> n,
                                                                const std::vector<Symbol> &output_symbols,
                                                                std::map<std::string, TypedValue> *summary) {
  auto &memory_tracker = ctx_.db_accessor->GetTransactionMemoryTracker();
  // Single query memory limit
  memory_tracker.SetQueryLimit(memory_limit_ ? *memory_limit_ : memgraph::memory::UNLIMITED_MEMORY);
  if (memory_limit_) memgraph::memory::StartTrackingCurrentThread(&memory_tracker);
#ifdef MG_ENTERPRISE
  // User-specific resource monitoring
  if (user_resource_ &&
      user_resource_->GetTransactionsMemory().second != utils::TransactionsMemoryResource::kUnlimited) {
    memgraph::memory::StartTrackingCurrentThread(&memory_tracker);  // Needs the query tracker for accurate tracking
    memgraph::memory::StartTrackingUserResource(user_resource_.get());
  }
#endif

  {  // Limiting scope of memory tracking
    auto reset_query_limit = utils::OnScopeExit{[]() {
      // Stopping tracking of transaction occurs in interpreter::pull
      // Exception can occur so we need to handle that case there.
      // We can't stop tracking here as there can be multiple pulls
      // so we need to take care of that after everything was pulled
      memgraph::memory::StopTrackingCurrentThread();
#ifdef MG_ENTERPRISE
      // User-specific resource monitoring
      memgraph::memory::StopTrackingUserResource();
#endif
    }};

    // Returns true if a result was pulled.
    const auto pull_result = [&]() -> bool { return cursor_->Pull(frame_, ctx_); };

    auto values = std::vector<TypedValue>(output_symbols.size());
    const auto stream_values = [&] {
      for (auto const i : rv::iota(0UL, output_symbols.size())) {
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
  }

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

void AccessorCompliance(PlanWrapper &plan, DbAccessor &dba) {
  const auto rw_type = plan.rw_type();
  if (rw_type == RWType::W || rw_type == RWType::RW) {
    if (dba.type() != storage::StorageAccessType::WRITE) {
      throw QueryRuntimeException("Accessor type {} and query type {} are misaligned!", dba.type(), rw_type);
    }
  }
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

PreparedQuery Interpreter::PrepareTransactionQuery(Interpreter::TransactionQuery tx_query_enum,
                                                   QueryExtras const &extras) {
  std::function<void()> handler;

  switch (tx_query_enum) {
    case TransactionQuery::BEGIN: {
      // TODO: Evaluate doing move(extras). Currently the extras is very small, but this will be important if it ever
      // becomes large.
      handler = [this, extras = extras] {
        if (in_explicit_transaction_) {
          throw ExplicitTransactionUsageException("Nested transactions are not supported.");
        }
        SetupInterpreterTransaction(extras);
        // Multiple paths can lead to transaction BEGIN, so we need to reset the timer in the handler
        current_timeout_timer_ = CreateTimeoutTimer(extras, interpreter_context_->config);
        in_explicit_transaction_ = true;
        expect_rollback_ = false;
        if (!current_db_.db_acc_)
          throw DatabaseContextRequiredException("No current database for transaction defined.");
        SetupDatabaseTransaction(true,
                                 extras.is_read ? storage::StorageAccessType::READ : storage::StorageAccessType::WRITE);
      };
    } break;
    case TransactionQuery::COMMIT: {
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
    } break;
    case TransactionQuery::ROLLBACK: {
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
    } break;
    default:
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

PreparedQuery PrepareCypherQuery(
    ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary, InterpreterContext *interpreter_context,
    CurrentDB &current_db, utils::MemoryResource *execution_memory, std::vector<Notification> *notifications,
    std::shared_ptr<QueryUserOrRole> user_or_role, StoppingContext stopping_context, Interpreter &interpreter,
    FrameChangeCollector *frame_change_collector = nullptr
#ifdef MG_ENTERPRISE
    ,
    std::shared_ptr<utils::UserResources> user_resource = {}
#endif
) {
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

  auto plan = CypherQueryToPlan(parsed_query.stripped_query, std::move(parsed_query.ast_storage), cypher_query,
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

  PrepareCaching(plan->ast_storage(), frame_change_collector);
  summary->insert_or_assign("cost_estimate", plan->cost());
  interpreter.LogQueryMessage(fmt::format("Plan cost: {}", plan->cost()));
  bool is_profile_query = false;
  if (interpreter.IsQueryLoggingActive()) {
    is_profile_query = true;
  }
  AccessorCompliance(*plan, *dba);
  const auto rw_type = plan->rw_type();
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
      std::move(user_or_role), std::move(stopping_context), dbms::DatabaseProtector{*current_db.db_acc_}.clone(),
      interpreter.query_logger_, trigger_context_collector, memory_limit,
      frame_change_collector->AnyCaches() ? frame_change_collector : nullptr, hops_limit
#ifdef MG_ENTERPRISE
      ,
      user_resource
#endif
  );
  return PreparedQuery{std::move(header),
                       std::move(parsed_query.required_privileges),
                       [pull_plan = std::move(pull_plan), output_symbols = std::move(output_symbols), summary](
                           AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
                         if (pull_plan->Pull(stream, n, output_symbols, summary)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       rw_type,
                       current_db.db_acc_->get()->name(),
                       utils::Priority::LOW};  // Default to LOW priority for all Cypher queries
}

PreparedQuery PrepareExplainQuery(ParsedQuery parsed_query, std::map<std::string, TypedValue> *summary,
                                  std::vector<Notification> *notifications, InterpreterContext *interpreter_context,
                                  Interpreter &interpreter, CurrentDB &current_db) {
  const std::string kExplainQueryStart = "explain ";
  MG_ASSERT(
      utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.stripped_query().str()), kExplainQueryStart),
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
      CypherQueryToPlan(parsed_inner_query.stripped_query, std::move(parsed_inner_query.ast_storage), cypher_query,
                        parsed_inner_query.parameters, plan_cache, dba);

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

PreparedQuery PrepareProfileQuery(
    ParsedQuery parsed_query, bool in_explicit_transaction, std::map<std::string, TypedValue> *summary,
    std::vector<Notification> *notifications, InterpreterContext *interpreter_context, Interpreter &interpreter,
    CurrentDB &current_db, utils::MemoryResource *execution_memory, std::shared_ptr<QueryUserOrRole> user_or_role,
    StoppingContext stopping_context, FrameChangeCollector *frame_change_collector
#ifdef MG_ENTERPRISE
    ,
    std::shared_ptr<utils::UserResources> user_resource = {}
#endif
) {
  const std::string kProfileQueryStart = "profile ";

  MG_ASSERT(
      utils::StartsWith(utils::ToLowerCase(parsed_query.stripped_query.stripped_query().str()), kProfileQueryStart),
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
      CypherQueryToPlan(parsed_inner_query.stripped_query, std::move(parsed_inner_query.ast_storage), cypher_query,
                        parsed_inner_query.parameters, plan_cache, dba);
  PrepareCaching(cypher_query_plan->ast_storage(), frame_change_collector);

  auto hints = plan::ProvidePlanHints(&cypher_query_plan->plan(), cypher_query_plan->symbol_table());
  for (const auto &hint : hints) {
    notifications->emplace_back(SeverityLevel::INFO, NotificationCode::PLAN_HINTING, hint);
    interpreter.LogQueryMessage(hint);
  }
  AccessorCompliance(*cypher_query_plan, *dba);
  const auto rw_type = cypher_query_plan->rw_type();

  return PreparedQuery{
      {"OPERATOR", "ACTUAL HITS", "RELATIVE TIME", "ABSOLUTE TIME"},
      std::move(parsed_query.required_privileges),
      [plan = std::move(cypher_query_plan), parameters = std::move(parsed_inner_query.parameters), summary, dba,
       interpreter_context, execution_memory, memory_limit, user_or_role = std::move(user_or_role),
       // We want to execute the query we are profiling lazily, so we delay
       // the construction of the corresponding context.
       stats_and_total_time = std::optional<plan::ProfilingStatsWithTotalTime>{},
       pull_plan = std::shared_ptr<PullPlanVector>(nullptr), frame_change_collector,
       stopping_context = std::move(stopping_context), db_acc = *current_db.db_acc_, hops_limit,
       &query_logger = interpreter.query_logger_
#ifdef MG_ENTERPRISE
       ,
       user_resource = std::move(user_resource)
#endif
  ](AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        // No output symbols are given so that nothing is streamed.
        if (!stats_and_total_time) {
          stats_and_total_time =
              PullPlan(plan, parameters, true, dba, interpreter_context, execution_memory, std::move(user_or_role),
                       std::move(stopping_context), dbms::DatabaseProtector{db_acc}.clone(), query_logger, nullptr,
                       memory_limit, frame_change_collector->AnyInListCaches() ? frame_change_collector : nullptr,
                       hops_limit
#ifdef MG_ENTERPRISE
                       ,
                       user_resource
#endif
                       )
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
      rw_type};
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
  using LPIndex = std::pair<storage::LabelId, std::vector<storage::PropertyPath>>;
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

  auto populate_label_property_stats = [execution_db_accessor, view](auto index_info) {
    std::map<LPIndex, std::map<std::vector<storage::PropertyValue>, int64_t>> label_property_counter;
    std::map<LPIndex, uint64_t> vertex_degree_counter;

    auto const count_vertex_prop_info = [&](LPIndex const &key) {
      struct StatsByPrefix {
        std::map<std::vector<storage::PropertyValue>, int64_t> *properties_value_counter;
        uint64_t *vertex_degree_counter;
      };

      // Cache the stats pointers for the decreasing slices of properties
      // that make up the prefixes.
      auto const uncomputed_stats_by_prefix = std::invoke([&] {
        auto prefix_key = key;
        auto stats_by_prefix = std::vector<StatsByPrefix>();
        stats_by_prefix.reserve(key.second.size());
        while (!prefix_key.second.empty()) {
          if (label_property_counter.contains(prefix_key)) {
            // If we've computed the stats for a given prefix, we also know
            // that we've computed stats for every prefix of the prefix and
            // so can stop.
            break;
          }

          stats_by_prefix.emplace_back(&label_property_counter[prefix_key], &vertex_degree_counter[prefix_key]);
          prefix_key.second.pop_back();
        }

        return stats_by_prefix;
      });

      if (uncomputed_stats_by_prefix.empty()) {
        return;
      }

      auto const &[label, properties] = key;

      auto prop_ranges = std::vector<storage::PropertyValueRange>();
      prop_ranges.reserve(properties.size());
      ranges::fill(prop_ranges, storage::PropertyValueRange::IsNotNull());

      for (auto const &vertex : execution_db_accessor->Vertices(view, label, properties, prop_ranges)) {
        // TODO(colinbarry) currently using a linear pass to get the property
        // values. Instead, gather all in one pass and permute as we usually do.
        // Additional, we should only extract the leaf property rather than
        // the entire one, which would save a call to `ReadNestedPropertyValue`.

        std::vector<storage::PropertyValue> property_values;
        property_values.reserve(properties.size());
        for (auto property_path : properties) {
          auto property_value = *vertex.GetProperty(view, property_path[0]);
          auto *nested_property_value = ReadNestedPropertyValue(property_value, property_path | rv::drop(1));
          if (nested_property_value) {
            property_values.push_back(*nested_property_value);
          } else {
            property_values.push_back(storage::PropertyValue{});
          }
        }

        for (auto &stats : uncomputed_stats_by_prefix) {
          // Once we've hit a prefix where all the properties in the prefixes
          // are null, we can stop checking as we know for sure that each
          // smaller slice of prop values will also all be null.
          if (std::ranges::all_of(property_values, [](auto const &prop) { return prop.IsNull(); })) {
            break;
          }

          (*stats.properties_value_counter)[property_values]++;
          (*stats.vertex_degree_counter) += *vertex.OutDegree(view) + *vertex.InDegree(view);

          property_values.pop_back();
        }
      }
    };

    // Compute the stat info in order based on the length of the composite key
    // properties: this ensures we never have to recompute prefixes which we
    // could have computed as part of a longer composite key. For example,
    // in computing the stats for :L1(a, b, c), we can quickly compute them for
    // :L1(a, b) and :L1(a).
    std::ranges::sort(index_info, std::greater{},
                      [](auto const &label_and_properties) { return label_and_properties.second.size(); });

    for (auto const &index : index_info) {
      count_vertex_prop_info(index);
    }

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

  std::vector<LPIndex> label_property_indices_info = index_info.label_properties;
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

  auto const prop_path_to_name = [execution_db_accessor](auto const &property_path) {
    return PropertyPathToName(execution_db_accessor, property_path);
  };

  std::for_each(label_property_stats.begin(), label_property_stats.end(), [&](const auto &stat_entry) {
    std::vector<TypedValue> result;
    result.reserve(kComputeStatisticsNumResults);
    result.emplace_back(execution_db_accessor->LabelToName(stat_entry.first.first));
    result.emplace_back(stat_entry.first.second | rv::transform(prop_path_to_name) | ranges::to_vector);
    result.emplace_back(static_cast<int64_t>(stat_entry.second.count));
    result.emplace_back(static_cast<int64_t>(stat_entry.second.distinct_values_count));
    result.emplace_back(stat_entry.second.avg_group_size);
    result.emplace_back(stat_entry.second.statistic);
    result.emplace_back(stat_entry.second.avg_degree);
    results.push_back(std::move(result));
  });

  return results;
}

// TODO: these TypedValue should be using the query allocator
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

  auto populate_label_results = [execution_db_accessor](auto const &index_info) {
    std::vector<storage::LabelId> label_results;
    label_results.reserve(index_info.size());
    std::for_each(index_info.begin(), index_info.end(),
                  [execution_db_accessor, &label_results](const storage::LabelId &label_id) {
                    const auto res = execution_db_accessor->DeleteLabelIndexStats(label_id);
                    if (res) label_results.emplace_back(label_id);
                  });

    return label_results;
  };

  auto populate_label_property_results = [execution_db_accessor](auto const &index_info) {
    std::vector<std::pair<storage::LabelId, std::vector<storage::PropertyPath>>> label_property_results;
    label_property_results.reserve(index_info.size());

    std::for_each(index_info.begin(), index_info.end(),
                  [execution_db_accessor, &label_property_results](
                      const std::pair<storage::LabelId, std::vector<storage::PropertyPath>> &label_property) {
                    auto res = execution_db_accessor->DeleteLabelPropertyIndexStats(label_property.first);
                    label_property_results.insert(label_property_results.end(), std::move_iterator{res.begin()},
                                                  std::move_iterator{res.end()});
                  });

    return label_property_results;
  };

  auto index_info = execution_db_accessor->ListAllIndices();

  auto label_indices_info = index_info.label;
  erase_not_specified_label_indices(label_indices_info);
  auto label_results = populate_label_results(label_indices_info);

  auto label_property_indices_info = index_info.label_properties;
  erase_not_specified_label_property_indices(label_property_indices_info);
  auto label_prop_results = populate_label_property_results(label_property_indices_info);

  std::vector<std::vector<TypedValue>> results;
  results.reserve(label_results.size() + label_prop_results.size());

  std::transform(
      label_results.begin(), label_results.end(), std::back_inserter(results),
      [execution_db_accessor](const auto &label_index) {
        return std::vector<TypedValue>{TypedValue(execution_db_accessor->LabelToName(label_index)), TypedValue("")};
      });

  auto const prop_path_to_name = [&](storage::PropertyPath const &property_path) {
    return TypedValue{PropertyPathToName(execution_db_accessor, property_path)};
  };

  std::transform(label_prop_results.begin(), label_prop_results.end(), std::back_inserter(results),
                 [&](const auto &label_property_index) {
                   return std::vector<TypedValue>{
                       TypedValue(execution_db_accessor->LabelToName(label_property_index.first)),
                       TypedValue(label_property_index.second | rv::transform(prop_path_to_name) | ranges::to_vector),
                   };
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

constexpr auto kCancelPeriodCreateIndex = 10'000;  // TODO: control via flag?
auto make_create_index_cancel_callback(StoppingContext stopping_context) {
  return
      [stopping_context = std::move(stopping_context), counter = utils::ResettableCounter{kCancelPeriodCreateIndex}]() {
        if (!counter()) {
          return false;
        }
        // For now only handle TERMINATED + SHUTDOWN
        // No timeout becasue we expect it would confuse users
        // ie. we don't want to run for 20min and then discard because of timeout
        auto reason = stopping_context.MustAbort();
        return reason == AbortReason::TERMINATED || reason == AbortReason::SHUTDOWN;
      };
}

PreparedQuery PrepareIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                std::vector<Notification> *notifications, CurrentDB &current_db,
                                StoppingContext stopping_context) {
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

  auto *storage = db_acc->storage();
  auto label = storage->NameToLabel(index_query->label_.name);

  std::vector<storage::PropertyPath> properties;
  std::vector<std::string> properties_string;
  properties.reserve(index_query->properties_.size());
  properties_string.reserve(index_query->properties_.size());

  for (const auto &property_path : index_query->properties_) {
    auto path = property_path.path |
                rv::transform([&](auto &&property) { return storage->NameToProperty(property.name); }) |
                ranges::to_vector;
    properties.push_back(std::move(path));
    properties_string.push_back(property_path.AsPathString());
  }

  auto properties_stringified = utils::Join(properties_string, ", ");

  Notification index_notification(SeverityLevel::INFO);
  switch (index_query->action_) {
    case IndexQuery::Action::CREATE: {
      // Creating an index influences computed plan costs.
      index_notification.code = NotificationCode::CREATE_INDEX;
      index_notification.title =
          fmt::format("Created index on label {} on properties {}.", index_query->label_.name, properties_stringified);

      // TODO: not just storage + invalidate_plan_cache. Need a DB transaction (for replication)
      handler = [dba, label, properties_stringified = std::move(properties_stringified),
                 label_name = index_query->label_.name, properties = std::move(properties),
                 stopping_context = std::move(stopping_context)](Notification &index_notification) mutable {
        auto cancel_callback = make_create_index_cancel_callback(stopping_context);
        auto maybe_index_error = properties.empty()
                                     ? dba->CreateIndex(label, std::move(cancel_callback))
                                     : dba->CreateIndex(label, std::move(properties), std::move(cancel_callback));
        if (maybe_index_error.HasError()) {
          auto const error_visitor = [&]<typename T>(T const &) {
            if constexpr (std::is_same_v<T, storage::IndexDefinitionError> ||
                          std::is_same_v<T, storage::IndexDefinitionConfigError> ||
                          std::is_same_v<T, storage::IndexDefinitionAlreadyExistsError>) {
              index_notification.code = NotificationCode::EXISTENT_INDEX;
              index_notification.title =
                  fmt::format("Index on label {} on properties {} already exists.", label_name, properties_stringified);
            } else if constexpr (std::is_same_v<T, storage::IndexDefinitionCancelationError>) {
              // TODO: could also be SHUTDOWN...but this is good enough for now
              throw HintedAbortError(AbortReason::TERMINATED);
            } else {
              static_assert(utils::always_false<T>, "Unhandled error type in error_visitor");
            }
          };

          std::visit(error_visitor, maybe_index_error.GetError());
        }
      };
      break;
    }
    case IndexQuery::Action::DROP: {
      // Creating an index influences computed plan costs.
      index_notification.code = NotificationCode::DROP_INDEX;
      index_notification.title = fmt::format("Dropped index on label {} on properties {}.", index_query->label_.name,
                                             utils::Join(properties_string, ", "));
      // TODO: not just storage + invalidate_plan_cache. Need a DB transaction (for replication)
      handler = [dba, label, properties_stringified = std::move(properties_stringified),
                 label_name = index_query->label_.name,
                 properties = std::move(properties)](Notification &index_notification) mutable {
        auto maybe_index_error =
            properties.empty() ? dba->DropIndex(label) : dba->DropIndex(label, std::move(properties));
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
                                    std::vector<Notification> *notifications, CurrentDB &current_db,
                                    StoppingContext stopping_context) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  if (!current_db.db_acc_->get()->config().salient.items.properties_on_edges) {
    throw EdgeIndexDisabledPropertiesOnEdgesException();
  }

  auto *index_query = utils::Downcast<EdgeIndexQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  MG_ASSERT(current_db.db_acc_, "Index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

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
    // TODO(composite_index): extend to also apply for edge type indices
    throw utils::NotYetImplemented("composite indices");
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

      handler = [dba, edge_type, edge_type_name = index_query->edge_type_.name, global_index = index_query->global_,
                 properties_stringified = std::move(properties_stringified), properties = std::move(properties),
                 stopping_context = std::move(stopping_context)](Notification &index_notification) {
        MG_ASSERT(properties.size() <= 1U);

        const utils::BasicResult<storage::StorageIndexDefinitionError, void> maybe_index_error = std::invoke([&] {
          auto cancel_check = make_create_index_cancel_callback(stopping_context);
          if (global_index) {
            if (properties.empty()) throw utils::BasicException("Missing property for global edge index.");
            return dba->CreateGlobalEdgeIndex(properties[0], std::move(cancel_check));
          } else if (properties.empty()) {
            return dba->CreateIndex(edge_type, std::move(cancel_check));
          }
          return dba->CreateIndex(edge_type, properties[0], std::move(cancel_check));
        });

        if (maybe_index_error.HasError()) {
          auto const error_visitor = [&]<typename T>(T const &) {
            if constexpr (std::is_same_v<T, storage::IndexDefinitionError> ||
                          std::is_same_v<T, storage::IndexDefinitionConfigError> ||
                          std::is_same_v<T, storage::IndexDefinitionAlreadyExistsError>) {
              index_notification.code = NotificationCode::EXISTENT_INDEX;
              index_notification.title = fmt::format("Index on edge-type {} on properties {} already exists.",
                                                     edge_type_name, properties_stringified);
            } else if constexpr (std::is_same_v<T, storage::IndexDefinitionCancelationError>) {
              // TODO: could also be SHUTDOWN...but this is good enough for now
              throw HintedAbortError(AbortReason::TERMINATED);
            } else {
              static_assert(utils::always_false<T>, "Unhandled error type in error_visitor");
            }
          };

          std::visit(error_visitor, maybe_index_error.GetError());
        }
      };
      break;
    }
    case EdgeIndexQuery::Action::DROP: {
      index_notification.code = NotificationCode::DROP_INDEX;
      index_notification.title = fmt::format("Dropped index on edge-type {}.", index_query->edge_type_.name);
      handler = [dba, edge_type, label_name = index_query->edge_type_.name, global_index = index_query->global_,
                 properties_stringified = std::move(properties_stringified),
                 properties = std::move(properties)](Notification &index_notification) {
        MG_ASSERT(properties.size() <= 1U);

        const utils::BasicResult<storage::StorageIndexDefinitionError, void> maybe_index_error = std::invoke([&] {
          if (global_index) {
            if (properties.size() != 1) throw utils::BasicException("Missing property for global edge index.");
            return dba->DropGlobalEdgeIndex(properties[0]);
          }
          return properties.empty() ? dba->DropIndex(edge_type) : dba->DropIndex(edge_type, properties[0]);
        });

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

PreparedQuery PrepareVectorIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                      std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  auto *vector_index_query = utils::Downcast<VectorIndexQuery>(parsed_query.query);
  std::function<Notification(void)> handler;

  MG_ASSERT(current_db.db_acc_, "Index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto const invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  auto index_name = vector_index_query->index_name_;
  auto label_name = vector_index_query->label_.name;
  auto prop_name = vector_index_query->property_.name;
  auto config = vector_index_query->configs_;
  auto *storage = db_acc->storage();
  switch (vector_index_query->action_) {
    case VectorIndexQuery::Action::CREATE: {
      const EvaluationContext evaluation_context{.timestamp = QueryTimestamp(), .parameters = parsed_query.parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
      auto vector_index_config = ParseVectorIndexConfigMap(config, evaluator);
      handler = [dba, storage, vector_index_config, invalidate_plan_cache = std::move(invalidate_plan_cache),
                 query_parameters = std::move(parsed_query.parameters), index_name = std::move(index_name),
                 label_name = std::move(label_name), prop_name = std::move(prop_name)]() {
        Notification index_notification(SeverityLevel::INFO);
        index_notification.code = NotificationCode::CREATE_INDEX;
        index_notification.title = fmt::format("Created vector index on label {}, property {}.", label_name, prop_name);
        auto label_id = storage->NameToLabel(label_name);
        auto prop_id = storage->NameToProperty(prop_name);
        auto maybe_error = dba->CreateVectorIndex(storage::VectorIndexSpec{
            .index_name = index_name,
            .label_id = label_id,
            .property = prop_id,
            .metric_kind = vector_index_config.metric,
            .dimension = vector_index_config.dimension,
            .resize_coefficient = vector_index_config.resize_coefficient,
            .capacity = vector_index_config.capacity,
            .scalar_kind = vector_index_config.scalar_kind,
        });
        utils::OnScopeExit const invalidator(invalidate_plan_cache);
        if (maybe_error.HasError()) {
          index_notification.title = fmt::format(
              "Error while creating vector index on label {}, property {}, for more information check the logs.",
              label_name, prop_name);
        }
        return index_notification;
      };
      break;
    }
    case VectorIndexQuery::Action::DROP: {
      handler = [dba, invalidate_plan_cache = std::move(invalidate_plan_cache), index_name = std::move(index_name)]() {
        Notification index_notification(SeverityLevel::INFO);
        index_notification.code = NotificationCode::DROP_INDEX;
        index_notification.title = fmt::format("Dropped vector index {}.", index_name);

        auto maybe_index_error = dba->DropVectorIndex(index_name);
        utils::OnScopeExit const invalidator(invalidate_plan_cache);
        if (maybe_index_error.HasError()) {
          index_notification.code = NotificationCode::NONEXISTENT_INDEX;
          index_notification.title = fmt::format("Vector index {} doesn't exist.", index_name);
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

PreparedQuery PrepareCreateVectorEdgeIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                                std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  if (!current_db.db_acc_->get()->config().salient.items.properties_on_edges) {
    throw EdgeIndexDisabledPropertiesOnEdgesException();
  }

  auto *vector_index_query = utils::Downcast<CreateVectorEdgeIndexQuery>(parsed_query.query);
  std::function<Notification(void)> handler;

  MG_ASSERT(current_db.db_acc_, "Index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto const invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  auto index_name = vector_index_query->index_name_;
  auto edge_type = vector_index_query->edge_type_.name;
  auto prop_name = vector_index_query->property_.name;
  auto config = vector_index_query->configs_;
  auto *storage = db_acc->storage();

  const EvaluationContext evaluation_context{.timestamp = QueryTimestamp(), .parameters = parsed_query.parameters};
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
  auto vector_index_config = ParseVectorIndexConfigMap(config, evaluator);
  handler = [dba, storage, vector_index_config, invalidate_plan_cache = std::move(invalidate_plan_cache),
             query_parameters = std::move(parsed_query.parameters), index_name = std::move(index_name),
             edge_type = std::move(edge_type), prop_name = std::move(prop_name)]() {
    Notification index_notification(SeverityLevel::INFO);
    index_notification.code = NotificationCode::CREATE_INDEX;
    index_notification.title = fmt::format("Created vector index on edge type {}, property {}.", edge_type, prop_name);
    auto edge_type_id = storage->NameToEdgeType(edge_type);
    auto prop_id = storage->NameToProperty(prop_name);
    auto maybe_error = dba->CreateVectorEdgeIndex(storage::VectorEdgeIndexSpec{
        .index_name = index_name,
        .edge_type_id = edge_type_id,
        .property = prop_id,
        .metric_kind = vector_index_config.metric,
        .dimension = vector_index_config.dimension,
        .resize_coefficient = vector_index_config.resize_coefficient,
        .capacity = vector_index_config.capacity,
        .scalar_kind = vector_index_config.scalar_kind,
    });
    utils::OnScopeExit const invalidator(invalidate_plan_cache);
    if (maybe_error.HasError()) {
      index_notification.title = fmt::format(
          "Error while creating vector index on edge type {}, property {}, for more information check the logs.",
          edge_type, prop_name);
    }
    return index_notification;
  };

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

  MG_ASSERT(current_db.db_acc_, "Text index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Text index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *storage = db_acc->storage();
  auto label_name = text_index_query->label_.name;
  auto label_id = storage->NameToLabel(label_name);
  auto &index_name = text_index_query->index_name_;
  auto property_ids = text_index_query->properties_ |
                      rv::transform([&](const auto &property) { return storage->NameToProperty(property.name); }) |
                      r::to_vector;

  Notification index_notification(SeverityLevel::INFO);
  switch (text_index_query->action_) {
    case TextIndexQuery::Action::CREATE: {
      index_notification.code = NotificationCode::CREATE_INDEX;
      index_notification.title = fmt::format("Created text index on label {}.", label_name);
      handler = [dba, label_id, index_name, label_name = std::move(label_name),
                 property_ids = std::move(property_ids)](Notification &index_notification) {
        auto maybe_error = dba->CreateTextIndex(storage::TextIndexSpec{index_name, label_id, property_ids});
        if (maybe_error.HasError()) {
          index_notification.code = NotificationCode::EXISTENT_INDEX;
          index_notification.title =
              fmt::format("Text index on label {} with name {} already exists.", label_name, index_name);
        }
      };
      break;
    }
    case TextIndexQuery::Action::DROP: {
      index_notification.code = NotificationCode::DROP_INDEX;
      index_notification.title = fmt::format("Dropped text index {}.", index_name);
      handler = [dba, index_name](Notification &index_notification) {
        auto maybe_error = dba->DropTextIndex(index_name);
        if (maybe_error.HasError()) {
          index_notification.code = NotificationCode::NONEXISTENT_INDEX;
          index_notification.title = fmt::format("Text index with name {} doesn't exist.", index_name);
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

PreparedQuery PrepareCreateTextEdgeIndexQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                              std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  if (!current_db.db_acc_->get()->config().salient.items.properties_on_edges) {
    throw EdgeIndexDisabledPropertiesOnEdgesException();
  }

  auto *text_edge_index_query = utils::Downcast<CreateTextEdgeIndexQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  MG_ASSERT(current_db.db_acc_, "Text index query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Text index query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto *storage = db_acc->storage();
  const auto edge_type_id = storage->NameToEdgeType(text_edge_index_query->edge_type_.name);
  const auto &index_name = text_edge_index_query->index_name_;
  auto property_ids = text_edge_index_query->properties_ |
                      rv::transform([&](const auto &property) { return storage->NameToProperty(property.name); }) |
                      r::to_vector;
  Notification index_notification(SeverityLevel::INFO);
  index_notification.code = NotificationCode::CREATE_INDEX;
  index_notification.title = fmt::format("Created text index on label {}.", text_edge_index_query->edge_type_.name);
  handler = [dba, edge_type_id, index_name, edge_type_name = std::move(text_edge_index_query->edge_type_.name),
             property_ids = std::move(property_ids)](Notification &index_notification) {
    auto maybe_error = dba->CreateTextEdgeIndex(storage::TextEdgeIndexSpec{index_name, edge_type_id, property_ids});
    if (maybe_error.HasError()) {
      index_notification.code = NotificationCode::EXISTENT_INDEX;
      index_notification.title =
          fmt::format("Text index on label {} with name {} already exists.", edge_type_name, index_name);
    }
  };

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

PreparedQuery PrepareDropAllIndexesQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                         std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw IndexInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Drop all indexes query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  if (db_acc->storage()->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw DropAllIndexesDisabledOnDiskStorage();
  }

  std::function<void(Notification &)> handler;

  MG_ASSERT(current_db.db_transactional_accessor_, "Drop all indexes query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto const invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  Notification index_notification(SeverityLevel::INFO);
  index_notification.code = NotificationCode::DROP_INDEX;
  index_notification.title = "Dropped all indexes.";

  handler = [dba, invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification & /**/) {
    utils::OnScopeExit const invalidator(invalidate_plan_cache);
    dba->DropAllIndexes();
  };

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

PreparedQuery PrepareDropAllConstraintsQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                             std::vector<Notification> *notifications, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw ConstraintInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Drop all constraints query expects a current DB");
  auto &db_acc = *current_db.db_acc_;

  if (db_acc->storage()->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw DropAllConstraintsDisabledOnDiskStorage();
  }

  std::function<void(Notification &)> handler;

  MG_ASSERT(current_db.db_transactional_accessor_, "Drop all constraints query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  auto const invalidate_plan_cache = [plan_cache = db_acc->plan_cache()] {
    plan_cache->WithLock([&](auto &cache) { cache.reset(); });
  };

  Notification constraint_notification(SeverityLevel::INFO);
  constraint_notification.code = NotificationCode::DROP_CONSTRAINT;
  constraint_notification.title = "Dropped all constraints.";

  handler = [dba, invalidate_plan_cache = std::move(invalidate_plan_cache)](Notification & /**/) {
    utils::OnScopeExit const invalidator(invalidate_plan_cache);
    dba->DropAllConstraints();
  };

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [handler = std::move(handler), notifications, constraint_notification = std::move(constraint_notification)](
          AnyStream * /*stream*/, std::optional<int> /*unused*/) mutable {
        handler(constraint_notification);
        notifications->push_back(constraint_notification);
        return QueryHandlerResult::COMMIT;
      },
      RWType::NONE};
}

#ifdef MG_ENTERPRISE
PreparedQuery PrepareTtlQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                              std::vector<Notification> *notifications, CurrentDB &current_db,
                              InterpreterContext *interpreter_context) {
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryRuntimeException(
        license::LicenseCheckErrorToString(license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "TTL"));
  }

  // Ensure TTL operations are only performed on MAIN instance
  EnsureMainInstance(interpreter_context, "TTL operations");

  if (in_explicit_transaction) {
    throw TtlInMulticommandTxException();
  }

  auto *ttl_query = utils::Downcast<TtlQuery>(parsed_query.query);
  std::function<void(Notification &)> handler;

  MG_ASSERT(current_db.db_acc_, "Time to live query expects a current DB");
  auto db_acc = *current_db.db_acc_;

  MG_ASSERT(current_db.db_transactional_accessor_, "Time to live query expects a current DB transaction");
  auto *dba = &*current_db.execution_db_accessor_;

  Notification notification(SeverityLevel::INFO);
  switch (ttl_query->type_) {
    case TtlQuery::Type::START: {
      handler = [db_acc = std::move(db_acc), dba](Notification &notification) mutable {
        dba->StartTtl();
        notification.code = NotificationCode::ENABLE_TTL;
        notification.title = fmt::format("Starting time-to-live worker. Will be executed");
      };
      break;
    }
    case TtlQuery::Type::CONFIGURE: {
      auto evaluation_context = EvaluationContext{.timestamp = QueryTimestamp(), .parameters = parsed_query.parameters};
      auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
      try {
        std::string info;
        std::string period;
        if (ttl_query->period_) {
          auto ttl_period = ttl_query->period_->Accept(evaluator);
          period = ttl_period.ValueString();
        }
        std::string start_time;
        if (ttl_query->specific_time_) {
          auto ttl_start_time = ttl_query->specific_time_->Accept(evaluator);
          start_time = ttl_start_time.ValueString();
        }
        bool run_edge_ttl = db_acc->config().salient.items.properties_on_edges &&
                            db_acc->GetStorageMode() != storage::StorageMode::ON_DISK_TRANSACTIONAL;
        auto ttl_info = storage::ttl::TtlInfo{period, start_time, run_edge_ttl};
        // TTL could already be configured; use the present config if no user-defined config
        info = "Starting time-to-live worker. Will be executed";
        if (ttl_info)
          info += ttl_info.ToString();
        else {
          // Get current TTL config through DbAccessor
          auto current_ttl_config = dba->GetTtlConfig();
          if (current_ttl_config) info += current_ttl_config.ToString();
        }

        handler = [db_acc = std::move(db_acc), dba, ttl_info,
                   info = std::move(info)](Notification &notification) mutable {
          dba->ConfigureTtl(ttl_info);
          dba->StartTtl();
          notification.code = NotificationCode::ENABLE_TTL;
          notification.title = info;
        };
      } catch (const storage::ttl::TtlException &e) {
        throw utils::BasicException(e.what());
      }
      break;
    }
    case TtlQuery::Type::DISABLE: {
      handler = [db_acc = std::move(db_acc), dba](Notification &notification) mutable {
        dba->DisableTtl();
        notification.code = NotificationCode::DISABLE_TTL;
        notification.title = fmt::format("Disabled time-to-live feature.");
      };
      break;
    }
    case TtlQuery::Type::STOP: {
      handler = [db_acc = std::move(db_acc), dba](Notification &notification) mutable {
        dba->StopTtl();
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
                               InterpreterContext *interpreter_context, Interpreter &interpreter,
                               std::optional<memgraph::dbms::DatabaseAccess> db_acc) {
  if (in_explicit_transaction) {
    throw UserModificationInMulticommandTxException();
  }

  auto *auth_query = utils::Downcast<AuthQuery>(parsed_query.query);

  // Special case for auth queries that don't require any privileges (those that work on the current user only)
  auto target_db = std::string{dbms::kSystemDB};
  if (auth_query->action_ == AuthQuery::Action::SHOW_CURRENT_USER ||
      auth_query->action_ == AuthQuery::Action::SHOW_CURRENT_ROLE ||
      auth_query->action_ == AuthQuery::Action::CHANGE_PASSWORD) {
    target_db = db_acc ? db_acc->get()->name() : "";
  }

  auto callback =
      HandleAuthQuery(auth_query, interpreter_context, parsed_query.parameters, interpreter, std::move(db_acc));

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
      RWType::NONE, target_db};
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
                       RWType::NONE, std::string(dbms::kSystemDB)};
  // False positive report for the std::make_shared above
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
}
PreparedQuery PrepareReplicationInfoQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                          ReplicationQueryHandler &replication_query_handler) {
  if (in_explicit_transaction) {
    throw ReplicationModificationInMulticommandTxException();
  }

  auto *replication_query = utils::Downcast<ReplicationInfoQuery>(parsed_query.query);
  auto callback = HandleReplicationInfoQuery(replication_query, replication_query_handler);

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

Callback CreateTrigger(TriggerQuery *trigger_query, const storage::ExternalPropertyValue::map_t &user_parameters,
                       TriggerStore *trigger_store, InterpreterContext *interpreter_context, DbAccessor *dba,
                       std::shared_ptr<QueryUserOrRole> user_or_role, const std::string &db_name) {
  // Make a copy of the user and pass it to the subsystem
  auto owner = interpreter_context->auth_checker->GenQueryUser(user_or_role->username(), user_or_role->rolenames());
  return {{},
          [trigger_name = std::move(trigger_query->trigger_name_),
           trigger_statement = std::move(trigger_query->statement_), event_type = trigger_query->event_type_,
           before_commit = trigger_query->before_commit_, trigger_store, interpreter_context, dba, user_parameters,
           owner = std::move(owner), db_name]() mutable -> std::vector<std::vector<TypedValue>> {
            trigger_store->AddTrigger(
                std::move(trigger_name), trigger_statement, user_parameters, ToTriggerEventType(event_type),
                before_commit ? TriggerPhase::BEFORE_COMMIT : TriggerPhase::AFTER_COMMIT,
                &interpreter_context->ast_cache, dba, interpreter_context->config.query, std::move(owner), db_name);
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
                               &trigger_notification, db_name = current_db.db_acc_->get()->name()]() mutable {
    switch (trigger_query->action_) {
      case TriggerQuery::Action::CREATE_TRIGGER:
        trigger_notification.emplace(SeverityLevel::INFO, NotificationCode::CREATE_TRIGGER,
                                     fmt::format("Created trigger {}.", trigger_query->trigger_name_));
        return CreateTrigger(trigger_query, user_parameters, trigger_store, interpreter_context, dba, std::move(owner),
                             db_name);
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
    ttl.Disable();

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

// NOLINTNEXTLINE
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

  Callback callback;
  callback.header = {"path"};
  callback.fn = [storage]() mutable -> std::vector<std::vector<TypedValue>> {
    auto *mem_storage = static_cast<storage::InMemoryStorage *>(storage);
    constexpr bool kForce = true;
    const auto maybe_path = mem_storage->CreateSnapshot(kForce);
    if (maybe_path.HasError()) {
      switch (maybe_path.GetError()) {
        case storage::InMemoryStorage::CreateSnapshotError::ReachedMaxNumTries:
          spdlog::warn("Failed to create snapshot. Reached max number of tries. Please contact support");
          break;
        case storage::InMemoryStorage::CreateSnapshotError::AbortSnapshot:
          throw utils::BasicException("Failed to create snapshot. The current snapshot needs to be aborted.");
        case storage::InMemoryStorage::CreateSnapshotError::AlreadyRunning:
          throw utils::BasicException("Another snapshot creation is already in progress.");
        case storage::InMemoryStorage::CreateSnapshotError::NothingNewToWrite:
          throw utils::BasicException("Nothing has been written since the last snapshot.");
      }
    }
    return std::vector<std::vector<TypedValue>>{{TypedValue{maybe_path.GetValue()}}};
  };

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
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
                       RWType::NONE};
}

PreparedQuery PrepareRecoverSnapshotQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db,
                                          replication_coordination_glue::ReplicationRole replication_role) {
  if (in_explicit_transaction) {
    throw RecoverSnapshotInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Recover Snapshot query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw RecoverSnapshotDisabledOnDiskStorage();
  }

  auto *recover_query = utils::Downcast<RecoverSnapshotQuery>(parsed_query.query);
  auto evaluation_context = EvaluationContext{.timestamp = QueryTimestamp(), .parameters = parsed_query.parameters};
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};
  auto path_value = recover_query->snapshot_->Accept(evaluator);

  return PreparedQuery{
      {},
      std::move(parsed_query.required_privileges),
      [db_acc = *current_db.db_acc_, replication_role, path = std::move(path_value.ValueString()),
       force = recover_query->force_](AnyStream * /*stream*/,
                                      std::optional<int> /*n*/) mutable -> std::optional<QueryHandlerResult> {
        auto *mem_storage = static_cast<storage::InMemoryStorage *>(db_acc->storage());
        if (auto maybe_error = mem_storage->RecoverSnapshot(path, force, replication_role); maybe_error.HasError()) {
          switch (maybe_error.GetError()) {
            case storage::InMemoryStorage::RecoverSnapshotError::DisabledForReplica:
              throw utils::BasicException(
                  "Failed to recover a snapshot. Replica instances are not allowed to create them.");
            case storage::InMemoryStorage::RecoverSnapshotError::NonEmptyStorage:
              throw utils::BasicException("Failed to recover a snapshot. Storage is not clean. Try using FORCE.");
            case storage::InMemoryStorage::RecoverSnapshotError::MissingFile:
              throw utils::BasicException("Failed to find the defined snapshot file.");
            case storage::InMemoryStorage::RecoverSnapshotError::CopyFailure:
              throw utils::BasicException("Failed to copy snapshot over to local snapshots directory.");
            case storage::InMemoryStorage::RecoverSnapshotError::BackupFailure:
              throw utils::BasicException(
                  "Failed to clear local wal and snapshots directories. Please clean them manually.");
          }
        }
        // REPLICATION
        // Note: This can only be called on MAIN.
        const auto locked_clients = mem_storage->repl_storage_state_.replication_storage_clients_.ReadLock();
        auto protector = dbms::DatabaseProtector{db_acc};
        for (const auto &client : *locked_clients) {
          client->ForceRecoverReplica(mem_storage, protector);
        }
        return QueryHandlerResult::COMMIT;
      },
      RWType::NONE};
}

PreparedQuery PrepareShowSnapshotsQuery(ParsedQuery parsed_query, bool in_explicit_transaction, CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw ShowSchemaInfoInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Show Snapshots query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw ShowSnapshotsDisabledOnDiskStorage();
  }

  Callback callback;
  callback.header = {"path", "timestamp", "creation_time", "size"};
  callback.fn = [storage]() mutable -> std::vector<std::vector<TypedValue>> {
    std::vector<std::vector<TypedValue>> infos;
    const auto res = static_cast<storage::InMemoryStorage *>(storage)->ShowSnapshots();
    infos.reserve(res.size());
    for (const auto &info : res) {
      infos.push_back({TypedValue{info.path.string()}, TypedValue{static_cast<int64_t>(info.durable_timestamp)},
                       TypedValue{info.creation_time.ToStringWTZ()},
                       TypedValue{utils::GetReadableSize(static_cast<double>(info.size))}});
    }
    return infos;
  };

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
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
                       RWType::NONE};
}

PreparedQuery PrepareShowNextSnapshotQuery(ParsedQuery parsed_query, bool in_explicit_transaction,
                                           CurrentDB &current_db) {
  if (in_explicit_transaction) {
    throw ShowSchemaInfoInMulticommandTxException();
  }

  MG_ASSERT(current_db.db_acc_, "Show Next Snapshot query expects a current DB");
  storage::Storage *storage = current_db.db_acc_->get()->storage();

  if (storage->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw ShowSnapshotsDisabledOnDiskStorage();
  }

  Callback callback;
  callback.header = {"path", "creation_time"};
  callback.fn = [storage]() mutable -> std::vector<std::vector<TypedValue>> {
    std::vector<std::vector<TypedValue>> infos;
    const auto res = static_cast<storage::InMemoryStorage *>(storage)->ShowNextSnapshot();

    if (res) {
      infos.push_back({TypedValue{res->path.string()}, TypedValue{res->creation_time.ToStringWTZ()}});
    }
    return infos;
  };

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
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
                       RWType::NONE};
}

PreparedQuery PrepareSettingQuery(ParsedQuery parsed_query, const bool in_explicit_transaction) {
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

    auto get_interpreter_db_name = [&]() -> std::string {
      return interpreter->current_db_.db_acc_ ? interpreter->current_db_.db_acc_->get()->name() : "";
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
      std::vector<uint64_t> maybe_kill_transaction_ids;
      std::transform(transaction_query->transaction_id_list_.begin(), transaction_query->transaction_id_list_.end(),
                     std::back_inserter(maybe_kill_transaction_ids), [&evaluator](Expression *expression) {
                       try {
                         auto value = expression->Accept(evaluator);
                         return std::stoul(value.ValueString().c_str());  // NOLINT
                       } catch (std::exception & /* unused */) {
                         return std::numeric_limits<uint64_t>::max();
                       }
                     });
      callback.header = {"transaction_id", "killed"};
      callback.fn = [interpreter_context, maybe_kill_transaction_ids = std::move(maybe_kill_transaction_ids),
                     user_or_role = std::move(user_or_role),
                     privilege_checker = std::move(privilege_checker)]() mutable {
        return interpreter_context->interpreters.WithLock([&](auto &interpreters) mutable {
          return interpreter_context->TerminateTransactions(interpreters, std::move(maybe_kill_transaction_ids),
                                                            user_or_role.get(), std::move(privilege_checker));
        });
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
        const std::string_view edge_property_index_mark{"edge-property"};
        const std::string_view text_label_index_mark{"label_text"};
        const std::string_view text_edge_type_index_mark{"edge-type_text"};
        const std::string_view point_label_property_index_mark{"point"};
        const std::string_view vector_label_property_index_mark{"label+property_vector"};
        const std::string_view vector_edge_property_index_mark{"edge-type+property_vector"};
        auto info = dba->ListAllIndices();
        auto storage_acc = database->Access();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(info.label.size() + info.label_properties.size() + info.text_indices.size());
        for (const auto &item : info.label) {
          results.push_back({TypedValue(label_index_mark), TypedValue(storage->LabelToName(item)), TypedValue(),
                             TypedValue(static_cast<int>(storage_acc->ApproximateVertexCount(item)))});
        }
        for (const auto &[label, properties] : info.label_properties) {
          auto const prop_path_to_name = [&](storage::PropertyPath const &property_path) {
            return TypedValue{PropertyPathToName(storage, property_path)};
          };
          auto props = properties | rv::transform(prop_path_to_name) | ranges::to_vector;
          results.push_back({TypedValue(label_property_index_mark), TypedValue(storage->LabelToName(label)),
                             TypedValue(std::move(props)),
                             TypedValue(static_cast<int>(storage_acc->ApproximateVertexCount(label, properties)))});
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
        for (const auto &item : info.edge_property) {
          results.push_back({TypedValue(edge_property_index_mark), TypedValue(),
                             TypedValue(storage->PropertyToName(item)),
                             TypedValue(static_cast<int>(storage_acc->ApproximateEdgeCount(item)))});
        }
        for (const auto &[index_name, label, properties] : info.text_indices) {
          auto prop_names =
              properties |
              rv::transform([storage](auto prop_id) { return TypedValue(storage->PropertyToName(prop_id)); }) |
              r::to_vector;
          results.push_back(
              {TypedValue(fmt::format("{} (name: {})", text_label_index_mark, index_name)),
               TypedValue(storage->LabelToName(label)), TypedValue(std::move(prop_names)),
               TypedValue(static_cast<int>(storage_acc->ApproximateVerticesTextCount(index_name).value_or(0)))});
        }
        for (const auto &[index_name, label, properties] : info.text_edge_indices) {
          auto prop_names =
              properties |
              rv::transform([storage](auto prop_id) { return TypedValue(storage->PropertyToName(prop_id)); }) |
              r::to_vector;
          results.push_back(
              {TypedValue(fmt::format("{} (name: {})", text_edge_type_index_mark, index_name)),
               TypedValue(storage->EdgeTypeToName(label)), TypedValue(std::move(prop_names)),
               TypedValue(static_cast<int>(storage_acc->ApproximateEdgesTextCount(index_name).value_or(0)))});
        }
        for (const auto &[label_id, prop_id] : info.point_label_property) {
          results.push_back({TypedValue(point_label_property_index_mark), TypedValue(storage->LabelToName(label_id)),
                             TypedValue(storage->PropertyToName(prop_id)),
                             TypedValue(static_cast<int>(
                                 storage_acc->ApproximateVerticesPointCount(label_id, prop_id).value_or(0)))});
        }

        for (const auto &spec : info.vector_indices_spec) {
          results.push_back(
              {TypedValue(vector_label_property_index_mark), TypedValue(storage->LabelToName(spec.label_id)),
               TypedValue(storage->PropertyToName(spec.property)),
               TypedValue(static_cast<int>(
                   storage_acc->ApproximateVerticesVectorCount(spec.label_id, spec.property).value_or(0)))});
        }

        for (const auto &spec : info.vector_edge_indices_spec) {
          results.push_back(
              {TypedValue(vector_edge_property_index_mark), TypedValue(storage->EdgeTypeToName(spec.edge_type_id)),
               TypedValue(storage->PropertyToName(spec.property)),
               TypedValue(static_cast<int>(
                   storage_acc->ApproximateEdgesVectorCount(spec.edge_type_id, spec.property).value_or(0)))});
        }

        std::ranges::sort(results, [&label_index_mark](const auto &record_1, const auto &record_2) {
          const auto type_1 = record_1[0].ValueString();
          const auto type_2 = record_2[0].ValueString();

          if (type_1 != type_2) {
            return type_1 < type_2;
          }

          // global edge index doesn't have a label
          const auto label_1 = record_1[1].IsString() ? record_1[1].UnsafeValueString() : "";
          const auto label_2 = record_2[1].IsString() ? record_2[1].UnsafeValueString() : "";
          if (type_1 == label_index_mark || label_1 != label_2) {
            return label_1 < label_2;
          }

          // NOTE: not all "property" are strings, since composite indices it could be a list of strings
          if (record_1[2].type() == TypedValue::Type::String && record_2[2].type() == TypedValue::Type::String) {
            return record_1[2].UnsafeValueString() < record_2[2].UnsafeValueString();
          } else if (record_1[2].type() == TypedValue::Type::List && record_2[2].type() == TypedValue::Type::List) {
            auto as_string = [](TypedValue const &v) -> auto const & { return v.ValueString(); };
            return std::ranges::lexicographical_compare(record_1[2].UnsafeValueList(), record_2[2].UnsafeValueList(),
                                                        std::ranges::less{}, as_string, as_string);
          } else {
            return record_1[2].type() < record_2[2].type();
          }
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
        throw QueryRuntimeException(license::LicenseCheckErrorToString(
            license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "SHOW METRICS INFO"));
      }
#else
      throw EnterpriseOnlyException();
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
    case DatabaseInfoQuery::InfoType::VECTOR_INDEX: {
      header = {"index_name", "label", "property",    "capacity",  "dimension",
                "metric",     "size",  "scalar_kind", "index_type"};
      handler = [database, dba] {
        auto *storage = database->storage();
        auto vector_indices = dba->ListAllVectorIndices();
        auto vector_edge_indices = dba->ListAllVectorEdgeIndices();
        auto storage_acc = database->Access();
        std::vector<std::vector<TypedValue>> results;
        results.reserve(vector_indices.size());

        for (const auto &spec : vector_indices) {
          results.push_back(
              {TypedValue(spec.index_name), TypedValue(storage->LabelToName(spec.label_id)),
               TypedValue(storage->PropertyToName(spec.property)), TypedValue(static_cast<int64_t>(spec.capacity)),
               TypedValue(spec.dimension), TypedValue(spec.metric), TypedValue(static_cast<int64_t>(spec.size)),
               TypedValue(spec.scalar_kind), TypedValue(VectorIndexTypeToString(storage::VectorIndexType::ON_NODES))});
        }

        for (const auto &spec : vector_edge_indices) {
          results.push_back(
              {TypedValue(spec.index_name), TypedValue(storage->EdgeTypeToName(spec.edge_type_id)),
               TypedValue(storage->PropertyToName(spec.property)), TypedValue(static_cast<int64_t>(spec.capacity)),
               TypedValue(spec.dimension), TypedValue(spec.metric), TypedValue(static_cast<int64_t>(spec.size)),
               TypedValue(spec.scalar_kind), TypedValue(VectorIndexTypeToString(storage::VectorIndexType::ON_EDGES))});
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
            {TypedValue("database_uuid"), TypedValue(static_cast<std::string>(storage->uuid()))},
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
    case SystemInfoQuery::InfoType::LICENSE: {
      header = {"license info", "value"};
      handler = [] {
        const auto license_info = license::global_license_checker.GetDetailedLicenseInfo();
        const auto memory_limit = license_info.memory_limit != 0
                                      ? utils::GetReadableSize(static_cast<double>(license_info.memory_limit))
                                      : "UNLIMITED";

        const std::vector<std::vector<TypedValue>> results{
            {TypedValue("organization_name"), TypedValue(license_info.organization_name)},
            {TypedValue("license_key"), TypedValue(license_info.license_key)},
            {TypedValue("is_valid"), TypedValue(license_info.is_valid)},
            {TypedValue("license_type"), TypedValue(license_info.license_type)},
            {TypedValue("valid_until"), TypedValue(license_info.valid_until)},
            {TypedValue("memory_limit"), TypedValue(memory_limit)},
            {TypedValue("status"), TypedValue(license_info.status)},
        };

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

PreparedQuery PrepareMultiDatabaseQuery(ParsedQuery parsed_query, InterpreterContext *interpreter_context,
                                        Interpreter &interpreter) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryRuntimeException(
        license::LicenseCheckErrorToString(license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "multi-tenancy"));
  }

  auto *query = utils::Downcast<MultiDatabaseQuery>(parsed_query.query);
  auto *db_handler = interpreter_context->dbms_handler;

  const bool is_replica = interpreter_context->repl_state.ReadLock()->IsReplica();

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
                      "Memgraph.");
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
          std::string(dbms::kSystemDB)};
    }
    case MultiDatabaseQuery::Action::DROP: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }

      return PreparedQuery{
          {"STATUS"},
          std::move(parsed_query.required_privileges),
          [db_name = query->db_name_, force = query->force_, db_handler, interpreter_context,
           auth = interpreter_context->auth,
           interpreter = &interpreter](AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
            if (!interpreter->system_transaction_) {
              throw QueryException("Expected to be in a system transaction");
            }

            std::vector<std::vector<TypedValue>> status;

            try {
              // Remove database
              dbms::DbmsHandler::DeleteResult success;
              if (force) {
                success = db_handler->Delete(db_name, &*interpreter->system_transaction_);
                if (!success.HasError()) {
                  // Try to terminate all interpreters using the database
                  // Best effort approach, if it fails, user will continue using the db until they commit/abort
                  // Get access to the interpreter context to notify all active interpreters
                  interpreter_context->interpreters.WithLock(
                      [db_name, interpreter_context, interpreter](auto &interpreters) {
                        auto privilege_checker = [](QueryUserOrRole *user_or_role, std::string const &db_name) {
                          return user_or_role &&
                                 user_or_role->IsAuthorized({query::AuthQuery::Privilege::TRANSACTION_MANAGEMENT},
                                                            db_name, &query::up_to_date_policy);
                        };
                        interpreter_context->TerminateTransactions(
                            interpreters, InterpreterContext::ShowTransactionsUsingDBName(interpreters, db_name),
                            interpreter->user_or_role_.get(), privilege_checker);
                      });
                }
              } else {
                success = db_handler->TryDelete(db_name, &*interpreter->system_transaction_);
              }
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
    case MultiDatabaseQuery::Action::RENAME: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }

      if (!query->new_db_name_) {
        throw QueryException("New database name is required for RENAME DATABASE query.");
      }

      return PreparedQuery{
          {"STATUS"},
          std::move(parsed_query.required_privileges),
          [old_name = query->db_name_, new_name = query->new_db_name_, db_handler, interpreter = &interpreter](
              AnyStream *stream, std::optional<int> n) -> std::optional<QueryHandlerResult> {
            if (!interpreter->system_transaction_) {
              throw QueryException("Expected to be in a system transaction");
            }

            std::vector<std::vector<TypedValue>> status;
            std::string res;

            try {
              auto result = db_handler->Rename(old_name, *new_name, &*interpreter->system_transaction_);
              if (!result.HasError()) {
                res = "Successfully renamed database " + old_name + " to " + *new_name;
              } else {
                switch (result.GetError()) {
                  case dbms::RenameError::DEFAULT_DB:
                    throw QueryRuntimeException("Cannot rename the default database.");
                  case dbms::RenameError::NON_EXISTENT:
                    throw QueryRuntimeException("Database {} does not exist.", old_name);
                  case dbms::RenameError::ALREADY_EXISTS:
                    throw QueryRuntimeException("Database {} already exists.", new_name);
                  case dbms::RenameError::USING:
                    throw QueryRuntimeException("Cannot rename {}, it is currently being used.", old_name);
                  case dbms::RenameError::FAIL:
                    throw QueryRuntimeException("Failed while renaming {}", old_name);
                }
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
          RWType::W,
          query->db_name_};
    }
  }
#else
  // here to satisfy clang-tidy
  (void)parsed_query;
  (void)interpreter_context;
  (void)interpreter;
  throw EnterpriseOnlyException();
#endif
}

PreparedQuery PrepareUseDatabaseQuery(ParsedQuery parsed_query, CurrentDB &current_db,
                                      InterpreterContext *interpreter_context,
                                      std::optional<std::function<void(std::string_view)>> on_change_cb) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryException(
        "Trying to use enterprise feature without a valid license. Check your license status by running SHOW LICENSE "
        "INFO.");
  }

  auto *query = utils::Downcast<UseDatabaseQuery>(parsed_query.query);
  auto *db_handler = interpreter_context->dbms_handler;

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
#else
  // here to satisfy clang-tidy
  (void)parsed_query;
  (void)current_db;
  (void)interpreter_context;
  (void)on_change_cb;
  throw EnterpriseOnlyException();
#endif
}

PreparedQuery PrepareShowDatabaseQuery(ParsedQuery parsed_query, CurrentDB &current_db) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryException(
        "Trying to use enterprise feature without a valid license. Check your license status by running SHOW LICENSE "
        "INFO.");
  }

  return PreparedQuery{{"Current"},
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
                       RWType::NONE};
#else
  // here to satisfy clang-tidy
  (void)parsed_query;
  (void)current_db;
  throw EnterpriseOnlyException();
#endif
}

PreparedQuery PrepareShowDatabasesQuery(ParsedQuery parsed_query, InterpreterContext *interpreter_context,
                                        std::shared_ptr<QueryUserOrRole> user_or_role) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryRuntimeException(
        license::LicenseCheckErrorToString(license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "multi-tenancy"));
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
      const auto &db_priv = auth->GetDatabasePrivileges(user_or_role->username().value(), user_or_role->rolenames());
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

  return PreparedQuery{std::move(callback.header), std::move(parsed_query.required_privileges),
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
                       RWType::NONE};
#else
  throw EnterpriseOnlyException();
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
  return PreparedQuery{{"Enum Name", "Enum Values"},
                       std::move(parsed_query.required_privileges),
                       [dba = *current_db.execution_db_accessor_, pull_plan = std::shared_ptr<PullPlanVector>(nullptr)](
                           AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
                         if (!pull_plan) {
                           auto enums = dba.ShowEnums();
                           auto to_row = [](auto &&p) {
                             return std::vector{TypedValue{p.first}, TypedValue{p.second}};
                           };
                           pull_plan = std::make_shared<PullPlanVector>(enums | rv::transform(to_row) | r::to_vector);
                         }

                         if (pull_plan->Pull(stream, n)) {
                           return QueryHandlerResult::COMMIT;
                         }
                         return std::nullopt;
                       },
                       RWType::NONE};
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

PreparedQuery PrepareShowSchemaInfoQuery(const ParsedQuery &parsed_query, CurrentDB &current_db
#ifdef MG_ENTERPRISE
                                         ,
                                         InterpreterContext *interpreter_context,
                                         std::shared_ptr<QueryUserOrRole> user_or_role
#endif
) {
  if (current_db.db_acc_->get()->GetStorageMode() == storage::StorageMode::ON_DISK_TRANSACTIONAL) {
    throw ShowSchemaInfoOnDiskException();
  }

  Callback callback;
  callback.header = {"schema"};
  callback.fn = [db = *current_db.db_acc_, db_acc = current_db.execution_db_accessor_,
                 storage_acc = current_db.db_transactional_accessor_.get()
#ifdef MG_ENTERPRISE
                     ,
                 interpreter_context, user_or_role
#endif
  ]() mutable -> std::vector<std::vector<TypedValue>> {
    memgraph::metrics::IncrementCounter(memgraph::metrics::ShowSchema);

    std::vector<std::vector<TypedValue>> schema;
    auto *storage = db->storage();
    if (storage->config_.salient.items.enable_schema_info) {
#if MG_ENTERPRISE
      // Apply fine-grained access control filtering if auth_checker is available
      std::unique_ptr<FineGrainedAuthChecker> auth_checker = nullptr;
      const bool has_user_or_role = user_or_role != nullptr && *user_or_role;
      if (license::global_license_checker.IsEnterpriseValidFast() && interpreter_context &&
          interpreter_context->auth_checker && has_user_or_role && db_acc) {
        auth_checker = interpreter_context->auth_checker->GetFineGrainedAuthChecker(user_or_role, &*db_acc);
      }

      const auto node_predicate = [&auth_checker](auto label_id) {
        return auth_checker &&
               auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ);
      };
      const auto edge_predicate = [&auth_checker](auto edge_type_id) {
        return auth_checker && auth_checker->Has(edge_type_id, AuthQuery::FineGrainedPrivilege::READ);
      };

      auto json = auth_checker != nullptr
                      ? storage->schema_info_.ToJson(*storage->name_id_mapper_, storage->enum_store_, node_predicate,
                                                     edge_predicate)
                      : storage->schema_info_.ToJson(*storage->name_id_mapper_, storage->enum_store_);
#else
      auto json = storage->schema_info_.ToJson(*storage->name_id_mapper_, storage->enum_store_);
#endif

      // INDICES
      auto node_indexes = nlohmann::json::array();
      auto edge_indexes = nlohmann::json::array();
      auto index_info = db_acc->ListAllIndices();
      // Vertex label indices
      for (const auto label_id : index_info.label) {
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        node_indexes.push_back(nlohmann::json::object({
            {"labels", {storage->LabelToName(label_id)}},
            {"properties", nlohmann::json::array()},
            {"count", storage_acc->ApproximateVertexCount(label_id)},
            {"type", "label"},

        }));
      }
      // Vertex label property indices
      for (const auto &[label_id, property_paths] : index_info.label_properties) {
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        auto const path_to_name = [&](const storage::PropertyPath &property_path) {
          return PropertyPathToName(storage, property_path);
        };

        auto props = property_paths | rv::transform(path_to_name) | r::to_vector;
        node_indexes.push_back(nlohmann::json::object({
            {"labels", {storage->LabelToName(label_id)}},
            {"properties", props},
            {"count", storage_acc->ApproximateVertexCount(label_id, property_paths)},
            {"type", "label+properties"},
        }));
      }
      // Vertex label text
      for (const auto &[index_name, label_id, properties] : index_info.text_indices) {
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        auto prop_names = properties | rv::transform([storage](const storage::PropertyId &property) {
                            return storage->PropertyToName(property);
                          }) |
                          r::to_vector;
        node_indexes.push_back(
            nlohmann::json::object({{"labels", {storage->LabelToName(label_id)}},
                                    {"properties", prop_names},
                                    {"count", storage_acc->ApproximateVerticesTextCount(index_name).value_or(0)},
                                    {"text", index_name},
                                    {"type", "label_text"}}));
      }
      // Vertex label property_point
      for (const auto &[label_id, property] : index_info.point_label_property) {
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        node_indexes.push_back(nlohmann::json::object(
            {{"labels", {storage->LabelToName(label_id)}},
             {"properties", {storage->PropertyToName(property)}},
             {"count", storage_acc->ApproximateVerticesPointCount(label_id, property).value_or(0)},
             {"type", "label+property_point"}}));
      }

      // Vertex label property_vector
      for (const auto &spec : index_info.vector_indices_spec) {
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{spec.label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        node_indexes.push_back(nlohmann::json::object(
            {{"labels", {storage->LabelToName(spec.label_id)}},
             {"properties", {storage->PropertyToName(spec.property)}},
             {"count", storage_acc->ApproximateVerticesVectorCount(spec.label_id, spec.property).value_or(0)},
             {"type", "label+property_vector"}}));
      }

      // Edge type indices
      for (const auto type : index_info.edge_type) {
#ifdef MG_ENTERPRISE
        if (auth_checker && !auth_checker->Has(type, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        edge_indexes.push_back(nlohmann::json::object({
            {"edge_type", {storage->EdgeTypeToName(type)}},
            {"properties", nlohmann::json::array()},
            {"count", storage_acc->ApproximateEdgeCount(type)},
            {"type", "edge_type"},
        }));
      }
      // Edge type property indices
      for (const auto &[type, property] : index_info.edge_type_property) {
#ifdef MG_ENTERPRISE
        if (auth_checker && !auth_checker->Has(type, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        edge_indexes.push_back(nlohmann::json::object({
            {"edge_type", {storage->EdgeTypeToName(type)}},
            {"properties", {storage->PropertyToName(property)}},
            {"count", storage_acc->ApproximateEdgeCount(type, property)},
            {"type", "edge_type+property"},
        }));
      }
      // Edge property indices
      for (const auto &property : index_info.edge_property) {
        edge_indexes.push_back(nlohmann::json::object({
            {"properties", {storage->PropertyToName(property)}},
            {"count", storage_acc->ApproximateEdgeCount(property)},
            {"type", "edge_property"},
        }));
      }
      // Edge type property_vector
      for (const auto &spec : index_info.vector_edge_indices_spec) {
#ifdef MG_ENTERPRISE
        if (auth_checker && !auth_checker->Has(spec.edge_type_id, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        node_indexes.push_back(nlohmann::json::object(
            {{"edge_type", {storage->EdgeTypeToName(spec.edge_type_id)}},
             {"properties", {storage->PropertyToName(spec.property)}},
             {"count", storage_acc->ApproximateEdgesVectorCount(spec.edge_type_id, spec.property).value_or(0)},
             {"type", "edge_type+property_vector"}}));
      }
      // Edge type text
      for (const auto &[index_name, edge_type, properties] : index_info.text_edge_indices) {
#ifdef MG_ENTERPRISE
        if (auth_checker && !auth_checker->Has(edge_type, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        auto prop_names =
            properties |
            rv::transform([storage](storage::PropertyId property_id) { return storage->PropertyToName(property_id); }) |
            r::to_vector;
        node_indexes.push_back(
            nlohmann::json::object({{"edge_type", {storage->EdgeTypeToName(edge_type)}},
                                    {"properties", std::move(prop_names)},
                                    {"count", storage_acc->ApproximateEdgesTextCount(index_name).value_or(0)},
                                    {"type", "edge_type_text"}}));
      }
      json.emplace("node_indexes", std::move(node_indexes));
      json.emplace("edge_indexes", std::move(edge_indexes));

      // CONSTRAINTS
      auto node_constraints = nlohmann::json::array();
      auto constraint_info = db_acc->ListAllConstraints();
      // Existence
      for (const auto &[label_id, property] : constraint_info.existence) {
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
        node_constraints.push_back(nlohmann::json::object({{"type", "existence"},
                                                           {"labels", {storage->LabelToName(label_id)}},
                                                           {"properties", {storage->PropertyToName(property)}}}));
      }
      // Unique
      for (const auto &[label_id, properties] : constraint_info.unique) {
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
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
#ifdef MG_ENTERPRISE
        if (auth_checker &&
            !auth_checker->Has(std::vector<storage::LabelId>{label_id}, AuthQuery::FineGrainedPrivilege::READ)) {
          continue;
        }
#endif
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
      throw QueryException(
          "SchemaInfo query is disabled. To enable it, start Memgraph with the --schema-info-enabled flag.");
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
                       RWType::R};
}  // namespace memgraph::query

PreparedQuery PrepareUserProfileQuery(ParsedQuery parsed_query, InterpreterContext *interpreter_context,
                                      Interpreter *interpreter) {
#ifdef MG_ENTERPRISE
  if (!license::global_license_checker.IsEnterpriseValidFast()) {
    throw QueryRuntimeException(
        license::LicenseCheckErrorToString(license::LicenseCheckError::NOT_ENTERPRISE_LICENSE, "user-profiles"));
  }

  auto *query = utils::Downcast<UserProfileQuery>(parsed_query.query);
  const bool is_replica = interpreter_context->repl_state.ReadLock()->IsReplica();

  Callback callback;
  // TODO: MemoryResource for EvaluationContext, it should probably be passed as
  // the argument to Callback.
  EvaluationContext evaluation_context;
  evaluation_context.timestamp = QueryTimestamp();
  evaluation_context.parameters = parsed_query.parameters;
  auto evaluator = PrimitiveLiteralExpressionEvaluator{evaluation_context};

  auto evaluate_literals = [&evaluator](UserProfileQuery::limits_t &limits) {
    for (auto &[_, limit_value] : limits) {
      switch (limit_value.type) {
        case query::UserProfileQuery::LimitValueResult::Type::UNLIMITED:
          // Nothing to evaluate
          break;
        case query::UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT: {
          const auto val = limit_value.mem_limit.expr->Accept(evaluator);
          if (val.IsInt() && val.ValueInt() > 0) {
            limit_value.mem_limit.value = val.ValueInt();
          } else {
            throw QueryException("Expected positive integer value for memory limit.");
          }
        } break;
        case query::UserProfileQuery::LimitValueResult::Type::QUANTITY: {
          const auto val = limit_value.quantity.expr->Accept(evaluator);
          if (val.IsInt() && val.ValueInt() > 0) {
            limit_value.quantity.value = val.ValueInt();
          } else {
            throw QueryException("Expected positive integer value for quantity limit.");
          }
        } break;
      }
    }
  };

  // Evaluate expressions and update limits with values (has to be done before callback)
  evaluate_literals(query->limits_);

  switch (query->action_) {
    case UserProfileQuery::Action::CREATE: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }
      callback.fn = [auth = interpreter_context->auth, profile_name = std::move(query->profile_name_),
                     limits = std::move(query->limits_), interpreter]() {
        if (!interpreter->system_transaction_) {
          throw QueryRuntimeException("Expected to be in a system transaction");
        }
        auth->CreateProfile(profile_name, limits, {/* no linked users */}, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>{};
      };
    } break;
    case UserProfileQuery::Action::UPDATE: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }
      callback.fn = [auth = interpreter_context->auth, profile_name = std::move(query->profile_name_),
                     limits = std::move(query->limits_), interpreter]() {
        if (!interpreter->system_transaction_) {
          throw QueryRuntimeException("Expected to be in a system transaction");
        }
        auth->UpdateProfile(profile_name, limits, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>{};
      };
    } break;
    case UserProfileQuery::Action::DROP: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }
      callback.fn = [auth = interpreter_context->auth, profile_name = std::move(query->profile_name_),
                     limits = std::move(query->limits_), interpreter]() {
        if (!interpreter->system_transaction_) {
          throw QueryRuntimeException("Expected to be in a system transaction");
        }
        auth->DropProfile(profile_name, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>{};
      };
    } break;
    case UserProfileQuery::Action::SET: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }
      callback.fn = [auth = interpreter_context->auth, profile_name = std::move(query->profile_name_),
                     user_or_role = std::move(query->user_or_role_), interpreter]() {
        if (!interpreter->system_transaction_) {
          throw QueryRuntimeException("Expected to be in a system transaction");
        }
        if (!user_or_role) {
          throw QueryException("Expected user or role.");
        }
        auth->SetProfile(profile_name, *user_or_role, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>{};
      };
    } break;
    case UserProfileQuery::Action::CLEAR: {
      if (is_replica) {
        throw QueryException("Query forbidden on the replica!");
      }
      callback.fn = [auth = interpreter_context->auth, user_or_role = std::move(query->user_or_role_), interpreter]() {
        if (!interpreter->system_transaction_) {
          throw QueryRuntimeException("Expected to be in a system transaction");
        }
        if (!user_or_role) {
          throw QueryException("Expected user or role.");
        }
        auth->RevokeProfile(*user_or_role, &*interpreter->system_transaction_);
        return std::vector<std::vector<TypedValue>>{};
      };
    } break;
    case UserProfileQuery::Action::SHOW_ALL: {
      callback.header = {"profile"};
      callback.fn = [auth = interpreter_context->auth]() {
        std::vector<std::vector<TypedValue>> res;
        for (const auto &[name, _] : auth->AllProfiles()) {
          res.emplace_back(std::vector<TypedValue>{TypedValue(name)});
        }
        return res;
      };
    } break;
    case UserProfileQuery::Action::SHOW_ONE: {
      callback.header = {"limit", "value"};
      callback.fn = [auth = interpreter_context->auth, profile_name = std::move(query->profile_name_)] {
        std::vector<std::vector<TypedValue>> res;
        auto limits = auth->GetProfile(profile_name);
        auto limit_to_tv = [](auto limit) {
          switch (limit.type) {
            case UserProfileQuery::LimitValueResult::Type::UNLIMITED:
              return TypedValue{"UNLIMITED"};
            case UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT: {
              auto str = std::to_string(limit.mem_limit.value);
              if (limit.mem_limit.scale == 1024) {
                str += "KB";
              } else if (limit.mem_limit.scale == 1024 * 1024) {
                str += "MB";
              } else {
                str = "UNKNOWN";
              }
              return TypedValue{std::move(str)};
            }
            case UserProfileQuery::LimitValueResult::Type::QUANTITY:
              return TypedValue{(int64_t)limit.quantity.value};
          }
        };
        for (const auto &[name, value] : limits) {
          res.emplace_back(std::vector<TypedValue>{TypedValue(name), limit_to_tv(value)});
        }
        return res;
      };
    } break;
    case UserProfileQuery::Action::SHOW_USERS: {
      const bool show_user = query->show_user_.value();  // Distinguish between user/role
      callback.header = {show_user ? "user" : "role"};
      callback.fn = [auth = interpreter_context->auth, profile_name = std::move(query->profile_name_), show_user]() {
        std::vector<std::vector<TypedValue>> res;
        if (show_user) {
          for (const auto &profile : auth->GetUsernamesForProfile(profile_name)) {
            res.emplace_back(std::vector<TypedValue>{TypedValue(profile)});
          }
        } else {
          for (const auto &profile : auth->GetRolenamesForProfile(profile_name)) {
            res.emplace_back(std::vector<TypedValue>{TypedValue(profile)});
          }
        }
        return res;
      };
    } break;
    case UserProfileQuery::Action::SHOW_FOR: {
      callback.header = {"profile"};
      callback.fn = [auth = interpreter_context->auth, user_or_role = std::move(query->user_or_role_)]() {
        if (!user_or_role) {
          throw QueryException("Expected user or role.");
        }
        std::vector<std::vector<TypedValue>> res;
        std::optional<std::string> profile;
        try {
          profile = auth->GetProfileForUser(*user_or_role);
        } catch (const QueryRuntimeException & /*unused*/) {
          try {
            profile = auth->GetProfileForRole(*user_or_role);
          } catch (const QueryRuntimeException & /*unused*/) {
            throw QueryRuntimeException(fmt::format("No user or role named '{}'.", user_or_role));
          }
        }
        if (profile) {
          res.emplace_back(std::vector<TypedValue>{TypedValue(*profile)});
        } else {
          res.emplace_back(std::vector<TypedValue>{TypedValue("null")});
        }
        return res;
      };
    } break;
    case UserProfileQuery::Action::SHOW_RESOURCE_USAGE: {
      callback.header = {"resource", "usage", "limit"};
      callback.fn = [auth = interpreter_context->auth, user_or_role = std::move(query->user_or_role_),
                     resource_monitor = interpreter_context->resource_monitoring]() {
        if (!user_or_role) {
          throw QueryException("Expected user or role.");
        }
        (void)auth->GetProfileForUser(*user_or_role);  // Throws if user doesn't exist
        std::vector<std::vector<TypedValue>> res;
        const auto resource = resource_monitor->GetUser(*user_or_role);
        // Session usage
        const auto [session_usage, session_limit] = resource->GetSessions();
        res.emplace_back(std::vector<TypedValue>{
            TypedValue(auth::UserProfiles::kLimits[(int)auth::UserProfiles::Limits::kSessions]),
            TypedValue(static_cast<int64_t>(session_usage)),
            session_limit == -1 ? TypedValue("UNLIMITED") : TypedValue(static_cast<int64_t>(session_limit))});
        // Transaction memory usage
        const auto [tm_usage, tm_limit] = resource->GetTransactionsMemory();
        const auto unlimited = tm_limit == utils::TransactionsMemoryResource::kUnlimited;
        res.emplace_back(std::vector<TypedValue>{
            TypedValue(auth::UserProfiles::kLimits[(int)auth::UserProfiles::Limits::kTransactionsMemory]),
            unlimited ? TypedValue(/* NA */) : TypedValue(utils::GetReadableSize(tm_usage)),
            TypedValue(unlimited ? "UNLIMITED" : utils::GetReadableSize(tm_limit))});
        return res;
      };
    } break;
  }

  return PreparedQuery{
      std::move(callback.header), std::move(parsed_query.required_privileges),
      [callback = std::move(callback.fn), pull_plan = std::shared_ptr<PullPlanVector>{nullptr}](
          AnyStream *stream, std::optional<int> n) mutable -> std::optional<QueryHandlerResult> {
        // First run -> execute
        if (!pull_plan) {
          pull_plan = std::make_shared<PullPlanVector>(callback());
        }
        if (pull_plan->Pull(stream, n)) {
          return QueryHandlerResult::COMMIT;
        }
        return std::nullopt;
      },
      RWType::NONE, std::string{dbms::kSystemDB}  // System query => system database
  };
#else
  // here to satisfy clang-tidy
  (void)parsed_query;
  (void)interpreter_context;
  (void)interpreter;
  throw EnterpriseOnlyException();
#endif
}

std::optional<uint64_t> Interpreter::GetTransactionId() const { return current_transaction_; }

void Interpreter::BeginTransaction(QueryExtras const &extras) {
  ResetInterpreter();
  const auto prepared_query = PrepareTransactionQuery(TransactionQuery::BEGIN, extras);
  prepared_query.query_handler(nullptr, {});
}

void Interpreter::CommitTransaction() {
  const auto prepared_query = PrepareTransactionQuery(TransactionQuery::COMMIT);
  prepared_query.query_handler(nullptr, {});
  ResetInterpreter();
}

void Interpreter::RollbackTransaction() {
  const auto prepared_query = PrepareTransactionQuery(TransactionQuery::ROLLBACK);
  prepared_query.query_handler(nullptr, {});
  ResetInterpreter();
}

#ifdef MG_ENTERPRISE
auto Interpreter::Route(std::map<std::string, std::string> const &routing, std::optional<std::string> const &db)
    -> RouteResult {
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
    if (interpreter_context_->repl_state.ReadLock()->IsMain()) {
      result.servers.emplace_back(std::vector<std::string>{address->second}, "WRITE");
    } else {
      result.servers.emplace_back(std::vector<std::string>{address->second}, "READ");
    }
    return result;
  }

  auto const db_name = db.has_value() ? *db : dbms::kDefaultDB;
  return RouteResult{.servers = interpreter_context_->coordinator_state_->get().GetRoutingTable(db_name)};
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

Interpreter::ParseRes Interpreter::Parse(const std::string &query_string, UserParameters_fn params_getter,
                                         QueryExtras const &extras) {
  LogQueryMessage(fmt::format("Accepted query: {}", query_string));
#ifdef MG_ENTERPRISE
  if (!flags::CoordinationSetupInstance().IsCoordinator()) {
    MG_ASSERT(user_or_role_, "Trying to prepare a query without a query user.");
  }
#else
  MG_ASSERT(user_or_role_, "Trying to prepare a query without a query user.");
#endif
  // Handle transaction control queries.
  const auto upper_case_query = utils::ToUpperCase(query_string);
  const auto trimmed_query = utils::Trim(upper_case_query);
  const bool is_begin = trimmed_query == "BEGIN";

  // Explicit transactions define the metadata at the beginning and reuse it
  spdlog::debug(
      "{}", QueryLogWrapper{query_string,
                            (in_explicit_transaction_ && metadata_ && !is_begin) ? &*metadata_ : &extras.metadata_pv,
                            current_db_.name()});

  if (is_begin) {
    return TransactionQuery::BEGIN;
  }
  if (trimmed_query == "COMMIT") {
    return TransactionQuery::COMMIT;
  }
  if (trimmed_query == "ROLLBACK") {
    return TransactionQuery::ROLLBACK;
  }

  try {
    if (!in_explicit_transaction_) {
      // BEGIN has a different execution path; for implicit transaction start the timer as soon as possible
      current_timeout_timer_ = CreateTimeoutTimer(extras, interpreter_context_->config);
    }
    // NOTE: query_string is not BEGIN, COMMIT or ROLLBACK
    bool const is_schema_assert_query{upper_case_query.find(kSchemaAssert) != std::string::npos};
    const utils::Timer parsing_timer;
    LogQueryMessage("Query parsing started.");
    ParsedQuery parsed_query = ParseQuery(query_string, params_getter(nullptr), &interpreter_context_->ast_cache,
                                          interpreter_context_->config.query);
    auto parsing_time = parsing_timer.Elapsed().count();
    LogQueryMessage("Query parsing ended.");
    return Interpreter::ParseInfo{std::move(parsed_query), parsing_time, is_schema_assert_query};
  } catch (const utils::BasicException &e) {
    LogQueryMessage(fmt::format("Failed query: {}", e.what()));
    // Trigger first failed query
    metrics::FirstFailedQuery();
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedQuery);
    memgraph::metrics::IncrementCounter(memgraph::metrics::FailedPrepare);
    AbortCommand({});
    throw;
  }
}

struct QueryTransactionRequirements : QueryVisitor<void> {
  using QueryVisitor<void>::Visit;

  QueryTransactionRequirements(bool is_schema_assert_query, bool is_cypher_read, bool is_in_memory_transactional)
      : is_schema_assert_query_{is_schema_assert_query},
        is_cypher_read_{is_cypher_read},
        is_in_memory_transactional_{is_in_memory_transactional} {}

  // Some queries do not require a database to be executed (current_db_ won't be passed on to the Prepare*; special
  // case for use database which overwrites the current database)

  // No database access required (and current database is not needed)
  void Visit(AuthQuery & /*unused*/) override {}
  void Visit(UserProfileQuery & /*unused*/) override {}
  void Visit(MultiDatabaseQuery & /*unused*/) override {}
  void Visit(ReplicationQuery & /*unused*/) override {}
  void Visit(ShowConfigQuery & /*unused*/) override {}
  void Visit(SettingQuery & /*unused*/) override {}
  void Visit(VersionQuery & /*unused*/) override {}
  void Visit(TransactionQueueQuery & /*unused*/) override {}
  void Visit(UseDatabaseQuery & /*unused*/) override {}
  void Visit(ShowDatabaseQuery & /*unused*/) override {}
  void Visit(ShowDatabasesQuery & /*unused*/) override {}
  void Visit(ReplicationInfoQuery & /*unused*/) override {}
  void Visit(CoordinatorQuery & /*unused*/) override {}

  // No database access required (but need current database)
  void Visit(SystemInfoQuery & /*unused*/) override {}
  void Visit(LockPathQuery & /*unused*/) override {}
  void Visit(FreeMemoryQuery & /*unused*/) override {}
  void Visit(StreamQuery & /*unused*/) override {}
  void Visit(IsolationLevelQuery & /*unused*/) override {}
  void Visit(StorageModeQuery & /*unused*/) override {}
  void Visit(CreateSnapshotQuery & /*unused*/)
      override { /*CreateSnapshot is also used in a periodic way so internally will arrange its own access*/
  }
  void Visit(ShowSnapshotsQuery & /*unused*/) override {}
  void Visit(ShowNextSnapshotQuery & /* unused */) override {}
  void Visit(EdgeImportModeQuery & /*unused*/) override {}
  void Visit(AlterEnumRemoveValueQuery & /*unused*/) override { /* Not implemented yet */
  }
  void Visit(DropEnumQuery & /*unused*/) override { /* Not implemented yet */
  }
  void Visit(SessionTraceQuery & /*unused*/) override {}

  // Some queries require an active transaction in order to be prepared.
  // Unique access required
  void Visit(PointIndexQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(TextIndexQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(CreateTextEdgeIndexQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(VectorIndexQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(CreateVectorEdgeIndexQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(ConstraintQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(DropAllIndexesQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(DropAllConstraintsQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(DropGraphQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(CreateEnumQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(AlterEnumAddValueQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(AlterEnumUpdateValueQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }
  void Visit(TtlQuery & /*unused*/) override {
    // TTLQuery is UNIQUE but indices it creates are created as READ_ONLY asynchronously
    // if using IN_MEMORY_TRANSACTIONAL otherwise UNIQUE
    accessor_type_ = storage::StorageAccessType::UNIQUE;
  }
  void Visit(RecoverSnapshotQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::UNIQUE; }

  // Read access required
  void Visit(ExplainQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::READ; }
  void Visit(DumpQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::READ; }
  void Visit(AnalyzeGraphQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::READ; }
  void Visit(DatabaseInfoQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::READ; }
  void Visit(ShowEnumsQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::READ; }
  void Visit(ShowSchemaInfoQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::READ; }

  // Write access required
  void Visit(CypherQuery & /*unused*/) override {
    could_commit_ = true;
    accessor_type_ = cypher_access_type();
  }
  void Visit(ProfileQuery & /*unused*/) override { accessor_type_ = cypher_access_type(); }
  void Visit(TriggerQuery & /*unused*/) override { accessor_type_ = storage::StorageAccessType::WRITE; }

  // Complex access logic
  void Visit(IndexQuery &index_query) override {
    using enum storage::StorageAccessType;
    if (is_in_memory_transactional_) {
      // Concurrent population of index requires snapshot isolation
      isolation_level_override_ = storage::IsolationLevel::SNAPSHOT_ISOLATION;
      accessor_type_ = (index_query.action_ == IndexQuery::Action::CREATE) ? READ_ONLY : READ;
    } else {
      // IN_MEMORY_ANALYTICAL and ON_DISK_TRANSACTIONAL require unique access
      accessor_type_ = UNIQUE;
    }
  }
  void Visit(EdgeIndexQuery &edge_index_query) override {
    using enum storage::StorageAccessType;
    if (is_in_memory_transactional_) {
      // Concurrent population of index requires snapshot isolation
      isolation_level_override_ = storage::IsolationLevel::SNAPSHOT_ISOLATION;
      accessor_type_ = (edge_index_query.action_ == EdgeIndexQuery::Action::CREATE) ? READ_ONLY : READ;
    } else {
      // IN_MEMORY_ANALYTICAL and ON_DISK_TRANSACTIONAL require unique access
      accessor_type_ = UNIQUE;
    }
  }

  // helper methods
  auto cypher_access_type() const -> storage::StorageAccessType {
    using enum storage::StorageAccessType;
    if (is_schema_assert_query_) {
      return UNIQUE;
    }
    if (is_cypher_read_) {
      return READ;
    }
    return WRITE;
  }

  bool const is_schema_assert_query_;
  bool const is_cypher_read_;
  bool const is_in_memory_transactional_;

  bool could_commit_ = false;
  std::optional<storage::IsolationLevel> isolation_level_override_;
  std::optional<storage::StorageAccessType> accessor_type_;
};

Interpreter::PrepareResult Interpreter::Prepare(ParseRes parse_res, UserParameters_fn params_getter,
                                                QueryExtras const &extras) {
  if (std::holds_alternative<TransactionQuery>(parse_res)) {
    const auto tx_query_enum = std::get<TransactionQuery>(parse_res);
    if (tx_query_enum == TransactionQuery::BEGIN) {
      ResetInterpreter();
    }
    // TODO: Use CreateThreadSafe once parallel multi-command queries are supported
    auto &query_execution = query_executions_.emplace_back(QueryExecution::Create());
    query_execution->prepared_query = PrepareTransactionQuery(tx_query_enum, extras);
    auto qid = in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};
    return {query_execution->prepared_query->header, query_execution->prepared_query->privileges, qid, {}};
  }

  MG_ASSERT(std::holds_alternative<ParseInfo>(parse_res), "Unkown ParseRes type");

  auto &parse_info = std::get<ParseInfo>(parse_res);
  auto &parsed_query = parse_info.parsed_query;

  // All queries other than transaction control queries advance the command in
  // an explicit transaction block.
  if (in_explicit_transaction_) {
    if (parse_info.is_schema_assert_query) {
      throw SchemaAssertInMulticommandTxException();
    }

    transaction_queries_->push_back(parsed_query.query_string);
    AdvanceCommand();
  } else {
    ResetInterpreter();
    transaction_queries_->push_back(parsed_query.query_string);
    if (current_db_.db_transactional_accessor_ /* && !in_explicit_transaction_*/) {
      // If we're not in an explicit transaction block and we have an open
      // transaction, abort it since we're about to prepare a new query.
      AbortCommand(nullptr);
    }

    SetupInterpreterTransaction(extras);
    LogQueryMessage(
        fmt::format("Query [{}] associated with transaction [{}]", parsed_query.query_string, *current_transaction_));
  }

  auto *const cypher_query = utils::Downcast<CypherQuery>(parsed_query.query);

  // Load parquet uses thread safe allocator that's why it is being checked here
  bool has_load_parquet{false};

  if (cypher_query) {
    auto clauses = cypher_query->single_query_->clauses_;
    has_load_parquet =
        std::ranges::any_of(clauses, [](const auto *clause) { return clause->GetTypeInfo() == LoadParquet::kType; });
  }

  std::unique_ptr<QueryExecution> *query_execution_ptr = nullptr;
  try {
    // Setup QueryExecution
    // TODO: Use CreateThreadSafe for multi-threaded queries
    if (has_load_parquet) {
      query_executions_.emplace_back(QueryExecution::CreateThreadSafe());
    } else {
      query_executions_.emplace_back(QueryExecution::Create());
    }
    auto &query_execution = query_executions_.back();
    query_execution_ptr = &query_execution;

    std::optional<int> qid =
        in_explicit_transaction_ ? static_cast<int>(query_executions_.size() - 1) : std::optional<int>{};

    query_execution->summary["parsing_time"] = parse_info.parsing_time;
    LogQueryMessage(fmt::format("Query parsing time: {}", parse_info.parsing_time));

    // Set a default cost estimate of 0. Individual queries can overwrite this
    // field with an improved estimate.
    query_execution->summary["cost_estimate"] = 0.0;

    // System queries require strict ordering; since there is no MVCC-like thing, we allow single queries
    bool system_queries =
        utils::Downcast<AuthQuery>(parsed_query.query) || utils::Downcast<MultiDatabaseQuery>(parsed_query.query) ||
        utils::Downcast<ReplicationQuery>(parsed_query.query) || utils::Downcast<UserProfileQuery>(parsed_query.query);

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

    if (!in_explicit_transaction_) {
      auto const is_in_memory_transactional = current_db_.db_acc_
                                                  ? (current_db_.db_acc_.value()->storage()->GetStorageMode() ==
                                                     storage::StorageMode::IN_MEMORY_TRANSACTIONAL)
                                                  : false;
      auto transaction_requirements = QueryTransactionRequirements{
          parse_info.is_schema_assert_query, parsed_query.is_cypher_read, is_in_memory_transactional};
      parsed_query.query->Accept(transaction_requirements);
      if (transaction_requirements.accessor_type_) {
        if (transaction_requirements.isolation_level_override_) {
          SetNextTransactionIsolationLevel(*transaction_requirements.isolation_level_override_);
        }
        SetupDatabaseTransaction(transaction_requirements.could_commit_, *transaction_requirements.accessor_type_);
      }
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

    const utils::Timer planning_timer;  // TODO: Think about moving it to Parse()
    LogQueryMessage("Query planning started!");
    PreparedQuery prepared_query;
    utils::MemoryResource *memory_resource = query_execution->resource();
    frame_change_collector_.reset();
    frame_change_collector_.emplace();

    auto make_stopping_context = [&]() {
      return StoppingContext{
          .transaction_status = &transaction_status_,
          .is_shutting_down = &interpreter_context_->is_shutting_down,
          .timer = current_timeout_timer_,
      };
    };

    if (utils::Downcast<CypherQuery>(parsed_query.query)) {
      prepared_query = PrepareCypherQuery(std::move(parsed_query), &query_execution->summary, interpreter_context_,
                                          current_db_, memory_resource, &query_execution->notifications, user_or_role_,
                                          make_stopping_context(), *this, &*frame_change_collector_
#ifdef MG_ENTERPRISE
                                          ,
                                          user_resource_
#endif
      );
    } else if (utils::Downcast<ExplainQuery>(parsed_query.query)) {
      prepared_query = PrepareExplainQuery(std::move(parsed_query), &query_execution->summary,
                                           &query_execution->notifications, interpreter_context_, *this, current_db_);
    } else if (utils::Downcast<ProfileQuery>(parsed_query.query)) {
      prepared_query =
          PrepareProfileQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->summary,
                              &query_execution->notifications, interpreter_context_, *this, current_db_,
                              memory_resource, user_or_role_, make_stopping_context(), &*frame_change_collector_
#ifdef MG_ENTERPRISE
                              ,
                              user_resource_
#endif
          );
    } else if (utils::Downcast<DumpQuery>(parsed_query.query)) {
      prepared_query = PrepareDumpQuery(std::move(parsed_query), current_db_);
    } else if (utils::Downcast<IndexQuery>(parsed_query.query)) {
      prepared_query = PrepareIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                         &query_execution->notifications, current_db_, make_stopping_context());
    } else if (utils::Downcast<DropAllIndexesQuery>(parsed_query.query)) {
      prepared_query = PrepareDropAllIndexesQuery(std::move(parsed_query), in_explicit_transaction_,
                                                  &query_execution->notifications, current_db_);
    } else if (utils::Downcast<DropAllConstraintsQuery>(parsed_query.query)) {
      prepared_query = PrepareDropAllConstraintsQuery(std::move(parsed_query), in_explicit_transaction_,
                                                      &query_execution->notifications, current_db_);
    } else if (utils::Downcast<EdgeIndexQuery>(parsed_query.query)) {
      prepared_query = PrepareEdgeIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                             &query_execution->notifications, current_db_, make_stopping_context());
    } else if (utils::Downcast<PointIndexQuery>(parsed_query.query)) {
      prepared_query = PreparePointIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                              &query_execution->notifications, current_db_);
    } else if (utils::Downcast<TextIndexQuery>(parsed_query.query)) {
      prepared_query = PrepareTextIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                             &query_execution->notifications, current_db_);
    } else if (utils::Downcast<CreateTextEdgeIndexQuery>(parsed_query.query)) {
      prepared_query = PrepareCreateTextEdgeIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                                       &query_execution->notifications, current_db_);
    } else if (utils::Downcast<VectorIndexQuery>(parsed_query.query)) {
      prepared_query = PrepareVectorIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                               &query_execution->notifications, current_db_);
    } else if (utils::Downcast<CreateVectorEdgeIndexQuery>(parsed_query.query)) {
      prepared_query = PrepareCreateVectorEdgeIndexQuery(std::move(parsed_query), in_explicit_transaction_,
                                                         &query_execution->notifications, current_db_);
    } else if (utils::Downcast<TtlQuery>(parsed_query.query)) {
#ifdef MG_ENTERPRISE
      prepared_query = PrepareTtlQuery(std::move(parsed_query), in_explicit_transaction_,
                                       &query_execution->notifications, current_db_, interpreter_context_);
#else
      throw EnterpriseOnlyException();
#endif  // MG_ENTERPRISE
    } else if (utils::Downcast<AnalyzeGraphQuery>(parsed_query.query)) {
      prepared_query = PrepareAnalyzeGraphQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<AuthQuery>(parsed_query.query)) {
      /// SYSTEM (Replication) PURE
      prepared_query = PrepareAuthQuery(std::move(parsed_query), in_explicit_transaction_, interpreter_context_, *this,
                                        current_db_.db_acc_);
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
      if (!current_db_.db_acc_ ||
          current_db_.db_acc_->get()->GetStorageMode() != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
        throw QueryRuntimeException("Replication query requires IN_MEMORY_TRANSACTIONAL mode.");
      }
      prepared_query =
          PrepareReplicationQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                                  *interpreter_context_->replication_handler_, current_db_, interpreter_context_->config
#ifdef MG_ENTERPRISE
                                  ,
                                  interpreter_context_->coordinator_state_
#endif
          );

    } else if (utils::Downcast<ReplicationInfoQuery>(parsed_query.query)) {
      prepared_query = PrepareReplicationInfoQuery(std::move(parsed_query), in_explicit_transaction_,
                                                   *interpreter_context_->replication_handler_);

    } else if (utils::Downcast<CoordinatorQuery>(parsed_query.query)) {
#ifdef MG_ENTERPRISE
      if (!interpreter_context_->coordinator_state_.has_value()) {
        throw QueryRuntimeException(
            "Coordinator was not initialized as coordinator port, coordinator id or management port were not "
            "set.");
      }
      prepared_query =
          PrepareCoordinatorQuery(std::move(parsed_query), in_explicit_transaction_, &query_execution->notifications,
                                  *interpreter_context_->coordinator_state_, interpreter_context_->config);
#else
      throw EnterpriseOnlyException();
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
      prepared_query = PrepareCreateSnapshotQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<RecoverSnapshotQuery>(parsed_query.query)) {
      auto const replication_role = interpreter_context_->repl_state.ReadLock()->GetRole();
      prepared_query =
          PrepareRecoverSnapshotQuery(std::move(parsed_query), in_explicit_transaction_, current_db_, replication_role);
    } else if (utils::Downcast<ShowSnapshotsQuery>(parsed_query.query)) {
      prepared_query = PrepareShowSnapshotsQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
    } else if (utils::Downcast<ShowNextSnapshotQuery>(parsed_query.query)) {
      prepared_query = PrepareShowNextSnapshotQuery(std::move(parsed_query), in_explicit_transaction_, current_db_);
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
      prepared_query = PrepareMultiDatabaseQuery(std::move(parsed_query), interpreter_context_, *this);
    } else if (utils::Downcast<UseDatabaseQuery>(parsed_query.query)) {
      if (in_explicit_transaction_) {
        throw UseDatabaseQueryInMulticommandTxException();
      }
      prepared_query = PrepareUseDatabaseQuery(std::move(parsed_query), current_db_, interpreter_context_, on_change_);
    } else if (utils::Downcast<ShowDatabaseQuery>(parsed_query.query)) {
      prepared_query = PrepareShowDatabaseQuery(std::move(parsed_query), current_db_);
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
      prepared_query = PrepareShowSchemaInfoQuery(parsed_query, current_db_
#ifdef MG_ENTERPRISE
                                                  ,
                                                  interpreter_context_, user_or_role_
#endif
      );
    } else if (utils::Downcast<SessionTraceQuery>(parsed_query.query)) {
      prepared_query = PrepareSessionTraceQuery(std::move(parsed_query), current_db_, this);
    } else if (utils::Downcast<UserProfileQuery>(parsed_query.query)) {
      prepared_query = PrepareUserProfileQuery(std::move(parsed_query), interpreter_context_, this);
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
      // TODO: This is a catch all for operations that should not be allowed to run via user query on REPLICA
      //       prefer more explicit EnsureMainInstance(interpreter_context, "XYZ operations");
      if (interpreter_context_->repl_state.ReadLock()->IsReplica()) {
        query_execution = nullptr;
        throw WriteQueryOnReplicaException();
      }
#ifdef MG_ENTERPRISE
      if (!interpreter_context_->repl_state.ReadLock()->IsMainWriteable()) {
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

void Interpreter::CheckAuthorized(std::vector<AuthQuery::Privilege> const &privileges, std::optional<std::string> db) {
  if (user_or_role_ && !user_or_role_->IsAuthorized(privileges, db, &query::session_long_policy)) {
    Abort();
    if (!db) {
      throw QueryException(
          "You are not authorized to execute this query! Please contact your database administrator. This issue comes "
          "from the user having not enough role-based access privileges to execute this query. If you want this issue "
          "to be resolved, ask your database administrator to grant you a specific privilege for query execution.");
    }
    throw QueryException(
        "You are not authorized to execute this query on database \"{}\"! Please contact your database "
        "administrator. This issue comes from the user having not enough role-based access privileges to execute this "
        "query. If you want this issue to be resolved, ask your database administrator to grant you a specific "
        "privilege for query execution.",
        db.value());
  }
}

void Interpreter::SetupDatabaseTransaction(bool couldCommit, storage::StorageAccessType acc_type) {
  current_db_.SetupDatabaseTransaction(GetIsolationLevelOverride(), couldCommit, acc_type);
}

void Interpreter::SetupInterpreterTransaction(const QueryExtras &extras) {
  metrics::IncrementCounter(metrics::ActiveTransactions);
  transaction_status_.store(TransactionStatus::ACTIVE, std::memory_order_release);
  current_transaction_ = interpreter_context_->id_handler.next();
  if (query_logger_) {
    query_logger_->SetTransactionId(std::to_string(*current_transaction_));
  }
  metadata_ = GenOptional(extras.metadata_pv);
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
#ifdef MG_ENTERPRISE
  if (user_resource_ && current_db_.db_transactional_accessor_) {
    const auto leftover = current_db_.db_transactional_accessor_->GetTransactionMemoryTracker().Amount();
    user_resource_->DecrementTransactionsMemory(leftover);
  }
#endif

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

auto make_commit_arg(bool is_main, dbms::DatabaseAccess const &db_acc) {
  if (is_main) {
    auto protector = dbms::DatabaseProtector{db_acc};
    return storage::CommitArgs::make_main(protector.clone());
  }
  return storage::CommitArgs::make_replica_read();
}

void RunTriggersAfterCommit(dbms::DatabaseAccess db_acc, InterpreterContext *interpreter_context,
                            TriggerContext original_trigger_context) {
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
      auto is_main = interpreter_context->repl_state.ReadLock()->IsMain();
      trigger.Execute(&db_accessor, db_acc, execution_memory.resource(), flags::run_time::GetExecutionTimeout(),
                      &interpreter_context->is_shutting_down, /* transaction_status = */ nullptr, trigger_context,
                      is_main);
    } catch (const utils::BasicException &exception) {
      spdlog::warn("Trigger '{}' failed with exception:\n{}", trigger.Name(), exception.what());
      db_accessor.Abort();
      continue;
    }

    auto maybe_commit_error = std::invoke([&]() {
      auto locked_repl_state = interpreter_context->repl_state.ReadLock();
      const bool is_main = locked_repl_state->IsMain();
      return db_accessor.Commit(make_commit_arg(is_main, db_acc));
    });

    if (maybe_commit_error.HasError()) {
      const auto &error = maybe_commit_error.GetError();

      std::visit(
          [&trigger, &db_accessor]<typename T>(T &&arg) {
            using ErrorType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<ErrorType, storage::SyncReplicationError>) {
              spdlog::warn("At least one SYNC replica has not confirmed execution of the trigger '{}'.",
                           trigger.Name());
            } else if constexpr (std::is_same_v<ErrorType, storage::StrictSyncReplicationError>) {
              spdlog::warn(
                  "At least one STRICT_SYNC replica has not confirmed execution of the trigger '{}'. Transaction will "
                  "be "
                  "aborted. ",
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
                default:
                  LOG_FATAL("Unknown ConstraintViolation type");
                  ;
              }
            } else if constexpr (std::is_same_v<ErrorType, storage::SerializationError>) {
              throw QueryException(MessageWithDocsLink(
                  "Unable to commit due to serialization error. Try retrying this transaction when the conflicting "
                  "transaction is finished."));
            } else if constexpr (std::is_same_v<ErrorType, storage::PersistenceError>) {
              throw QueryException("Unable to commit due to persistance error.");
            } else if constexpr (std::is_same_v<ErrorType, storage::ReplicaShouldNotWriteError>) {
              throw QueryException("Queries on replica shouldn't write.");
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
#ifdef MG_ENTERPRISE
  if (user_resource_ && current_db_.db_transactional_accessor_) {
    const auto leftover = current_db_.db_transactional_accessor_->GetTransactionMemoryTracker().Amount();
    user_resource_->DecrementTransactionsMemory(leftover);
  }
#endif

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
      // We cannot simply put it to IDLE, since the status is used as a synchronization method and we have to follow
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
        auto is_main = interpreter_context_->repl_state.ReadLock()->IsMain();
        trigger.Execute(&*current_db_.execution_db_accessor_, *current_db_.db_acc_, execution_memory.resource(),
                        flags::run_time::GetExecutionTimeout(), &interpreter_context_->is_shutting_down,
                        &transaction_status_, *trigger_context, is_main);
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
  utils::OnScopeExit const reset_members(reset_necessary_members);

  bool commit_confirmed_by_all_sync_replicas{true};
  bool commit_confirmed_by_all_strict_sync_replicas{true};

  auto locked_repl_state = std::optional{interpreter_context_->repl_state.ReadLock()};
  bool const is_main = locked_repl_state.value()->IsMain();
  auto *curr_txn = current_db_.db_transactional_accessor_->GetTransaction();
  // if I was main with write txn which became replica, abort.
  if (!is_main && !curr_txn->deltas.empty()) {
    throw QueryException("Cannot commit because instance is not main anymore.");
  }
  auto maybe_commit_error =
      current_db_.db_transactional_accessor_->PrepareForCommitPhase(make_commit_arg(is_main, *current_db_.db_acc_));
  // Proactively unlock repl_state
  locked_repl_state.reset();

  if (maybe_commit_error.HasError()) {
    const auto &error = maybe_commit_error.GetError();

    std::visit(
        [&execution_db_accessor = current_db_.execution_db_accessor_, &commit_confirmed_by_all_sync_replicas,
         &commit_confirmed_by_all_strict_sync_replicas]<typename T>(const T &arg) {
          using ErrorType = std::remove_cvref_t<T>;
          if constexpr (std::is_same_v<ErrorType, storage::SyncReplicationError>) {
            commit_confirmed_by_all_sync_replicas = false;
          } else if constexpr (std::is_same_v<ErrorType, storage::StrictSyncReplicationError>) {
            commit_confirmed_by_all_strict_sync_replicas = false;
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
              default:
                LOG_FATAL("Unknown constraint violation type");
            }
          } else if constexpr (std::is_same_v<ErrorType, storage::SerializationError>) {
            throw QueryException("Unable to commit due to serialization error.");
          } else if constexpr (std::is_same_v<ErrorType, storage::PersistenceError>) {
            throw QueryException("Unable to commit due to persistence error.");
          } else if constexpr (std::is_same_v<ErrorType, storage::ReplicaShouldNotWriteError>) {
            throw QueryException("Queries on replica shouldn't write.");
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
    db->AddTask([db_acc = *current_db_.db_acc_, interpreter_context = interpreter_context_,
                 trigger_context = std::move(*trigger_context),
                 user_transaction = std::shared_ptr(std::move(current_db_.db_transactional_accessor_))]() mutable {
      RunTriggersAfterCommit(db_acc, interpreter_context, std::move(trigger_context));
      user_transaction->FinalizeTransaction();
      SPDLOG_DEBUG("Finished executing after commit triggers");  // NOLINT(bugprone-lambda-function-name)
    });
  }

  SPDLOG_DEBUG("Finished committing the transaction");
  if (!commit_confirmed_by_all_sync_replicas) {
    throw ReplicationException("At least one SYNC replica has not confirmed committing last transaction.");
  }

  if (!commit_confirmed_by_all_strict_sync_replicas) {
    throw ReplicationException(
        "At least one STRICT_SYNC replica has not confirmed committing last transaction. Transaction will be aborted "
        "on all instances.");
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

#ifdef MG_ENTERPRISE
void Interpreter::SetUser(std::shared_ptr<QueryUserOrRole> user_or_role,
                          std::shared_ptr<utils::UserResources> user_resource) {
  user_or_role_ = std::move(user_or_role);
  if (query_logger_) {
    std::string username;
    if (user_or_role_ && user_or_role_->username()) {
      username = user_or_role_->username().value();
    }
    query_logger_->SetUser(username);
  }
  // Pre-existsing user resource; decrement session (since it is not being used anymore)
  if (user_resource_) {
    user_resource_->DecrementSessions();
    user_resource_.reset();
  }
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    // Monitoring always, resource limit check only if license is valid
    if (user_resource && !user_resource->IncrementSessions()) {
      throw auth::AuthException("User exceeded session limit.");
    }
    user_resource_ = std::move(user_resource);
  }
}
#else
void Interpreter::SetUser(std::shared_ptr<QueryUserOrRole> user_or_role) {
  user_or_role_ = std::move(user_or_role);
  if (query_logger_) {
    std::string username;
    if (user_or_role_ && user_or_role_->username()) {
      username = user_or_role_->username().value();
    }
    query_logger_->SetUser(username);
  }
}
#endif

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
#ifdef MG_ENTERPRISE
  if (user_resource_) {
    user_resource_->DecrementSessions();
    user_resource_.reset();
  }
#endif
}

bool Interpreter::IsQueryLoggingActive() const { return query_logger_.has_value(); }

void Interpreter::LogQueryMessage(std::string message) {
  if (query_logger_.has_value()) {
    (*query_logger_).trace(message);
  }
}

}  // namespace memgraph::query
