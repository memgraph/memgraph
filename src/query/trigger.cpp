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

#include "query/trigger.hpp"

#include "query/config.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "query/query_user.hpp"
#include "query/serialization/property_value.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/event_counter.hpp"
#include "utils/memory.hpp"

namespace memgraph::metrics {
extern const Event TriggersExecuted;
}  // namespace memgraph::metrics

namespace memgraph::query {
namespace {
auto IdentifierString(const TriggerIdentifierTag tag) noexcept {
  switch (tag) {
    case TriggerIdentifierTag::CREATED_VERTICES:
      return "createdVertices";

    case TriggerIdentifierTag::CREATED_EDGES:
      return "createdEdges";

    case TriggerIdentifierTag::CREATED_OBJECTS:
      return "createdObjects";

    case TriggerIdentifierTag::DELETED_VERTICES:
      return "deletedVertices";

    case TriggerIdentifierTag::DELETED_EDGES:
      return "deletedEdges";

    case TriggerIdentifierTag::DELETED_OBJECTS:
      return "deletedObjects";

    case TriggerIdentifierTag::SET_VERTEX_PROPERTIES:
      return "setVertexProperties";

    case TriggerIdentifierTag::SET_EDGE_PROPERTIES:
      return "setEdgeProperties";

    case TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES:
      return "removedVertexProperties";

    case TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES:
      return "removedEdgeProperties";

    case TriggerIdentifierTag::SET_VERTEX_LABELS:
      return "setVertexLabels";

    case TriggerIdentifierTag::REMOVED_VERTEX_LABELS:
      return "removedVertexLabels";

    case TriggerIdentifierTag::UPDATED_VERTICES:
      return "updatedVertices";

    case TriggerIdentifierTag::UPDATED_EDGES:
      return "updatedEdges";

    case TriggerIdentifierTag::UPDATED_OBJECTS:
      return "updatedObjects";
  }
}

template <typename T>
concept SameAsIdentifierTag = std::same_as<T, TriggerIdentifierTag>;

template <SameAsIdentifierTag... TArgs>
std::vector<std::pair<Identifier, TriggerIdentifierTag>> TagsToIdentifiers(const TArgs &...args) {
  std::vector<std::pair<Identifier, TriggerIdentifierTag>> identifiers;
  identifiers.reserve(sizeof...(args));

  auto add_identifier = [&identifiers](const auto tag) {
    identifiers.emplace_back(Identifier{IdentifierString(tag), false}, tag);
  };

  (add_identifier(args), ...);

  return identifiers;
};

std::vector<std::pair<Identifier, TriggerIdentifierTag>> GetPredefinedIdentifiers(const TriggerEventType event_type) {
  using IdentifierTag = TriggerIdentifierTag;
  using EventType = TriggerEventType;

  switch (event_type) {
    case EventType::ANY:
      return TagsToIdentifiers(
          IdentifierTag::CREATED_VERTICES, IdentifierTag::CREATED_EDGES, IdentifierTag::CREATED_OBJECTS,
          IdentifierTag::DELETED_VERTICES, IdentifierTag::DELETED_EDGES, IdentifierTag::DELETED_OBJECTS,
          IdentifierTag::SET_VERTEX_PROPERTIES, IdentifierTag::REMOVED_VERTEX_PROPERTIES,
          IdentifierTag::SET_VERTEX_LABELS, IdentifierTag::REMOVED_VERTEX_LABELS, IdentifierTag::UPDATED_VERTICES,
          IdentifierTag::SET_EDGE_PROPERTIES, IdentifierTag::REMOVED_EDGE_PROPERTIES, IdentifierTag::UPDATED_EDGES,
          IdentifierTag::UPDATED_OBJECTS);

    case EventType::CREATE:
      return TagsToIdentifiers(IdentifierTag::CREATED_VERTICES, IdentifierTag::CREATED_EDGES,
                               IdentifierTag::CREATED_OBJECTS);

    case EventType::VERTEX_CREATE:
      return TagsToIdentifiers(IdentifierTag::CREATED_VERTICES);

    case EventType::EDGE_CREATE:
      return TagsToIdentifiers(IdentifierTag::CREATED_EDGES);

    case EventType::DELETE:
      return TagsToIdentifiers(IdentifierTag::DELETED_VERTICES, IdentifierTag::DELETED_EDGES,
                               IdentifierTag::DELETED_OBJECTS);

    case EventType::VERTEX_DELETE:
      return TagsToIdentifiers(IdentifierTag::DELETED_VERTICES);

    case EventType::EDGE_DELETE:
      return TagsToIdentifiers(IdentifierTag::DELETED_EDGES);

    case EventType::UPDATE:
      return TagsToIdentifiers(IdentifierTag::SET_VERTEX_PROPERTIES, IdentifierTag::REMOVED_VERTEX_PROPERTIES,
                               IdentifierTag::SET_VERTEX_LABELS, IdentifierTag::REMOVED_VERTEX_LABELS,
                               IdentifierTag::UPDATED_VERTICES, IdentifierTag::SET_EDGE_PROPERTIES,
                               IdentifierTag::REMOVED_EDGE_PROPERTIES, IdentifierTag::UPDATED_EDGES,
                               IdentifierTag::UPDATED_OBJECTS);

    case EventType::VERTEX_UPDATE:
      return TagsToIdentifiers(IdentifierTag::SET_VERTEX_PROPERTIES, IdentifierTag::REMOVED_VERTEX_PROPERTIES,
                               IdentifierTag::SET_VERTEX_LABELS, IdentifierTag::REMOVED_VERTEX_LABELS,
                               IdentifierTag::UPDATED_VERTICES);

    case EventType::EDGE_UPDATE:
      return TagsToIdentifiers(IdentifierTag::SET_EDGE_PROPERTIES, IdentifierTag::REMOVED_EDGE_PROPERTIES,
                               IdentifierTag::UPDATED_EDGES);
  }
}
}  // namespace

Trigger::Trigger(std::string name, const std::string &query, const storage::PropertyValue::map_t &user_parameters,
                 const TriggerEventType event_type, utils::SkipList<QueryCacheEntry> *query_cache,
                 DbAccessor *db_accessor, const InterpreterConfig::Query &query_config,
                 std::shared_ptr<QueryUserOrRole> owner)
    : name_{std::move(name)},
      parsed_statements_{ParseQuery(query, user_parameters, query_cache, query_config)},
      event_type_{event_type},
      owner_{std::move(owner)} {
  // We check immediately if the query is valid by trying to create a plan.
  GetPlan(db_accessor);
}

Trigger::TriggerPlan::TriggerPlan(std::unique_ptr<LogicalPlan> logical_plan, std::vector<IdentifierInfo> identifiers)
    : cached_plan(std::move(logical_plan)), identifiers(std::move(identifiers)) {}

std::shared_ptr<Trigger::TriggerPlan> Trigger::GetPlan(DbAccessor *db_accessor) const {
  std::lock_guard plan_guard{plan_lock_};
  if (!parsed_statements_.is_cacheable || !trigger_plan_) {
    auto identifiers = GetPredefinedIdentifiers(event_type_);

    AstStorage ast_storage;
    ast_storage.properties_ = parsed_statements_.ast_storage.properties_;
    ast_storage.labels_ = parsed_statements_.ast_storage.labels_;
    ast_storage.edge_types_ = parsed_statements_.ast_storage.edge_types_;

    std::vector<Identifier *> predefined_identifiers;
    predefined_identifiers.reserve(identifiers.size());
    std::transform(identifiers.begin(), identifiers.end(), std::back_inserter(predefined_identifiers),
                   [](auto &identifier) { return &identifier.first; });

    auto logical_plan = MakeLogicalPlan(std::move(ast_storage), utils::Downcast<CypherQuery>(parsed_statements_.query),
                                        parsed_statements_.parameters, db_accessor, predefined_identifiers);

    trigger_plan_ = std::make_shared<TriggerPlan>(std::move(logical_plan), std::move(identifiers));
  }
  if (!owner_->IsAuthorized(parsed_statements_.required_privileges, "", &up_to_date_policy)) {
    throw utils::BasicException("The owner of trigger '{}' is not authorized to execute the query!", name_);
  }
  return trigger_plan_;
}

void Trigger::Execute(DbAccessor *dba, DatabaseAccessProtector db_acc, utils::MemoryResource *execution_memory,
                      const double max_execution_time_sec, std::atomic<bool> *is_shutting_down,
                      std::atomic<TransactionStatus> *transaction_status, const TriggerContext &context) const {
  if (!context.ShouldEventTrigger(event_type_)) {
    return;
  }

  spdlog::debug("Executing trigger '{}'", name_);
  auto trigger_plan = GetPlan(dba);
  MG_ASSERT(trigger_plan, "Invalid trigger plan received");
  auto &[plan, identifiers] = *trigger_plan;

  ExecutionContext ctx;
  ctx.db_accessor = dba;
  ctx.symbol_table = plan.symbol_table();
  ctx.evaluation_context.timestamp = QueryTimestamp();
  ctx.evaluation_context.parameters = parsed_statements_.parameters;
  ctx.evaluation_context.properties = NamesToProperties(plan.ast_storage().properties_, dba);
  ctx.evaluation_context.labels = NamesToLabels(plan.ast_storage().labels_, dba);
  ctx.timer = (max_execution_time_sec > 0.0) ? std::make_shared<utils::AsyncTimer>(max_execution_time_sec) : nullptr;
  ctx.is_shutting_down = is_shutting_down;
  ctx.transaction_status = transaction_status;
  ctx.is_profile_query = false;
  ctx.evaluation_context.memory = execution_memory;
  ctx.db_acc = std::move(db_acc);

  auto cursor = plan.plan().MakeCursor(execution_memory);
  Frame frame{plan.symbol_table().max_position(), execution_memory};
  for (const auto &[identifier, tag] : identifiers) {
    if (identifier.symbol_pos_ == -1) {
      continue;
    }

    frame[plan.symbol_table().at(identifier)] = context.GetTypedValue(tag, dba);
  }

  while (cursor->Pull(frame, ctx))
    ;

  cursor->Shutdown();
  memgraph::metrics::IncrementCounter(memgraph::metrics::TriggersExecuted);
}

namespace {
// When the format of the persisted trigger is changed, increase this version
inline constexpr uint64_t kVersion{2};
}  // namespace

TriggerStore::TriggerStore(std::filesystem::path directory) : storage_{std::move(directory)} {}

void TriggerStore::RestoreTrigger(utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                                  const InterpreterConfig::Query &query_config, const query::AuthChecker *auth_checker,
                                  std::string_view trigger_name, std::string_view trigger_data) {
  const auto get_failed_message = [&trigger_name = trigger_name](const std::string_view message) {
    return fmt::format("Failed to load trigger '{}'. {}", trigger_name, message);
  };

  const auto invalid_state_message = get_failed_message("Invalid state of the trigger data.");

  spdlog::debug("Loading trigger '{}'", trigger_name);
  auto json_trigger_data = nlohmann::json::parse(trigger_data);

  if (!json_trigger_data["version"].is_number_unsigned()) {
    spdlog::warn(invalid_state_message);
    return;
  }
  if (json_trigger_data["version"] != kVersion) {
    spdlog::warn(get_failed_message("Invalid version of the trigger data."));
    return;
  }

  if (!json_trigger_data["statement"].is_string()) {
    spdlog::warn(invalid_state_message);
    return;
  }
  auto statement = json_trigger_data["statement"].get<std::string>();

  if (!json_trigger_data["phase"].is_number_integer()) {
    spdlog::warn(invalid_state_message);
    return;
  }
  const auto phase = json_trigger_data["phase"].get<TriggerPhase>();

  if (!json_trigger_data["event_type"].is_number_integer()) {
    spdlog::warn(invalid_state_message);
    return;
  }
  const auto event_type = json_trigger_data["event_type"].get<TriggerEventType>();

  if (!json_trigger_data["user_parameters"].is_object()) {
    spdlog::warn(invalid_state_message);
    return;
  }
  const auto user_parameters = serialization::DeserializePropertyValueMap(json_trigger_data["user_parameters"],
                                                                          db_accessor->GetStorageAccessor());

  // TODO: Migration
  const auto owner_json = json_trigger_data["owner"];
  std::optional<std::string> owner{};
  if (owner_json.is_string()) {
    owner.emplace(owner_json.get<std::string>());
  } else if (!owner_json.is_null()) {
    spdlog::warn(invalid_state_message);
    return;
  }

  const auto owner_role_json = json_trigger_data["owner_role"];
  const std::optional<std::string> role{};
  if (owner_role_json.is_string()) {
    owner.emplace(owner_role_json.get<std::string>());
  } else if (!owner_role_json.is_null()) {
    spdlog::warn(invalid_state_message);
    return;
  }

  std::shared_ptr<query::QueryUserOrRole> user = nullptr;
  try {
    user = auth_checker->GenQueryUser(owner, role);
  } catch (const utils::BasicException &e) {
    spdlog::warn(
        fmt::format("Failed to load trigger '{}' because its owner is not an existing Memgraph user.", trigger_name));
    return;
  }

  std::optional<Trigger> trigger;
  try {
    trigger.emplace(std::string{trigger_name}, statement, user_parameters, event_type, query_cache, db_accessor,
                    query_config, std::move(user));
  } catch (const utils::BasicException &e) {
    spdlog::warn("Failed to create trigger '{}' because: {}", trigger_name, e.what());
    return;
  }

  auto triggers_acc =
      phase == TriggerPhase::BEFORE_COMMIT ? before_commit_triggers_.access() : after_commit_triggers_.access();
  triggers_acc.insert(std::move(*trigger));

  spdlog::debug("Trigger loaded successfully!");
}

void TriggerStore::RestoreTriggers(utils::SkipList<QueryCacheEntry> *query_cache, DbAccessor *db_accessor,
                                   const InterpreterConfig::Query &query_config,
                                   const query::AuthChecker *auth_checker) {
  MG_ASSERT(before_commit_triggers_.size() == 0 && after_commit_triggers_.size() == 0,
            "Cannot restore trigger when some triggers already exist!");
  spdlog::info("Loading triggers...");

  for (const auto &[trigger_name, trigger_data] : storage_) {
    RestoreTrigger(query_cache, db_accessor, query_config, auth_checker, trigger_name, trigger_data);
  }
}

void TriggerStore::AddTrigger(std::string name, const std::string &query,
                              const storage::PropertyValue::map_t &user_parameters, TriggerEventType event_type,
                              TriggerPhase phase, utils::SkipList<QueryCacheEntry> *query_cache,
                              DbAccessor *db_accessor, const InterpreterConfig::Query &query_config,
                              std::shared_ptr<QueryUserOrRole> owner) {
  std::unique_lock store_guard{store_lock_};
  if (storage_.Get(name)) {
    throw utils::BasicException("Trigger with the same name already exists.");
  }

  std::optional<Trigger> trigger;
  try {
    trigger.emplace(std::move(name), query, user_parameters, event_type, query_cache, db_accessor, query_config,
                    std::move(owner));
  } catch (const utils::BasicException &e) {
    const auto identifiers = GetPredefinedIdentifiers(event_type);
    std::stringstream identifier_names_stream;
    utils::PrintIterable(identifier_names_stream, identifiers, ", ",
                         [](auto &stream, const auto &identifier) { stream << identifier.first.name_; });

    throw utils::BasicException(
        "Failed creating the trigger.\nError message: '{}'\nThe error was mostly likely generated because of the wrong "
        "statement that this trigger executes.\nMake sure all predefined variables used are present for the specified "
        "event.\nAllowed variables for event '{}' are: {}",
        e.what(), TriggerEventTypeToString(event_type), identifier_names_stream.str());
  }

  // When the format of the persisted trigger is changed, update the kVersion
  nlohmann::json data = nlohmann::json::object();
  data["statement"] = query;
  data["user_parameters"] =
      serialization::SerializePropertyValueMap(user_parameters, db_accessor->GetStorageAccessor());
  data["event_type"] = event_type;
  data["phase"] = phase;
  data["version"] = kVersion;

  if (const auto &owner_from_trigger = trigger->Owner(); owner_from_trigger && *owner_from_trigger) {
    const auto &maybe_username = owner_from_trigger->username();
    if (maybe_username) {
      data["owner"] = *maybe_username;
      // Roles need to be associated with a username
      const auto &maybe_rolename = owner_from_trigger->rolename();
      if (maybe_rolename) {
        data["owner_role"] = *maybe_rolename;
      } else {
        data["owner_role"] = nullptr;
      }
    } else {
      data["owner"] = nullptr;
    }
  } else {
    data["owner"] = nullptr;
    data["owner_role"] = nullptr;
  }
  storage_.Put(trigger->Name(), data.dump());
  store_guard.unlock();

  auto triggers_acc =
      phase == TriggerPhase::BEFORE_COMMIT ? before_commit_triggers_.access() : after_commit_triggers_.access();
  triggers_acc.insert(std::move(*trigger));
}

void TriggerStore::DropTrigger(const std::string &name) {
  std::unique_lock store_guard{store_lock_};
  const auto maybe_trigger_data = storage_.Get(name);
  if (!maybe_trigger_data) {
    throw utils::BasicException("Trigger with name '{}' doesn't exist", name);
  }

  nlohmann::json data;
  try {
    data = nlohmann::json::parse(*maybe_trigger_data);
  } catch (const nlohmann::json::parse_error &e) {
    throw utils::BasicException("Couldn't load trigger data!");
  }

  if (!data.is_object()) {
    throw utils::BasicException("Couldn't load trigger data!");
  }

  if (!data["phase"].is_number_integer()) {
    throw utils::BasicException("Invalid type loaded inside the trigger data!");
  }

  auto triggers_acc =
      data["phase"] == TriggerPhase::BEFORE_COMMIT ? before_commit_triggers_.access() : after_commit_triggers_.access();
  triggers_acc.remove(name);
  storage_.Delete(name);
}

void TriggerStore::DropAll() {
  std::unique_lock store_guard{store_lock_};

  storage_.DeletePrefix("");
  before_commit_triggers_.clear();
  after_commit_triggers_.clear();
}

std::vector<TriggerStore::TriggerInfo> TriggerStore::GetTriggerInfo() const {
  std::vector<TriggerInfo> info;
  info.reserve(before_commit_triggers_.size() + after_commit_triggers_.size());

  const auto add_info = [&](const utils::SkipList<Trigger> &trigger_list, const TriggerPhase phase) {
    for (const auto &trigger : trigger_list.access()) {
      std::optional<std::string> owner_str{};
      if (const auto &owner = trigger.Owner(); owner && *owner) owner_str = owner->username();
      info.push_back({trigger.Name(), trigger.OriginalStatement(), trigger.EventType(), phase, std::move(owner_str)});
    }
  };

  add_info(before_commit_triggers_, TriggerPhase::BEFORE_COMMIT);
  add_info(after_commit_triggers_, TriggerPhase::AFTER_COMMIT);

  return info;
}

std::unordered_set<TriggerEventType> TriggerStore::GetEventTypes() const {
  std::unordered_set<TriggerEventType> event_types;

  const auto add_event_types = [&](const utils::SkipList<Trigger> &trigger_list) {
    for (const auto &trigger : trigger_list.access()) {
      event_types.insert(trigger.EventType());
    }
  };

  add_event_types(before_commit_triggers_);
  add_event_types(after_commit_triggers_);
  return event_types;
}
}  // namespace memgraph::query
