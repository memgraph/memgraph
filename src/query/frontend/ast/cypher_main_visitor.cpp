// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//////////////////////////////////////////////////////
// THIS INCLUDE SHOULD ALWAYS COME BEFORE THE
// "cypher_main_visitor.hpp"
// "module.hpp" includes json.hpp which uses libc's
// EOF macro while "cypher_main_visitor.hpp" includes
// "antlr4-runtime.h" which contains a static variable
// of the same name, EOF.
// This hides the definition of the macro which causes
// the compilation to fail.
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/procedure/module.hpp"
//////////////////////////////////////////////////////
#include "query/frontend/ast/cypher_main_visitor.hpp"

#include <algorithm>
#include <climits>
#include <codecvt>
#include <cstring>
#include <iterator>
#include <limits>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "query/exceptions.hpp"
#include "query/frontend/parsing.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/stream/common.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace query::frontend {

const std::string CypherMainVisitor::kAnonPrefix = "anon";

namespace {
template <typename TVisitor>
std::optional<std::pair<query::Expression *, size_t>> VisitMemoryLimit(
    MemgraphCypher::MemoryLimitContext *memory_limit_ctx, TVisitor *visitor) {
  MG_ASSERT(memory_limit_ctx);
  if (memory_limit_ctx->UNLIMITED()) {
    return std::nullopt;
  }

  auto memory_limit = memory_limit_ctx->literal()->accept(visitor);
  size_t memory_scale = 1024U;
  if (memory_limit_ctx->MB()) {
    memory_scale = 1024U * 1024U;
  } else {
    MG_ASSERT(memory_limit_ctx->KB());
    memory_scale = 1024U;
  }

  return std::make_pair(memory_limit, memory_scale);
}

std::string JoinTokens(const auto &tokens, const auto &string_projection, const auto &separator) {
  std::vector<std::string> tokens_string;
  tokens_string.reserve(tokens.size());
  for (auto *token : tokens) {
    tokens_string.emplace_back(string_projection(token));
  }
  return utils::Join(tokens_string, separator);
}

std::string JoinSymbolicNames(antlr4::tree::ParseTreeVisitor *visitor,
                              const std::vector<MemgraphCypher::SymbolicNameContext *> symbolicNames,
                              const std::string &separator = ".") {
  return JoinTokens(
      symbolicNames, [&](auto *token) { return token->accept(visitor).template as<std::string>(); }, separator);
}

std::string JoinSymbolicNamesWithDotsAndMinus(antlr4::tree::ParseTreeVisitor &visitor,
                                              MemgraphCypher::SymbolicNameWithDotsAndMinusContext &ctx) {
  return JoinTokens(
      ctx.symbolicNameWithMinus(), [&](auto *token) { return JoinSymbolicNames(&visitor, token->symbolicName(), "-"); },
      ".");
}
}  // namespace

antlrcpp::Any CypherMainVisitor::visitExplainQuery(MemgraphCypher::ExplainQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 2, "ExplainQuery should have exactly two children!");
  auto *cypher_query = ctx->children[1]->accept(this).as<CypherQuery *>();
  auto *explain_query = storage_->Create<ExplainQuery>();
  explain_query->cypher_query_ = cypher_query;
  query_ = explain_query;
  return explain_query;
}

antlrcpp::Any CypherMainVisitor::visitProfileQuery(MemgraphCypher::ProfileQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 2, "ProfileQuery should have exactly two children!");
  auto *cypher_query = ctx->children[1]->accept(this).as<CypherQuery *>();
  auto *profile_query = storage_->Create<ProfileQuery>();
  profile_query->cypher_query_ = cypher_query;
  query_ = profile_query;
  return profile_query;
}

antlrcpp::Any CypherMainVisitor::visitInfoQuery(MemgraphCypher::InfoQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 2, "InfoQuery should have exactly two children!");
  auto *info_query = storage_->Create<InfoQuery>();
  query_ = info_query;
  if (ctx->storageInfo()) {
    info_query->info_type_ = InfoQuery::InfoType::STORAGE;
    return info_query;
  } else if (ctx->indexInfo()) {
    info_query->info_type_ = InfoQuery::InfoType::INDEX;
    return info_query;
  } else if (ctx->constraintInfo()) {
    info_query->info_type_ = InfoQuery::InfoType::CONSTRAINT;
    return info_query;
  } else {
    throw utils::NotYetImplemented("Info query: '{}'", ctx->getText());
  }
}

antlrcpp::Any CypherMainVisitor::visitConstraintQuery(MemgraphCypher::ConstraintQueryContext *ctx) {
  auto *constraint_query = storage_->Create<ConstraintQuery>();
  MG_ASSERT(ctx->CREATE() || ctx->DROP());
  if (ctx->CREATE()) {
    constraint_query->action_type_ = ConstraintQuery::ActionType::CREATE;
  } else if (ctx->DROP()) {
    constraint_query->action_type_ = ConstraintQuery::ActionType::DROP;
  }
  constraint_query->constraint_ = ctx->constraint()->accept(this).as<Constraint>();
  query_ = constraint_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitConstraint(MemgraphCypher::ConstraintContext *ctx) {
  Constraint constraint;
  MG_ASSERT(ctx->EXISTS() || ctx->UNIQUE() || (ctx->NODE() && ctx->KEY()));
  if (ctx->EXISTS()) {
    constraint.type = Constraint::Type::EXISTS;
  } else if (ctx->UNIQUE()) {
    constraint.type = Constraint::Type::UNIQUE;
  } else if (ctx->NODE() && ctx->KEY()) {
    constraint.type = Constraint::Type::NODE_KEY;
  }
  constraint.label = AddLabel(ctx->labelName()->accept(this));
  std::string node_name = ctx->nodeName->symbolicName()->accept(this);
  for (const auto &var_ctx : ctx->constraintPropertyList()->variable()) {
    std::string var_name = var_ctx->symbolicName()->accept(this);
    if (var_name != node_name) {
      throw SemanticException("All constraint variable should reference node '{}'", node_name);
    }
  }
  for (const auto &prop_lookup : ctx->constraintPropertyList()->propertyLookup()) {
    constraint.properties.push_back(prop_lookup->propertyKeyName()->accept(this));
  }

  return constraint;
}

antlrcpp::Any CypherMainVisitor::visitCypherQuery(MemgraphCypher::CypherQueryContext *ctx) {
  auto *cypher_query = storage_->Create<CypherQuery>();
  MG_ASSERT(ctx->singleQuery(), "Expected single query.");
  cypher_query->single_query_ = ctx->singleQuery()->accept(this).as<SingleQuery *>();

  // Check that union and union all dont mix
  bool has_union = false;
  bool has_union_all = false;
  for (auto *child : ctx->cypherUnion()) {
    if (child->ALL()) {
      has_union_all = true;
    } else {
      has_union = true;
    }
    if (has_union && has_union_all) {
      throw SemanticException("Invalid combination of UNION and UNION ALL.");
    }
    cypher_query->cypher_unions_.push_back(child->accept(this).as<CypherUnion *>());
  }

  if (auto *memory_limit_ctx = ctx->queryMemoryLimit()) {
    const auto memory_limit_info = VisitMemoryLimit(memory_limit_ctx->memoryLimit(), this);
    if (memory_limit_info) {
      cypher_query->memory_limit_ = memory_limit_info->first;
      cypher_query->memory_scale_ = memory_limit_info->second;
    }
  }

  query_ = cypher_query;
  return cypher_query;
}

antlrcpp::Any CypherMainVisitor::visitIndexQuery(MemgraphCypher::IndexQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "IndexQuery should have exactly one child!");
  auto *index_query = ctx->children[0]->accept(this).as<IndexQuery *>();
  query_ = index_query;
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateIndex(MemgraphCypher::CreateIndexContext *ctx) {
  auto *index_query = storage_->Create<IndexQuery>();
  index_query->action_ = IndexQuery::Action::CREATE;
  index_query->label_ = AddLabel(ctx->labelName()->accept(this));
  if (ctx->propertyKeyName()) {
    PropertyIx name_key = ctx->propertyKeyName()->accept(this);
    index_query->properties_ = {name_key};
  }
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitDropIndex(MemgraphCypher::DropIndexContext *ctx) {
  auto *index_query = storage_->Create<IndexQuery>();
  index_query->action_ = IndexQuery::Action::DROP;
  if (ctx->propertyKeyName()) {
    PropertyIx key = ctx->propertyKeyName()->accept(this);
    index_query->properties_ = {key};
  }
  index_query->label_ = AddLabel(ctx->labelName()->accept(this));
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitAuthQuery(MemgraphCypher::AuthQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "AuthQuery should have exactly one child!");
  auto *auth_query = ctx->children[0]->accept(this).as<AuthQuery *>();
  query_ = auth_query;
  return auth_query;
}

antlrcpp::Any CypherMainVisitor::visitDumpQuery(MemgraphCypher::DumpQueryContext *ctx) {
  auto *dump_query = storage_->Create<DumpQuery>();
  query_ = dump_query;
  return dump_query;
}

antlrcpp::Any CypherMainVisitor::visitReplicationQuery(MemgraphCypher::ReplicationQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "ReplicationQuery should have exactly one child!");
  auto *replication_query = ctx->children[0]->accept(this).as<ReplicationQuery *>();
  query_ = replication_query;
  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::SET_REPLICATION_ROLE;
  if (ctx->MAIN()) {
    if (ctx->WITH() || ctx->PORT()) {
      throw SemanticException("Main can't set a port!");
    }
    replication_query->role_ = ReplicationQuery::ReplicationRole::MAIN;
  } else if (ctx->REPLICA()) {
    replication_query->role_ = ReplicationQuery::ReplicationRole::REPLICA;
    if (ctx->WITH() && ctx->PORT()) {
      if (ctx->port->numberLiteral() && ctx->port->numberLiteral()->integerLiteral()) {
        replication_query->port_ = ctx->port->accept(this);
      } else {
        throw SyntaxException("Port must be an integer literal!");
      }
    }
  }
  return replication_query;
}
antlrcpp::Any CypherMainVisitor::visitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::SHOW_REPLICATION_ROLE;
  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitRegisterReplica(MemgraphCypher::RegisterReplicaContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::REGISTER_REPLICA;
  replication_query->replica_name_ = ctx->replicaName()->symbolicName()->accept(this).as<std::string>();
  if (ctx->SYNC()) {
    replication_query->sync_mode_ = query::ReplicationQuery::SyncMode::SYNC;
    if (ctx->WITH() && ctx->TIMEOUT()) {
      if (ctx->timeout->numberLiteral()) {
        // we accept both double and integer literals
        replication_query->timeout_ = ctx->timeout->accept(this);
      } else {
        throw SemanticException("Timeout should be a integer or double literal!");
      }
    }
  } else if (ctx->ASYNC()) {
    if (ctx->WITH() && ctx->TIMEOUT()) {
      throw SyntaxException("Timeout can be set only for the SYNC replication mode!");
    }
    replication_query->sync_mode_ = query::ReplicationQuery::SyncMode::ASYNC;
  }

  if (!ctx->socketAddress()->literal()->StringLiteral()) {
    throw SemanticException("Socket address should be a string literal!");
  } else {
    replication_query->socket_address_ = ctx->socketAddress()->accept(this);
  }

  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitDropReplica(MemgraphCypher::DropReplicaContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::DROP_REPLICA;
  replication_query->replica_name_ = ctx->replicaName()->symbolicName()->accept(this).as<std::string>();
  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitShowReplicas(MemgraphCypher::ShowReplicasContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::SHOW_REPLICAS;
  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitLockPathQuery(MemgraphCypher::LockPathQueryContext *ctx) {
  auto *lock_query = storage_->Create<LockPathQuery>();
  if (ctx->LOCK()) {
    lock_query->action_ = LockPathQuery::Action::LOCK_PATH;
  } else if (ctx->UNLOCK()) {
    lock_query->action_ = LockPathQuery::Action::UNLOCK_PATH;
  } else {
    throw SyntaxException("Expected LOCK or UNLOCK");
  }

  query_ = lock_query;
  return lock_query;
}

antlrcpp::Any CypherMainVisitor::visitLoadCsv(MemgraphCypher::LoadCsvContext *ctx) {
  query_info_.has_load_csv = true;

  auto *load_csv = storage_->Create<LoadCsv>();
  // handle file name
  if (ctx->csvFile()->literal()->StringLiteral()) {
    load_csv->file_ = ctx->csvFile()->accept(this);
  } else {
    throw SemanticException("CSV file path should be a string literal");
  }

  // handle header options
  // Don't have to check for ctx->HEADER(), as it's a mandatory token.
  // Just need to check if ctx->WITH() is not nullptr - otherwise, we have a
  // ctx->NO() and ctx->HEADER() present.
  load_csv->with_header_ = ctx->WITH() != nullptr;

  // handle skip bad row option
  load_csv->ignore_bad_ = ctx->IGNORE() && ctx->BAD();

  // handle delimiter
  if (ctx->DELIMITER()) {
    if (ctx->delimiter()->literal()->StringLiteral()) {
      load_csv->delimiter_ = ctx->delimiter()->accept(this);
    } else {
      throw SemanticException("Delimiter should be a string literal");
    }
  }

  // handle quote
  if (ctx->QUOTE()) {
    if (ctx->quote()->literal()->StringLiteral()) {
      load_csv->quote_ = ctx->quote()->accept(this);
    } else {
      throw SemanticException("Quote should be a string literal");
    }
  }

  // handle row variable
  load_csv->row_var_ = storage_->Create<Identifier>(ctx->rowVar()->variable()->accept(this).as<std::string>());

  return load_csv;
}

antlrcpp::Any CypherMainVisitor::visitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext *ctx) {
  auto *free_memory_query = storage_->Create<FreeMemoryQuery>();
  query_ = free_memory_query;
  return free_memory_query;
}

antlrcpp::Any CypherMainVisitor::visitTriggerQuery(MemgraphCypher::TriggerQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "TriggerQuery should have exactly one child!");
  auto *trigger_query = ctx->children[0]->accept(this).as<TriggerQuery *>();
  query_ = trigger_query;
  return trigger_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateTrigger(MemgraphCypher::CreateTriggerContext *ctx) {
  auto *trigger_query = storage_->Create<TriggerQuery>();
  trigger_query->action_ = TriggerQuery::Action::CREATE_TRIGGER;
  trigger_query->trigger_name_ = ctx->triggerName()->symbolicName()->accept(this).as<std::string>();

  auto *statement = ctx->triggerStatement();
  antlr4::misc::Interval interval{statement->start->getStartIndex(), statement->stop->getStopIndex()};
  trigger_query->statement_ = ctx->start->getInputStream()->getText(interval);

  trigger_query->event_type_ = [ctx] {
    if (!ctx->ON()) {
      return TriggerQuery::EventType::ANY;
    }

    if (ctx->CREATE(1)) {
      if (ctx->emptyVertex()) {
        return TriggerQuery::EventType::VERTEX_CREATE;
      }
      if (ctx->emptyEdge()) {
        return TriggerQuery::EventType::EDGE_CREATE;
      }
      return TriggerQuery::EventType::CREATE;
    }

    if (ctx->DELETE()) {
      if (ctx->emptyVertex()) {
        return TriggerQuery::EventType::VERTEX_DELETE;
      }
      if (ctx->emptyEdge()) {
        return TriggerQuery::EventType::EDGE_DELETE;
      }
      return TriggerQuery::EventType::DELETE;
    }

    if (ctx->UPDATE()) {
      if (ctx->emptyVertex()) {
        return TriggerQuery::EventType::VERTEX_UPDATE;
      }
      if (ctx->emptyEdge()) {
        return TriggerQuery::EventType::EDGE_UPDATE;
      }
      return TriggerQuery::EventType::UPDATE;
    }

    LOG_FATAL("Invalid token allowed for the query");
  }();

  trigger_query->before_commit_ = ctx->BEFORE();

  return trigger_query;
}

antlrcpp::Any CypherMainVisitor::visitDropTrigger(MemgraphCypher::DropTriggerContext *ctx) {
  auto *trigger_query = storage_->Create<TriggerQuery>();
  trigger_query->action_ = TriggerQuery::Action::DROP_TRIGGER;
  trigger_query->trigger_name_ = ctx->triggerName()->symbolicName()->accept(this).as<std::string>();
  return trigger_query;
}

antlrcpp::Any CypherMainVisitor::visitShowTriggers(MemgraphCypher::ShowTriggersContext *ctx) {
  auto *trigger_query = storage_->Create<TriggerQuery>();
  trigger_query->action_ = TriggerQuery::Action::SHOW_TRIGGERS;
  return trigger_query;
}

antlrcpp::Any CypherMainVisitor::visitIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext *ctx) {
  auto *isolation_level_query = storage_->Create<IsolationLevelQuery>();

  isolation_level_query->isolation_level_scope_ = [scope = ctx->isolationLevelScope()]() {
    if (scope->GLOBAL()) {
      return IsolationLevelQuery::IsolationLevelScope::GLOBAL;
    }
    if (scope->SESSION()) {
      return IsolationLevelQuery::IsolationLevelScope::SESSION;
    }
    return IsolationLevelQuery::IsolationLevelScope::NEXT;
  }();

  isolation_level_query->isolation_level_ = [level = ctx->isolationLevel()]() {
    if (level->SNAPSHOT()) {
      return IsolationLevelQuery::IsolationLevel::SNAPSHOT_ISOLATION;
    }
    if (level->COMMITTED()) {
      return IsolationLevelQuery::IsolationLevel::READ_COMMITTED;
    }
    return IsolationLevelQuery::IsolationLevel::READ_UNCOMMITTED;
  }();

  query_ = isolation_level_query;
  return isolation_level_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext *ctx) {
  query_ = storage_->Create<CreateSnapshotQuery>();
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitStreamQuery(MemgraphCypher::StreamQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "StreamQuery should have exactly one child!");
  auto *stream_query = ctx->children[0]->accept(this).as<StreamQuery *>();
  query_ = stream_query;
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateStream(MemgraphCypher::CreateStreamContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "CreateStreamQuery should have exactly one child!");
  auto *stream_query = ctx->children[0]->accept(this).as<StreamQuery *>();
  query_ = stream_query;
  return stream_query;
}

std::vector<std::string> TopicNamesFromSymbols(
    antlr4::tree::ParseTreeVisitor &visitor,
    const std::vector<MemgraphCypher::SymbolicNameWithDotsAndMinusContext *> &topic_name_symbols) {
  MG_ASSERT(!topic_name_symbols.empty());
  std::vector<std::string> topic_names;
  topic_names.reserve(topic_names.size());
  std::transform(topic_name_symbols.begin(), topic_name_symbols.end(), std::back_inserter(topic_names),
                 [&visitor](auto *topic_name) { return JoinSymbolicNamesWithDotsAndMinus(visitor, *topic_name); });
  return topic_names;
}

template <bool required, typename... ValueTypes>
void MapConfig(auto &memory, const auto &enum_key, auto &destination) {
  const auto key = static_cast<uint8_t>(enum_key);
  if (!memory.contains(key)) {
    if constexpr (required) {
      throw SemanticException("Config {} is required.", ToString(enum_key));
    } else {
      return;
    }
  }

  std::visit(
      [&]<typename T>(T &&value) {
        using ValueType = std::decay_t<T>;
        if constexpr (utils::SameAsAnyOf<ValueType, ValueTypes...>) {
          destination = std::forward<T>(value);
        } else {
          LOG_FATAL("Invalid type mapped");
        }
      },
      std::move(memory[key]));
}

enum class CommonStreamConfigKey : uint8_t { TRANSFORM, BATCH_INTERVAL, BATCH_SIZE, END };

constexpr std::array all_common_stream_config_keys{
    CommonStreamConfigKey::TRANSFORM, CommonStreamConfigKey::BATCH_INTERVAL, CommonStreamConfigKey::BATCH_SIZE};

std::string_view ToString(const CommonStreamConfigKey key) {
  switch (key) {
    case CommonStreamConfigKey::TRANSFORM:
      return "TRANSFORM";
    case CommonStreamConfigKey::BATCH_INTERVAL:
      return "BATCH_INTERVAL";
    case CommonStreamConfigKey::BATCH_SIZE:
      return "BATCH_SIZE";
    case CommonStreamConfigKey::END:
      LOG_FATAL("Invalid config key used");
  }
}

#define CONCAT_HELPER(a, b) a##b
#define CONCAT(a, b) CONCAT_HELPER(a, b)

#define GENERATE_STREAM_CONFIG_KEY_ENUM(stream, first_config, ...)   \
  enum class CONCAT(stream, ConfigKey) : uint8_t {                   \
    first_config = static_cast<uint8_t>(CommonStreamConfigKey::END), \
    __VA_ARGS__                                                      \
  };

GENERATE_STREAM_CONFIG_KEY_ENUM(Kafka, TOPICS, CONSUMER_GROUP, BOOTSTRAP_SERVERS);

constexpr std::array all_kafka_config_keys{KafkaConfigKey::TOPICS, KafkaConfigKey::CONSUMER_GROUP,
                                           KafkaConfigKey::BOOTSTRAP_SERVERS};

std::string_view ToString(const KafkaConfigKey key) {
  switch (key) {
    case KafkaConfigKey::TOPICS:
      return "TOPICS";
    case KafkaConfigKey::CONSUMER_GROUP:
      return "CONSUMER_GROUP";
    case KafkaConfigKey::BOOTSTRAP_SERVERS:
      return "BOOTSTRAP_SERVERS";
  }
}

void MapCommonStreamConfigs(auto &memory, StreamQuery &stream_query) {
  for (const auto key : all_common_stream_config_keys) {
    switch (key) {
      case CommonStreamConfigKey::TRANSFORM:
        MapConfig<true, std::string>(memory, CommonStreamConfigKey::TRANSFORM, stream_query.transform_name_);
        return;
      case CommonStreamConfigKey::BATCH_INTERVAL:
        MapConfig<false, Expression *>(memory, CommonStreamConfigKey::BATCH_INTERVAL, stream_query.batch_interval_);
        return;
      case CommonStreamConfigKey::BATCH_SIZE:
        MapConfig<false, Expression *>(memory, CommonStreamConfigKey::BATCH_SIZE, stream_query.batch_size_);
        return;
      case CommonStreamConfigKey::END:
        LOG_FATAL("Invalid config key used");
    }
  }
}

antlrcpp::Any CypherMainVisitor::visitKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::CREATE_STREAM;
  stream_query->type_ = StreamQuery::Type::KAFKA;
  stream_query->stream_name_ = ctx->streamName()->symbolicName()->accept(this).as<std::string>();

  for (auto *create_config_ctx : ctx->kafkaCreateStreamConfig()) {
    create_config_ctx->accept(this);
  }

  for (const auto key : all_kafka_config_keys) {
    switch (key) {
      case KafkaConfigKey::TOPICS:
        MapConfig<true, std::vector<std::string>>(memory_, KafkaConfigKey::TOPICS, stream_query->topic_names_);
        break;
      case KafkaConfigKey::CONSUMER_GROUP:
        MapConfig<false, std::string>(memory_, KafkaConfigKey::CONSUMER_GROUP, stream_query->consumer_group_);
        break;
      case KafkaConfigKey::BOOTSTRAP_SERVERS:
        MapConfig<false, Expression *>(memory_, KafkaConfigKey::BOOTSTRAP_SERVERS, stream_query->bootstrap_servers_);
        break;
    }
  }

  MapCommonStreamConfigs(memory_, *stream_query);

  return stream_query;
}

void ThrowIfExists(auto &map, const auto &enum_key) {
  const auto key = static_cast<uint8_t>(enum_key);
  if (map.contains(key)) {
    throw SemanticException("{} defined multiple times in the query", ToString(enum_key));
  }
}

antlrcpp::Any CypherMainVisitor::visitKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext *ctx) {
  if (ctx->commonCreateStreamConfig()) {
    return ctx->commonCreateStreamConfig()->accept(this);
  }

  if (ctx->TOPICS()) {
    ThrowIfExists(memory_, KafkaConfigKey::TOPICS);
    auto *topic_names_ctx = ctx->topicNames();
    MG_ASSERT(topic_names_ctx != nullptr);
    const auto topic_key = static_cast<uint8_t>(KafkaConfigKey::TOPICS);
    memory_[topic_key] = TopicNamesFromSymbols(*this, topic_names_ctx->symbolicNameWithDotsAndMinus());
    return {};
  }

  if (ctx->CONSUMER_GROUP()) {
    ThrowIfExists(memory_, KafkaConfigKey::CONSUMER_GROUP);
    const auto consumer_group_key = static_cast<uint8_t>(KafkaConfigKey::CONSUMER_GROUP);
    memory_[consumer_group_key] = JoinSymbolicNamesWithDotsAndMinus(*this, *ctx->consumerGroup);
    return {};
  }

  MG_ASSERT(ctx->BOOTSTRAP_SERVERS());
  ThrowIfExists(memory_, KafkaConfigKey::BOOTSTRAP_SERVERS);
  if (!ctx->bootstrapServers->StringLiteral()) {
    throw SemanticException("Bootstrap servers should be a string!");
  }

  const auto bootstrap_servers_key = static_cast<uint8_t>(KafkaConfigKey::BOOTSTRAP_SERVERS);
  memory_[bootstrap_servers_key] = ctx->bootstrapServers->accept(this).as<Expression *>();
  return {};
}  // namespace query::frontend

GENERATE_STREAM_CONFIG_KEY_ENUM(Pulsar, TOPICS, SERVICE_URL);

constexpr std::array all_pulsar_config_keys{PulsarConfigKey::TOPICS, PulsarConfigKey::SERVICE_URL};

std::string_view ToString(const PulsarConfigKey key) {
  switch (key) {
    case PulsarConfigKey::TOPICS:
      return "TOPICS";
    case PulsarConfigKey::SERVICE_URL:
      return "SERVICE_URL";
  }
}

antlrcpp::Any CypherMainVisitor::visitPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::CREATE_STREAM;
  stream_query->type_ = StreamQuery::Type::PULSAR;
  stream_query->stream_name_ = ctx->streamName()->symbolicName()->accept(this).as<std::string>();

  for (auto *create_config_ctx : ctx->pulsarCreateStreamConfig()) {
    create_config_ctx->accept(this);
  }

  for (const auto key : all_pulsar_config_keys) {
    switch (key) {
      case PulsarConfigKey::TOPICS:
        MapConfig<true, std::vector<std::string>, Expression *>(memory_, PulsarConfigKey::TOPICS,
                                                                stream_query->topic_names_);
        break;
      case PulsarConfigKey::SERVICE_URL:
        MapConfig<true, Expression *>(memory_, PulsarConfigKey::SERVICE_URL, stream_query->service_url_);
        break;
    }
  }

  MapCommonStreamConfigs(memory_, *stream_query);

  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext *ctx) {
  if (ctx->commonCreateStreamConfig()) {
    return ctx->commonCreateStreamConfig()->accept(this);
  }

  if (ctx->TOPICS()) {
    ThrowIfExists(memory_, PulsarConfigKey::TOPICS);
    auto *pulsar_topic_names_ctx = ctx->pulsarTopicNames();
    MG_ASSERT(pulsar_topic_names_ctx != nullptr);
    const auto topics_key = static_cast<uint8_t>(PulsarConfigKey::TOPICS);
    if (auto *topic_names_ctx = pulsar_topic_names_ctx->topicNames()) {
      memory_[topics_key] = TopicNamesFromSymbols(*this, topic_names_ctx->symbolicNameWithDotsAndMinus());
    } else {
      if (!pulsar_topic_names_ctx->literal()->StringLiteral()) {
        throw SemanticException("Topic names should be defined in a string");
      }
      memory_[topics_key] = pulsar_topic_names_ctx->accept(this).as<Expression *>();
    }
    return {};
  }

  MG_ASSERT(ctx->SERVICE_URL());
  ThrowIfExists(memory_, PulsarConfigKey::SERVICE_URL);
  if (!ctx->serviceUrl->StringLiteral()) {
    throw SemanticException("Service url should be a string!");
  }
  const auto service_url_key = static_cast<uint8_t>(PulsarConfigKey::SERVICE_URL);
  memory_[service_url_key] = ctx->serviceUrl->accept(this).as<Expression *>();
  return {};
}

antlrcpp::Any CypherMainVisitor::visitCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext *ctx) {
  if (ctx->TRANSFORM()) {
    ThrowIfExists(memory_, CommonStreamConfigKey::TRANSFORM);
    const auto transform_key = static_cast<uint8_t>(CommonStreamConfigKey::TRANSFORM);
    memory_[transform_key] = JoinSymbolicNames(this, ctx->transformationName->symbolicName());
    return {};
  }

  if (ctx->BATCH_INTERVAL()) {
    ThrowIfExists(memory_, CommonStreamConfigKey::BATCH_INTERVAL);
    if (!ctx->batchInterval->numberLiteral() || !ctx->batchInterval->numberLiteral()->integerLiteral()) {
      throw SemanticException("Batch interval should be an integer literal!");
    }
    const auto batch_interval_key = static_cast<uint8_t>(CommonStreamConfigKey::BATCH_INTERVAL);
    memory_[batch_interval_key] = ctx->batchInterval->accept(this).as<Expression *>();
    return {};
  }

  MG_ASSERT(ctx->BATCH_SIZE());
  ThrowIfExists(memory_, CommonStreamConfigKey::BATCH_SIZE);
  if (!ctx->batchSize->numberLiteral() || !ctx->batchSize->numberLiteral()->integerLiteral()) {
    throw SemanticException("Batch size should be an integer literal!");
  }
  const auto batch_size_key = static_cast<uint8_t>(CommonStreamConfigKey::BATCH_SIZE);
  memory_[batch_size_key] = ctx->batchSize->accept(this).as<Expression *>();
  return {};
}

antlrcpp::Any CypherMainVisitor::visitDropStream(MemgraphCypher::DropStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::DROP_STREAM;
  stream_query->stream_name_ = ctx->streamName()->symbolicName()->accept(this).as<std::string>();
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitStartStream(MemgraphCypher::StartStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::START_STREAM;
  stream_query->stream_name_ = ctx->streamName()->symbolicName()->accept(this).as<std::string>();
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitStartAllStreams(MemgraphCypher::StartAllStreamsContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::START_ALL_STREAMS;
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitStopStream(MemgraphCypher::StopStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::STOP_STREAM;
  stream_query->stream_name_ = ctx->streamName()->symbolicName()->accept(this).as<std::string>();
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitStopAllStreams(MemgraphCypher::StopAllStreamsContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::STOP_ALL_STREAMS;
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitShowStreams(MemgraphCypher::ShowStreamsContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::SHOW_STREAMS;
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitCheckStream(MemgraphCypher::CheckStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::CHECK_STREAM;
  stream_query->stream_name_ = ctx->streamName()->symbolicName()->accept(this).as<std::string>();

  if (ctx->BATCH_LIMIT()) {
    if (!ctx->batchLimit->numberLiteral() || !ctx->batchLimit->numberLiteral()->integerLiteral()) {
      throw SemanticException("Batch limit should be an integer literal!");
    }
    stream_query->batch_limit_ = ctx->batchLimit->accept(this);
  }
  if (ctx->TIMEOUT()) {
    if (!ctx->timeout->numberLiteral() || !ctx->timeout->numberLiteral()->integerLiteral()) {
      throw SemanticException("Timeout should be an integer literal!");
    }
    stream_query->timeout_ = ctx->timeout->accept(this);
  }
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitSettingQuery(MemgraphCypher::SettingQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "SettingQuery should have exactly one child!");
  auto *setting_query = ctx->children[0]->accept(this).as<SettingQuery *>();
  query_ = setting_query;
  return setting_query;
}

antlrcpp::Any CypherMainVisitor::visitSetSetting(MemgraphCypher::SetSettingContext *ctx) {
  auto *setting_query = storage_->Create<SettingQuery>();
  setting_query->action_ = SettingQuery::Action::SET_SETTING;

  if (!ctx->settingName()->literal()->StringLiteral()) {
    throw SemanticException("Setting name should be a string literal");
  }

  if (!ctx->settingValue()->literal()->StringLiteral()) {
    throw SemanticException("Setting value should be a string literal");
  }

  setting_query->setting_name_ = ctx->settingName()->accept(this);
  MG_ASSERT(setting_query->setting_name_);

  setting_query->setting_value_ = ctx->settingValue()->accept(this);
  MG_ASSERT(setting_query->setting_value_);
  return setting_query;
}

antlrcpp::Any CypherMainVisitor::visitShowSetting(MemgraphCypher::ShowSettingContext *ctx) {
  auto *setting_query = storage_->Create<SettingQuery>();
  setting_query->action_ = SettingQuery::Action::SHOW_SETTING;

  if (!ctx->settingName()->literal()->StringLiteral()) {
    throw SemanticException("Setting name should be a string literal");
  }

  setting_query->setting_name_ = ctx->settingName()->accept(this);
  MG_ASSERT(setting_query->setting_name_);

  return setting_query;
}

antlrcpp::Any CypherMainVisitor::visitShowSettings(MemgraphCypher::ShowSettingsContext * /*ctx*/) {
  auto *setting_query = storage_->Create<SettingQuery>();
  setting_query->action_ = SettingQuery::Action::SHOW_ALL_SETTINGS;
  return setting_query;
}

antlrcpp::Any CypherMainVisitor::visitCypherUnion(MemgraphCypher::CypherUnionContext *ctx) {
  bool distinct = !ctx->ALL();
  auto *cypher_union = storage_->Create<CypherUnion>(distinct);
  DMG_ASSERT(ctx->singleQuery(), "Expected single query.");
  cypher_union->single_query_ = ctx->singleQuery()->accept(this).as<SingleQuery *>();
  return cypher_union;
}

antlrcpp::Any CypherMainVisitor::visitSingleQuery(MemgraphCypher::SingleQueryContext *ctx) {
  auto *single_query = storage_->Create<SingleQuery>();
  for (auto *child : ctx->clause()) {
    antlrcpp::Any got = child->accept(this);
    if (got.is<Clause *>()) {
      single_query->clauses_.push_back(got.as<Clause *>());
    } else {
      auto child_clauses = got.as<std::vector<Clause *>>();
      single_query->clauses_.insert(single_query->clauses_.end(), child_clauses.begin(), child_clauses.end());
    }
  }

  // Check if ordering of clauses makes sense.
  //
  // TODO: should we forbid multiple consecutive set clauses? That case is
  // little bit problematic because multiple barriers are needed. Multiple
  // consecutive SET clauses are undefined behaviour in neo4j.
  bool has_update = false;
  bool has_return = false;
  bool has_optional_match = false;
  bool has_call_procedure = false;
  bool calls_write_procedure = false;
  bool has_any_update = false;
  bool has_load_csv = false;

  auto check_write_procedure = [&calls_write_procedure](const std::string_view clause) {
    if (calls_write_procedure) {
      throw SemanticException(
          "{} can't be put after calling a writeable procedure, only RETURN clause can be put after.", clause);
    }
  };

  for (Clause *clause : single_query->clauses_) {
    const auto &clause_type = clause->GetTypeInfo();
    if (const auto *call_procedure = utils::Downcast<CallProcedure>(clause); call_procedure != nullptr) {
      if (has_return) {
        throw SemanticException("CALL can't be put after RETURN clause.");
      }
      check_write_procedure("CALL");
      has_call_procedure = true;
      if (call_procedure->is_write_) {
        calls_write_procedure = true;
        has_update = true;
      }
    } else if (utils::IsSubtype(clause_type, Unwind::kType)) {
      check_write_procedure("UNWIND");
      if (has_update || has_return) {
        throw SemanticException("UNWIND can't be put after RETURN clause or after an update.");
      }
    } else if (utils::IsSubtype(clause_type, LoadCsv::kType)) {
      if (has_load_csv) {
        throw SemanticException("Can't have multiple LOAD CSV clauses in a single query.");
      }
      check_write_procedure("LOAD CSV");
      if (has_return) {
        throw SemanticException("LOAD CSV can't be put after RETURN clause.");
      }
      has_load_csv = true;
    } else if (auto *match = utils::Downcast<Match>(clause)) {
      if (has_update || has_return) {
        throw SemanticException("MATCH can't be put after RETURN clause or after an update.");
      }
      if (match->optional_) {
        has_optional_match = true;
      } else if (has_optional_match) {
        throw SemanticException("MATCH can't be put after OPTIONAL MATCH.");
      }
      check_write_procedure("MATCH");
    } else if (utils::IsSubtype(clause_type, Create::kType) || utils::IsSubtype(clause_type, Delete::kType) ||
               utils::IsSubtype(clause_type, SetProperty::kType) ||
               utils::IsSubtype(clause_type, SetProperties::kType) || utils::IsSubtype(clause_type, SetLabels::kType) ||
               utils::IsSubtype(clause_type, RemoveProperty::kType) ||
               utils::IsSubtype(clause_type, RemoveLabels::kType) || utils::IsSubtype(clause_type, Merge::kType)) {
      if (has_return) {
        throw SemanticException("Update clause can't be used after RETURN.");
      }
      check_write_procedure("Update clause");
      has_update = true;
      has_any_update = true;
    } else if (utils::IsSubtype(clause_type, Return::kType)) {
      if (has_return) {
        throw SemanticException("There can only be one RETURN in a clause.");
      }
      has_return = true;
    } else if (utils::IsSubtype(clause_type, With::kType)) {
      if (has_return) {
        throw SemanticException("RETURN can't be put before WITH.");
      }
      check_write_procedure("WITH");
      has_update = has_return = has_optional_match = false;
    } else {
      DLOG_FATAL("Can't happen");
    }
  }
  bool is_standalone_call_procedure = has_call_procedure && single_query->clauses_.size() == 1U;
  if (!has_update && !has_return && !is_standalone_call_procedure) {
    throw SemanticException("Query should either create or update something, or return results!");
  }

  if (has_any_update && calls_write_procedure) {
    throw SemanticException("Write procedures cannot be used in queries that contains any update clauses!");
  }
  // Construct unique names for anonymous identifiers;
  int id = 1;
  for (auto **identifier : anonymous_identifiers) {
    while (true) {
      std::string id_name = kAnonPrefix + std::to_string(id++);
      if (users_identifiers.find(id_name) == users_identifiers.end()) {
        *identifier = storage_->Create<Identifier>(id_name, false);
        break;
      }
    }
  }
  return single_query;
}

antlrcpp::Any CypherMainVisitor::visitClause(MemgraphCypher::ClauseContext *ctx) {
  if (ctx->cypherReturn()) {
    return static_cast<Clause *>(ctx->cypherReturn()->accept(this).as<Return *>());
  }
  if (ctx->cypherMatch()) {
    return static_cast<Clause *>(ctx->cypherMatch()->accept(this).as<Match *>());
  }
  if (ctx->create()) {
    return static_cast<Clause *>(ctx->create()->accept(this).as<Create *>());
  }
  if (ctx->cypherDelete()) {
    return static_cast<Clause *>(ctx->cypherDelete()->accept(this).as<Delete *>());
  }
  if (ctx->set()) {
    // Different return type!!!
    return ctx->set()->accept(this).as<std::vector<Clause *>>();
  }
  if (ctx->remove()) {
    // Different return type!!!
    return ctx->remove()->accept(this).as<std::vector<Clause *>>();
  }
  if (ctx->with()) {
    return static_cast<Clause *>(ctx->with()->accept(this).as<With *>());
  }
  if (ctx->merge()) {
    return static_cast<Clause *>(ctx->merge()->accept(this).as<Merge *>());
  }
  if (ctx->unwind()) {
    return static_cast<Clause *>(ctx->unwind()->accept(this).as<Unwind *>());
  }
  if (ctx->callProcedure()) {
    return static_cast<Clause *>(ctx->callProcedure()->accept(this).as<CallProcedure *>());
  }
  if (ctx->loadCsv()) {
    return static_cast<Clause *>(ctx->loadCsv()->accept(this).as<LoadCsv *>());
  }
  // TODO: implement other clauses.
  throw utils::NotYetImplemented("clause '{}'", ctx->getText());
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitCypherMatch(MemgraphCypher::CypherMatchContext *ctx) {
  auto *match = storage_->Create<Match>();
  match->optional_ = !!ctx->OPTIONAL();
  if (ctx->where()) {
    match->where_ = ctx->where()->accept(this);
  }
  match->patterns_ = ctx->pattern()->accept(this).as<std::vector<Pattern *>>();
  return match;
}

antlrcpp::Any CypherMainVisitor::visitCreate(MemgraphCypher::CreateContext *ctx) {
  auto *create = storage_->Create<Create>();
  create->patterns_ = ctx->pattern()->accept(this).as<std::vector<Pattern *>>();
  return create;
}

antlrcpp::Any CypherMainVisitor::visitCallProcedure(MemgraphCypher::CallProcedureContext *ctx) {
  // Don't cache queries which call procedures because the
  // procedure definition can affect the behaviour of the visitor and
  // the execution of the query.
  // If a user recompiles and reloads the procedure with different result
  // names, because of the cache, old result names will be expected while the
  // procedure will return results mapped to new names.
  query_info_.is_cacheable = false;

  auto *call_proc = storage_->Create<CallProcedure>();
  MG_ASSERT(!ctx->procedureName()->symbolicName().empty());
  call_proc->procedure_name_ = JoinSymbolicNames(this, ctx->procedureName()->symbolicName());
  call_proc->arguments_.reserve(ctx->expression().size());
  for (auto *expr : ctx->expression()) {
    call_proc->arguments_.push_back(expr->accept(this));
  }

  if (auto *memory_limit_ctx = ctx->procedureMemoryLimit()) {
    const auto memory_limit_info = VisitMemoryLimit(memory_limit_ctx->memoryLimit(), this);
    if (memory_limit_info) {
      call_proc->memory_limit_ = memory_limit_info->first;
      call_proc->memory_scale_ = memory_limit_info->second;
    }
  } else {
    // Default to 100 MB
    call_proc->memory_limit_ = storage_->Create<PrimitiveLiteral>(TypedValue(100));
    call_proc->memory_scale_ = 1024U * 1024U;
  }

  const auto &maybe_found =
      procedure::FindProcedure(procedure::gModuleRegistry, call_proc->procedure_name_, utils::NewDeleteResource());
  if (!maybe_found) {
    throw SemanticException("There is no procedure named '{}'.", call_proc->procedure_name_);
  }
  call_proc->is_write_ = maybe_found->second->is_write_procedure;

  auto *yield_ctx = ctx->yieldProcedureResults();
  if (!yield_ctx) {
    if (!maybe_found->second->results.empty()) {
      throw SemanticException(
          "CALL without YIELD may only be used on procedures which do not "
          "return any result fields.");
    }
    // When we return, we will release the lock on modules. This means that
    // someone may reload the procedure and change the result signature. But to
    // keep the implementation simple, we ignore the case as the rest of the
    // code doesn't really care whether we yield or not, so it should not break.
    return call_proc;
  }
  if (yield_ctx->getTokens(MemgraphCypher::ASTERISK).empty()) {
    call_proc->result_fields_.reserve(yield_ctx->procedureResult().size());
    call_proc->result_identifiers_.reserve(yield_ctx->procedureResult().size());
    for (auto *result : yield_ctx->procedureResult()) {
      MG_ASSERT(result->variable().size() == 1 || result->variable().size() == 2);
      call_proc->result_fields_.push_back(result->variable()[0]->accept(this).as<std::string>());
      std::string result_alias;
      if (result->variable().size() == 2) {
        result_alias = result->variable()[1]->accept(this).as<std::string>();
      } else {
        result_alias = result->variable()[0]->accept(this).as<std::string>();
      }
      call_proc->result_identifiers_.push_back(storage_->Create<Identifier>(result_alias));
    }
  } else {
    const auto &maybe_found =
        procedure::FindProcedure(procedure::gModuleRegistry, call_proc->procedure_name_, utils::NewDeleteResource());
    if (!maybe_found) {
      throw SemanticException("There is no procedure named '{}'.", call_proc->procedure_name_);
    }
    const auto &[module, proc] = *maybe_found;
    call_proc->result_fields_.reserve(proc->results.size());
    call_proc->result_identifiers_.reserve(proc->results.size());
    for (const auto &[result_name, desc] : proc->results) {
      bool is_deprecated = desc.second;
      if (is_deprecated) continue;
      call_proc->result_fields_.emplace_back(result_name);
      call_proc->result_identifiers_.push_back(storage_->Create<Identifier>(std::string(result_name)));
    }
    // When we leave the scope, we will release the lock on modules. This means
    // that someone may reload the procedure and change its result signature. We
    // are fine with this, because if new result fields were added then we yield
    // the subset of those and that will appear to a user as if they used the
    // procedure before reload. Any subsequent `CALL ... YIELD *` will fetch the
    // new fields as well. In case the result signature has had some result
    // fields removed, then the query execution will report an error that we are
    // yielding missing fields. The user can then just retry the query.
  }

  return call_proc;
}

/**
 * @return std::string
 */
antlrcpp::Any CypherMainVisitor::visitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *ctx) {
  return ctx->symbolicName()->accept(this).as<std::string>();
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitCreateRole(MemgraphCypher::CreateRoleContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::CREATE_ROLE;
  auth->role_ = ctx->role->accept(this).as<std::string>();
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitDropRole(MemgraphCypher::DropRoleContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DROP_ROLE;
  auth->role_ = ctx->role->accept(this).as<std::string>();
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowRoles(MemgraphCypher::ShowRolesContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_ROLES;
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitCreateUser(MemgraphCypher::CreateUserContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::CREATE_USER;
  auth->user_ = ctx->user->accept(this).as<std::string>();
  if (ctx->password) {
    if (!ctx->password->StringLiteral() && !ctx->literal()->CYPHERNULL()) {
      throw SyntaxException("Password should be a string literal or null.");
    }
    auth->password_ = ctx->password->accept(this);
  }
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitSetPassword(MemgraphCypher::SetPasswordContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SET_PASSWORD;
  auth->user_ = ctx->user->accept(this).as<std::string>();
  if (!ctx->password->StringLiteral() && !ctx->literal()->CYPHERNULL()) {
    throw SyntaxException("Password should be a string literal or null.");
  }
  auth->password_ = ctx->password->accept(this);
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitDropUser(MemgraphCypher::DropUserContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DROP_USER;
  auth->user_ = ctx->user->accept(this).as<std::string>();
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowUsers(MemgraphCypher::ShowUsersContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_USERS;
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitSetRole(MemgraphCypher::SetRoleContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SET_ROLE;
  auth->user_ = ctx->user->accept(this).as<std::string>();
  auth->role_ = ctx->role->accept(this).as<std::string>();
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitClearRole(MemgraphCypher::ClearRoleContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::CLEAR_ROLE;
  auth->user_ = ctx->user->accept(this).as<std::string>();
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::GRANT_PRIVILEGE;
  auth->user_or_role_ = ctx->userOrRole->accept(this).as<std::string>();
  if (ctx->privilegeList()) {
    for (auto *privilege : ctx->privilegeList()->privilege()) {
      auth->privileges_.push_back(privilege->accept(this));
    }
  } else {
    /* grant all privileges */
    auth->privileges_ = kPrivilegesAll;
  }
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitDenyPrivilege(MemgraphCypher::DenyPrivilegeContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DENY_PRIVILEGE;
  auth->user_or_role_ = ctx->userOrRole->accept(this).as<std::string>();
  if (ctx->privilegeList()) {
    for (auto *privilege : ctx->privilegeList()->privilege()) {
      auth->privileges_.push_back(privilege->accept(this));
    }
  } else {
    /* deny all privileges */
    auth->privileges_ = kPrivilegesAll;
  }
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::REVOKE_PRIVILEGE;
  auth->user_or_role_ = ctx->userOrRole->accept(this).as<std::string>();
  if (ctx->privilegeList()) {
    for (auto *privilege : ctx->privilegeList()->privilege()) {
      auth->privileges_.push_back(privilege->accept(this));
    }
  } else {
    /* revoke all privileges */
    auth->privileges_ = kPrivilegesAll;
  }
  return auth;
}

/**
 * @return AuthQuery::Privilege
 */
antlrcpp::Any CypherMainVisitor::visitPrivilege(MemgraphCypher::PrivilegeContext *ctx) {
  if (ctx->CREATE()) return AuthQuery::Privilege::CREATE;
  if (ctx->DELETE()) return AuthQuery::Privilege::DELETE;
  if (ctx->MATCH()) return AuthQuery::Privilege::MATCH;
  if (ctx->MERGE()) return AuthQuery::Privilege::MERGE;
  if (ctx->SET()) return AuthQuery::Privilege::SET;
  if (ctx->REMOVE()) return AuthQuery::Privilege::REMOVE;
  if (ctx->INDEX()) return AuthQuery::Privilege::INDEX;
  if (ctx->STATS()) return AuthQuery::Privilege::STATS;
  if (ctx->AUTH()) return AuthQuery::Privilege::AUTH;
  if (ctx->CONSTRAINT()) return AuthQuery::Privilege::CONSTRAINT;
  if (ctx->DUMP()) return AuthQuery::Privilege::DUMP;
  if (ctx->REPLICATION()) return AuthQuery::Privilege::REPLICATION;
  if (ctx->READ_FILE()) return AuthQuery::Privilege::READ_FILE;
  if (ctx->FREE_MEMORY()) return AuthQuery::Privilege::FREE_MEMORY;
  if (ctx->TRIGGER()) return AuthQuery::Privilege::TRIGGER;
  if (ctx->CONFIG()) return AuthQuery::Privilege::CONFIG;
  if (ctx->DURABILITY()) return AuthQuery::Privilege::DURABILITY;
  if (ctx->STREAM()) return AuthQuery::Privilege::STREAM;
  LOG_FATAL("Should not get here - unknown privilege!");
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowPrivileges(MemgraphCypher::ShowPrivilegesContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_PRIVILEGES;
  auth->user_or_role_ = ctx->userOrRole->accept(this).as<std::string>();
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_ROLE_FOR_USER;
  auth->user_ = ctx->user->accept(this).as<std::string>();
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *ctx) {
  AuthQuery *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_USERS_FOR_ROLE;
  auth->role_ = ctx->role->accept(this).as<std::string>();
  return auth;
}

antlrcpp::Any CypherMainVisitor::visitCypherReturn(MemgraphCypher::CypherReturnContext *ctx) {
  auto *return_clause = storage_->Create<Return>();
  return_clause->body_ = ctx->returnBody()->accept(this);
  if (ctx->DISTINCT()) {
    return_clause->body_.distinct = true;
  }
  return return_clause;
}

antlrcpp::Any CypherMainVisitor::visitReturnBody(MemgraphCypher::ReturnBodyContext *ctx) {
  ReturnBody body;
  if (ctx->order()) {
    body.order_by = ctx->order()->accept(this).as<std::vector<SortItem>>();
  }
  if (ctx->skip()) {
    body.skip = static_cast<Expression *>(ctx->skip()->accept(this));
  }
  if (ctx->limit()) {
    body.limit = static_cast<Expression *>(ctx->limit()->accept(this));
  }
  std::tie(body.all_identifiers, body.named_expressions) =
      ctx->returnItems()->accept(this).as<std::pair<bool, std::vector<NamedExpression *>>>();
  return body;
}

antlrcpp::Any CypherMainVisitor::visitReturnItems(MemgraphCypher::ReturnItemsContext *ctx) {
  std::vector<NamedExpression *> named_expressions;
  for (auto *item : ctx->returnItem()) {
    named_expressions.push_back(item->accept(this));
  }
  return std::pair<bool, std::vector<NamedExpression *>>(ctx->getTokens(MemgraphCypher::ASTERISK).size(),
                                                         named_expressions);
}

antlrcpp::Any CypherMainVisitor::visitReturnItem(MemgraphCypher::ReturnItemContext *ctx) {
  auto *named_expr = storage_->Create<NamedExpression>();
  named_expr->expression_ = ctx->expression()->accept(this);
  MG_ASSERT(named_expr->expression_);
  if (ctx->variable()) {
    named_expr->name_ = std::string(ctx->variable()->accept(this).as<std::string>());
    users_identifiers.insert(named_expr->name_);
  } else {
    if (in_with_ && !utils::IsSubtype(*named_expr->expression_, Identifier::kType)) {
      throw SemanticException("Only variables can be non-aliased in WITH.");
    }
    named_expr->name_ = std::string(ctx->getText());
    named_expr->token_position_ = ctx->expression()->getStart()->getTokenIndex();
  }
  return named_expr;
}

antlrcpp::Any CypherMainVisitor::visitOrder(MemgraphCypher::OrderContext *ctx) {
  std::vector<SortItem> order_by;
  for (auto *sort_item : ctx->sortItem()) {
    order_by.push_back(sort_item->accept(this));
  }
  return order_by;
}

antlrcpp::Any CypherMainVisitor::visitSortItem(MemgraphCypher::SortItemContext *ctx) {
  return SortItem{ctx->DESC() || ctx->DESCENDING() ? Ordering::DESC : Ordering::ASC, ctx->expression()->accept(this)};
}

antlrcpp::Any CypherMainVisitor::visitNodePattern(MemgraphCypher::NodePatternContext *ctx) {
  auto *node = storage_->Create<NodeAtom>();
  if (ctx->variable()) {
    std::string variable = ctx->variable()->accept(this);
    node->identifier_ = storage_->Create<Identifier>(variable);
    users_identifiers.insert(variable);
  } else {
    anonymous_identifiers.push_back(&node->identifier_);
  }
  if (ctx->nodeLabels()) {
    node->labels_ = ctx->nodeLabels()->accept(this).as<std::vector<LabelIx>>();
  }
  if (ctx->properties()) {
    // This can return either properties or parameters
    if (ctx->properties()->mapLiteral()) {
      node->properties_ = ctx->properties()->accept(this).as<std::unordered_map<PropertyIx, Expression *>>();
    } else {
      node->properties_ = ctx->properties()->accept(this).as<ParameterLookup *>();
    }
  }
  return node;
}

antlrcpp::Any CypherMainVisitor::visitNodeLabels(MemgraphCypher::NodeLabelsContext *ctx) {
  std::vector<LabelIx> labels;
  for (auto *node_label : ctx->nodeLabel()) {
    labels.push_back(AddLabel(node_label->accept(this)));
  }
  return labels;
}

antlrcpp::Any CypherMainVisitor::visitProperties(MemgraphCypher::PropertiesContext *ctx) {
  if (ctx->mapLiteral()) {
    return ctx->mapLiteral()->accept(this);
  }
  // If child is not mapLiteral that means child is params.
  MG_ASSERT(ctx->parameter());
  return ctx->parameter()->accept(this);
}

antlrcpp::Any CypherMainVisitor::visitMapLiteral(MemgraphCypher::MapLiteralContext *ctx) {
  std::unordered_map<PropertyIx, Expression *> map;
  for (int i = 0; i < static_cast<int>(ctx->propertyKeyName().size()); ++i) {
    PropertyIx key = ctx->propertyKeyName()[i]->accept(this);
    Expression *value = ctx->expression()[i]->accept(this);
    if (!map.insert({key, value}).second) {
      throw SemanticException("Same key can't appear twice in a map literal.");
    }
  }
  return map;
}

antlrcpp::Any CypherMainVisitor::visitListLiteral(MemgraphCypher::ListLiteralContext *ctx) {
  std::vector<Expression *> expressions;
  for (auto expr_ctx_ptr : ctx->expression()) expressions.push_back(expr_ctx_ptr->accept(this));
  return expressions;
}

antlrcpp::Any CypherMainVisitor::visitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *ctx) {
  return AddProperty(visitChildren(ctx));
}

antlrcpp::Any CypherMainVisitor::visitSymbolicName(MemgraphCypher::SymbolicNameContext *ctx) {
  if (ctx->EscapedSymbolicName()) {
    auto quoted_name = ctx->getText();
    DMG_ASSERT(quoted_name.size() >= 2U && quoted_name[0] == '`' && quoted_name.back() == '`',
               "Can't happen. Grammar ensures this");
    // Remove enclosing backticks.
    std::string escaped_name = quoted_name.substr(1, static_cast<int>(quoted_name.size()) - 2);
    // Unescape remaining backticks.
    std::string name;
    bool escaped = false;
    for (auto c : escaped_name) {
      if (escaped) {
        if (c == '`') {
          name.push_back('`');
          escaped = false;
        } else {
          DLOG_FATAL("Can't happen. Grammar ensures that.");
        }
      } else if (c == '`') {
        escaped = true;
      } else {
        name.push_back(c);
      }
    }
    return name;
  }
  if (ctx->UnescapedSymbolicName()) {
    return std::string(ctx->getText());
  }
  return ctx->getText();
}

antlrcpp::Any CypherMainVisitor::visitPattern(MemgraphCypher::PatternContext *ctx) {
  std::vector<Pattern *> patterns;
  for (auto *pattern_part : ctx->patternPart()) {
    patterns.push_back(pattern_part->accept(this));
  }
  return patterns;
}

antlrcpp::Any CypherMainVisitor::visitPatternPart(MemgraphCypher::PatternPartContext *ctx) {
  Pattern *pattern = ctx->anonymousPatternPart()->accept(this);
  if (ctx->variable()) {
    std::string variable = ctx->variable()->accept(this);
    pattern->identifier_ = storage_->Create<Identifier>(variable);
    users_identifiers.insert(variable);
  } else {
    anonymous_identifiers.push_back(&pattern->identifier_);
  }
  return pattern;
}

antlrcpp::Any CypherMainVisitor::visitPatternElement(MemgraphCypher::PatternElementContext *ctx) {
  if (ctx->patternElement()) {
    return ctx->patternElement()->accept(this);
  }
  auto pattern = storage_->Create<Pattern>();
  pattern->atoms_.push_back(ctx->nodePattern()->accept(this).as<NodeAtom *>());
  for (auto *pattern_element_chain : ctx->patternElementChain()) {
    std::pair<PatternAtom *, PatternAtom *> element = pattern_element_chain->accept(this);
    pattern->atoms_.push_back(element.first);
    pattern->atoms_.push_back(element.second);
  }
  return pattern;
}

antlrcpp::Any CypherMainVisitor::visitPatternElementChain(MemgraphCypher::PatternElementChainContext *ctx) {
  return std::pair<PatternAtom *, PatternAtom *>(ctx->relationshipPattern()->accept(this).as<EdgeAtom *>(),
                                                 ctx->nodePattern()->accept(this).as<NodeAtom *>());
}

antlrcpp::Any CypherMainVisitor::visitRelationshipPattern(MemgraphCypher::RelationshipPatternContext *ctx) {
  auto *edge = storage_->Create<EdgeAtom>();

  auto relationshipDetail = ctx->relationshipDetail();
  auto *variableExpansion = relationshipDetail ? relationshipDetail->variableExpansion() : nullptr;
  edge->type_ = EdgeAtom::Type::SINGLE;
  if (variableExpansion)
    std::tie(edge->type_, edge->lower_bound_, edge->upper_bound_) =
        variableExpansion->accept(this).as<std::tuple<EdgeAtom::Type, Expression *, Expression *>>();

  if (ctx->leftArrowHead() && !ctx->rightArrowHead()) {
    edge->direction_ = EdgeAtom::Direction::IN;
  } else if (!ctx->leftArrowHead() && ctx->rightArrowHead()) {
    edge->direction_ = EdgeAtom::Direction::OUT;
  } else {
    // <-[]-> and -[]- is the same thing as far as we understand openCypher
    // grammar.
    edge->direction_ = EdgeAtom::Direction::BOTH;
  }

  if (!relationshipDetail) {
    anonymous_identifiers.push_back(&edge->identifier_);
    return edge;
  }

  if (relationshipDetail->name) {
    std::string variable = relationshipDetail->name->accept(this);
    edge->identifier_ = storage_->Create<Identifier>(variable);
    users_identifiers.insert(variable);
  } else {
    anonymous_identifiers.push_back(&edge->identifier_);
  }

  if (relationshipDetail->relationshipTypes()) {
    edge->edge_types_ = ctx->relationshipDetail()->relationshipTypes()->accept(this).as<std::vector<EdgeTypeIx>>();
  }

  auto relationshipLambdas = relationshipDetail->relationshipLambda();
  if (variableExpansion) {
    if (relationshipDetail->total_weight && edge->type_ != EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
      throw SemanticException(
          "Variable for total weight is allowed only with weighted shortest "
          "path expansion.");
    auto visit_lambda = [this](auto *lambda) {
      EdgeAtom::Lambda edge_lambda;
      std::string traversed_edge_variable = lambda->traversed_edge->accept(this);
      edge_lambda.inner_edge = storage_->Create<Identifier>(traversed_edge_variable);
      std::string traversed_node_variable = lambda->traversed_node->accept(this);
      edge_lambda.inner_node = storage_->Create<Identifier>(traversed_node_variable);
      edge_lambda.expression = lambda->expression()->accept(this);
      return edge_lambda;
    };
    auto visit_total_weight = [&]() {
      if (relationshipDetail->total_weight) {
        std::string total_weight_name = relationshipDetail->total_weight->accept(this);
        edge->total_weight_ = storage_->Create<Identifier>(total_weight_name);
      } else {
        anonymous_identifiers.push_back(&edge->total_weight_);
      }
    };
    switch (relationshipLambdas.size()) {
      case 0:
        if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
          throw SemanticException(
              "Lambda for calculating weights is mandatory with weighted "
              "shortest path expansion.");
        // In variable expansion inner variables are mandatory.
        anonymous_identifiers.push_back(&edge->filter_lambda_.inner_edge);
        anonymous_identifiers.push_back(&edge->filter_lambda_.inner_node);
        break;
      case 1:
        if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH) {
          // For wShortest, the first (and required) lambda is used for weight
          // calculation.
          edge->weight_lambda_ = visit_lambda(relationshipLambdas[0]);
          visit_total_weight();
          // Add mandatory inner variables for filter lambda.
          anonymous_identifiers.push_back(&edge->filter_lambda_.inner_edge);
          anonymous_identifiers.push_back(&edge->filter_lambda_.inner_node);
        } else {
          // Other variable expands only have the filter lambda.
          edge->filter_lambda_ = visit_lambda(relationshipLambdas[0]);
        }
        break;
      case 2:
        if (edge->type_ != EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
          throw SemanticException("Only one filter lambda can be supplied.");
        edge->weight_lambda_ = visit_lambda(relationshipLambdas[0]);
        visit_total_weight();
        edge->filter_lambda_ = visit_lambda(relationshipLambdas[1]);
        break;
      default:
        throw SemanticException("Only one filter lambda can be supplied.");
    }
  } else if (!relationshipLambdas.empty()) {
    throw SemanticException("Filter lambda is only allowed in variable length expansion.");
  }

  auto properties = relationshipDetail->properties();
  switch (properties.size()) {
    case 0:
      break;
    case 1: {
      if (properties[0]->mapLiteral()) {
        edge->properties_ = properties[0]->accept(this).as<std::unordered_map<PropertyIx, Expression *>>();
        break;
      }
      MG_ASSERT(properties[0]->parameter());
      edge->properties_ = properties[0]->accept(this).as<ParameterLookup *>();
      break;
    }
    default:
      throw SemanticException("Only one property map can be supplied for edge.");
  }

  return edge;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipDetail(MemgraphCypher::RelationshipDetailContext *) {
  DLOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipLambda(MemgraphCypher::RelationshipLambdaContext *) {
  DLOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipTypes(MemgraphCypher::RelationshipTypesContext *ctx) {
  std::vector<EdgeTypeIx> types;
  for (auto *edge_type : ctx->relTypeName()) {
    types.push_back(AddEdgeType(edge_type->accept(this)));
  }
  return types;
}

antlrcpp::Any CypherMainVisitor::visitVariableExpansion(MemgraphCypher::VariableExpansionContext *ctx) {
  DMG_ASSERT(ctx->expression().size() <= 2U, "Expected 0, 1 or 2 bounds in range literal.");

  EdgeAtom::Type edge_type = EdgeAtom::Type::DEPTH_FIRST;
  if (!ctx->getTokens(MemgraphCypher::BFS).empty())
    edge_type = EdgeAtom::Type::BREADTH_FIRST;
  else if (!ctx->getTokens(MemgraphCypher::WSHORTEST).empty())
    edge_type = EdgeAtom::Type::WEIGHTED_SHORTEST_PATH;
  Expression *lower = nullptr;
  Expression *upper = nullptr;

  if (ctx->expression().size() == 0U) {
    // Case -[*]-
  } else if (ctx->expression().size() == 1U) {
    auto dots_tokens = ctx->getTokens(MemgraphCypher::DOTS);
    Expression *bound = ctx->expression()[0]->accept(this);
    if (!dots_tokens.size()) {
      // Case -[*bound]-
      if (edge_type != EdgeAtom::Type::WEIGHTED_SHORTEST_PATH) lower = bound;
      upper = bound;
    } else if (dots_tokens[0]->getSourceInterval().startsAfter(ctx->expression()[0]->getSourceInterval())) {
      // Case -[*bound..]-
      lower = bound;
    } else {
      // Case -[*..bound]-
      upper = bound;
    }
  } else {
    // Case -[*lbound..rbound]-
    lower = ctx->expression()[0]->accept(this);
    upper = ctx->expression()[1]->accept(this);
  }
  if (lower && edge_type == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
    throw SemanticException("Lower bound is not allowed in weighted shortest path expansion.");

  return std::make_tuple(edge_type, lower, upper);
}

antlrcpp::Any CypherMainVisitor::visitExpression(MemgraphCypher::ExpressionContext *ctx) {
  return static_cast<Expression *>(ctx->expression12()->accept(this));
}

// OR.
antlrcpp::Any CypherMainVisitor::visitExpression12(MemgraphCypher::Expression12Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression11(), ctx->children, {MemgraphCypher::OR});
}

// XOR.
antlrcpp::Any CypherMainVisitor::visitExpression11(MemgraphCypher::Expression11Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression10(), ctx->children, {MemgraphCypher::XOR});
}

// AND.
antlrcpp::Any CypherMainVisitor::visitExpression10(MemgraphCypher::Expression10Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression9(), ctx->children, {MemgraphCypher::AND});
}

// NOT.
antlrcpp::Any CypherMainVisitor::visitExpression9(MemgraphCypher::Expression9Context *ctx) {
  return PrefixUnaryOperator(ctx->expression8(), ctx->children, {MemgraphCypher::NOT});
}

// Comparisons.
// Expresion 1 < 2 < 3 is converted to 1 < 2 && 2 < 3 and then binary operator
// ast node is constructed for each operator.
antlrcpp::Any CypherMainVisitor::visitExpression8(MemgraphCypher::Expression8Context *ctx) {
  if (!ctx->partialComparisonExpression().size()) {
    // There is no comparison operators. We generate expression7.
    return ctx->expression7()->accept(this);
  }

  // There is at least one comparison. We need to generate code for each of
  // them. We don't call visitPartialComparisonExpression but do everything in
  // this function and call expression7-s directly. Since every expression7
  // can be generated twice (because it can appear in two comparisons) code
  // generated by whole subtree of expression7 must not have any sideeffects.
  // We handle chained comparisons as defined by mathematics, neo4j handles
  // them in a very interesting, illogical and incomprehensible way. For
  // example in neo4j:
  //  1 < 2 < 3 -> true,
  //  1 < 2 < 3 < 4 -> false,
  //  5 > 3 < 5 > 3 -> true,
  //  4 <= 5 < 7 > 6 -> false
  //  All of those comparisons evaluate to true in memgraph.
  std::vector<Expression *> children;
  children.push_back(ctx->expression7()->accept(this));
  std::vector<size_t> operators;
  auto partial_comparison_expressions = ctx->partialComparisonExpression();
  for (auto *child : partial_comparison_expressions) {
    children.push_back(child->expression7()->accept(this));
  }
  // First production is comparison operator.
  for (auto *child : partial_comparison_expressions) {
    operators.push_back(static_cast<antlr4::tree::TerminalNode *>(child->children[0])->getSymbol()->getType());
  }

  // Make all comparisons.
  Expression *first_operand = children[0];
  std::vector<Expression *> comparisons;
  for (int i = 0; i < (int)operators.size(); ++i) {
    auto *expr = children[i + 1];
    // TODO: first_operand should only do lookup if it is only calculated and
    // not recalculated whole subexpression once again. SymbolGenerator should
    // generate symbol for every expresion and then lookup would be possible.
    comparisons.push_back(CreateBinaryOperatorByToken(operators[i], first_operand, expr));
    first_operand = expr;
  }

  first_operand = comparisons[0];
  // Calculate logical and of results of comparisons.
  for (int i = 1; i < (int)comparisons.size(); ++i) {
    first_operand = storage_->Create<AndOperator>(first_operand, comparisons[i]);
  }
  return first_operand;
}

antlrcpp::Any CypherMainVisitor::visitPartialComparisonExpression(
    MemgraphCypher::PartialComparisonExpressionContext *) {
  DLOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

// Addition and subtraction.
antlrcpp::Any CypherMainVisitor::visitExpression7(MemgraphCypher::Expression7Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression6(), ctx->children,
                                           {MemgraphCypher::PLUS, MemgraphCypher::MINUS});
}

// Multiplication, division, modding.
antlrcpp::Any CypherMainVisitor::visitExpression6(MemgraphCypher::Expression6Context *ctx) {
  return LeftAssociativeOperatorExpression(ctx->expression5(), ctx->children,
                                           {MemgraphCypher::ASTERISK, MemgraphCypher::SLASH, MemgraphCypher::PERCENT});
}

// Power.
antlrcpp::Any CypherMainVisitor::visitExpression5(MemgraphCypher::Expression5Context *ctx) {
  if (ctx->expression4().size() > 1U) {
    // TODO: implement power operator. In neo4j power is left associative and
    // int^int -> float.
    throw utils::NotYetImplemented("power (^) operator");
  }
  return visitChildren(ctx);
}

// Unary minus and plus.
antlrcpp::Any CypherMainVisitor::visitExpression4(MemgraphCypher::Expression4Context *ctx) {
  return PrefixUnaryOperator(ctx->expression3a(), ctx->children, {MemgraphCypher::PLUS, MemgraphCypher::MINUS});
}

// IS NULL, IS NOT NULL, STARTS WITH, ..
antlrcpp::Any CypherMainVisitor::visitExpression3a(MemgraphCypher::Expression3aContext *ctx) {
  Expression *expression = ctx->expression3b()->accept(this);

  for (auto *op : ctx->stringAndNullOperators()) {
    if (op->IS() && op->NOT() && op->CYPHERNULL()) {
      expression =
          static_cast<Expression *>(storage_->Create<NotOperator>(storage_->Create<IsNullOperator>(expression)));
    } else if (op->IS() && op->CYPHERNULL()) {
      expression = static_cast<Expression *>(storage_->Create<IsNullOperator>(expression));
    } else if (op->IN()) {
      expression =
          static_cast<Expression *>(storage_->Create<InListOperator>(expression, op->expression3b()->accept(this)));
    } else if (utils::StartsWith(op->getText(), "=~")) {
      auto *regex_match = storage_->Create<RegexMatch>();
      regex_match->string_expr_ = expression;
      regex_match->regex_ = op->expression3b()->accept(this);
      expression = regex_match;
    } else {
      std::string function_name;
      if (op->STARTS() && op->WITH()) {
        function_name = kStartsWith;
      } else if (op->ENDS() && op->WITH()) {
        function_name = kEndsWith;
      } else if (op->CONTAINS()) {
        function_name = kContains;
      } else {
        throw utils::NotYetImplemented("function '{}'", op->getText());
      }
      auto expression2 = op->expression3b()->accept(this);
      std::vector<Expression *> args = {expression, expression2};
      expression = static_cast<Expression *>(storage_->Create<Function>(function_name, args));
    }
  }
  return expression;
}
antlrcpp::Any CypherMainVisitor::visitStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext *) {
  DLOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitExpression3b(MemgraphCypher::Expression3bContext *ctx) {
  Expression *expression = ctx->expression2a()->accept(this);
  for (auto *list_op : ctx->listIndexingOrSlicing()) {
    if (list_op->getTokens(MemgraphCypher::DOTS).size() == 0U) {
      // If there is no '..' then we need to create list indexing operator.
      expression = storage_->Create<SubscriptOperator>(expression, list_op->expression()[0]->accept(this));
    } else if (!list_op->lower_bound && !list_op->upper_bound) {
      throw SemanticException("List slicing operator requires at least one bound.");
    } else {
      Expression *lower_bound_ast =
          list_op->lower_bound ? static_cast<Expression *>(list_op->lower_bound->accept(this)) : nullptr;
      Expression *upper_bound_ast =
          list_op->upper_bound ? static_cast<Expression *>(list_op->upper_bound->accept(this)) : nullptr;
      expression = storage_->Create<ListSlicingOperator>(expression, lower_bound_ast, upper_bound_ast);
    }
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext *) {
  DLOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitExpression2a(MemgraphCypher::Expression2aContext *ctx) {
  Expression *expression = ctx->expression2b()->accept(this);
  if (ctx->nodeLabels()) {
    auto labels = ctx->nodeLabels()->accept(this).as<std::vector<LabelIx>>();
    expression = storage_->Create<LabelsTest>(expression, labels);
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitExpression2b(MemgraphCypher::Expression2bContext *ctx) {
  Expression *expression = ctx->atom()->accept(this);
  for (auto *lookup : ctx->propertyLookup()) {
    PropertyIx key = lookup->accept(this);
    auto property_lookup = storage_->Create<PropertyLookup>(expression, key);
    expression = property_lookup;
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitAtom(MemgraphCypher::AtomContext *ctx) {
  if (ctx->literal()) {
    return ctx->literal()->accept(this);
  } else if (ctx->parameter()) {
    return static_cast<Expression *>(ctx->parameter()->accept(this).as<ParameterLookup *>());
  } else if (ctx->parenthesizedExpression()) {
    return static_cast<Expression *>(ctx->parenthesizedExpression()->accept(this));
  } else if (ctx->variable()) {
    std::string variable = ctx->variable()->accept(this);
    users_identifiers.insert(variable);
    return static_cast<Expression *>(storage_->Create<Identifier>(variable));
  } else if (ctx->functionInvocation()) {
    return static_cast<Expression *>(ctx->functionInvocation()->accept(this));
  } else if (ctx->COALESCE()) {
    std::vector<Expression *> exprs;
    for (auto *expr_context : ctx->expression()) {
      exprs.emplace_back(expr_context->accept(this).as<Expression *>());
    }
    return static_cast<Expression *>(storage_->Create<Coalesce>(std::move(exprs)));
  } else if (ctx->COUNT()) {
    // Here we handle COUNT(*). COUNT(expression) is handled in
    // visitFunctionInvocation with other aggregations. This is visible in
    // functionInvocation and atom producions in opencypher grammar.
    return static_cast<Expression *>(storage_->Create<Aggregation>(nullptr, nullptr, Aggregation::Op::COUNT));
  } else if (ctx->ALL()) {
    auto *ident =
        storage_->Create<Identifier>(ctx->filterExpression()->idInColl()->variable()->accept(this).as<std::string>());
    Expression *list_expr = ctx->filterExpression()->idInColl()->expression()->accept(this);
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("ALL(...) requires a WHERE predicate.");
    }
    Where *where = ctx->filterExpression()->where()->accept(this);
    return static_cast<Expression *>(storage_->Create<All>(ident, list_expr, where));
  } else if (ctx->SINGLE()) {
    auto *ident =
        storage_->Create<Identifier>(ctx->filterExpression()->idInColl()->variable()->accept(this).as<std::string>());
    Expression *list_expr = ctx->filterExpression()->idInColl()->expression()->accept(this);
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("SINGLE(...) requires a WHERE predicate.");
    }
    Where *where = ctx->filterExpression()->where()->accept(this);
    return static_cast<Expression *>(storage_->Create<Single>(ident, list_expr, where));
  } else if (ctx->ANY()) {
    auto *ident =
        storage_->Create<Identifier>(ctx->filterExpression()->idInColl()->variable()->accept(this).as<std::string>());
    Expression *list_expr = ctx->filterExpression()->idInColl()->expression()->accept(this);
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("ANY(...) requires a WHERE predicate.");
    }
    Where *where = ctx->filterExpression()->where()->accept(this);
    return static_cast<Expression *>(storage_->Create<Any>(ident, list_expr, where));
  } else if (ctx->NONE()) {
    auto *ident =
        storage_->Create<Identifier>(ctx->filterExpression()->idInColl()->variable()->accept(this).as<std::string>());
    Expression *list_expr = ctx->filterExpression()->idInColl()->expression()->accept(this);
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("NONE(...) requires a WHERE predicate.");
    }
    Where *where = ctx->filterExpression()->where()->accept(this);
    return static_cast<Expression *>(storage_->Create<None>(ident, list_expr, where));
  } else if (ctx->REDUCE()) {
    auto *accumulator =
        storage_->Create<Identifier>(ctx->reduceExpression()->accumulator->accept(this).as<std::string>());
    Expression *initializer = ctx->reduceExpression()->initial->accept(this);
    auto *ident =
        storage_->Create<Identifier>(ctx->reduceExpression()->idInColl()->variable()->accept(this).as<std::string>());
    Expression *list = ctx->reduceExpression()->idInColl()->expression()->accept(this);
    Expression *expr = ctx->reduceExpression()->expression().back()->accept(this);
    return static_cast<Expression *>(storage_->Create<Reduce>(accumulator, initializer, ident, list, expr));
  } else if (ctx->caseExpression()) {
    return static_cast<Expression *>(ctx->caseExpression()->accept(this));
  } else if (ctx->extractExpression()) {
    auto *ident =
        storage_->Create<Identifier>(ctx->extractExpression()->idInColl()->variable()->accept(this).as<std::string>());
    Expression *list = ctx->extractExpression()->idInColl()->expression()->accept(this);
    Expression *expr = ctx->extractExpression()->expression()->accept(this);
    return static_cast<Expression *>(storage_->Create<Extract>(ident, list, expr));
  }
  // TODO: Implement this. We don't support comprehensions, filtering... at
  // the moment.
  throw utils::NotYetImplemented("atom expression '{}'", ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitParameter(MemgraphCypher::ParameterContext *ctx) {
  return storage_->Create<ParameterLookup>(ctx->getStart()->getTokenIndex());
}

antlrcpp::Any CypherMainVisitor::visitLiteral(MemgraphCypher::LiteralContext *ctx) {
  if (ctx->CYPHERNULL() || ctx->StringLiteral() || ctx->booleanLiteral() || ctx->numberLiteral()) {
    int token_position = ctx->getStart()->getTokenIndex();
    if (ctx->CYPHERNULL()) {
      return static_cast<Expression *>(storage_->Create<PrimitiveLiteral>(TypedValue(), token_position));
    } else if (context_.is_query_cached) {
      // Instead of generating PrimitiveLiteral, we generate a
      // ParameterLookup, so that the AST can be cached. This allows for
      // varying literals, which are then looked up in the parameters table
      // (even though they are not user provided). Note, that NULL always
      // generates a PrimitiveLiteral.
      return static_cast<Expression *>(storage_->Create<ParameterLookup>(token_position));
    } else if (ctx->StringLiteral()) {
      return static_cast<Expression *>(storage_->Create<PrimitiveLiteral>(
          visitStringLiteral(ctx->StringLiteral()->getText()).as<std::string>(), token_position));
    } else if (ctx->booleanLiteral()) {
      return static_cast<Expression *>(
          storage_->Create<PrimitiveLiteral>(ctx->booleanLiteral()->accept(this).as<bool>(), token_position));
    } else if (ctx->numberLiteral()) {
      return static_cast<Expression *>(
          storage_->Create<PrimitiveLiteral>(ctx->numberLiteral()->accept(this).as<TypedValue>(), token_position));
    }
    LOG_FATAL("Expected to handle all cases above");
  } else if (ctx->listLiteral()) {
    return static_cast<Expression *>(
        storage_->Create<ListLiteral>(ctx->listLiteral()->accept(this).as<std::vector<Expression *>>()));
  } else {
    return static_cast<Expression *>(storage_->Create<MapLiteral>(
        ctx->mapLiteral()->accept(this).as<std::unordered_map<PropertyIx, Expression *>>()));
  }
  return visitChildren(ctx);
}

antlrcpp::Any CypherMainVisitor::visitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *ctx) {
  return static_cast<Expression *>(ctx->expression()->accept(this));
}

antlrcpp::Any CypherMainVisitor::visitNumberLiteral(MemgraphCypher::NumberLiteralContext *ctx) {
  if (ctx->integerLiteral()) {
    return TypedValue(ctx->integerLiteral()->accept(this).as<int64_t>());
  } else if (ctx->doubleLiteral()) {
    return TypedValue(ctx->doubleLiteral()->accept(this).as<double>());
  } else {
    // This should never happen, except grammar changes and we don't notice
    // change in this production.
    DLOG_FATAL("can't happen");
    throw std::exception();
  }
}

antlrcpp::Any CypherMainVisitor::visitFunctionInvocation(MemgraphCypher::FunctionInvocationContext *ctx) {
  if (ctx->DISTINCT()) {
    throw utils::NotYetImplemented("DISTINCT function call");
  }
  std::string function_name = ctx->functionName()->accept(this);
  std::vector<Expression *> expressions;
  for (auto *expression : ctx->expression()) {
    expressions.push_back(expression->accept(this));
  }
  if (expressions.size() == 1U) {
    if (function_name == Aggregation::kCount) {
      return static_cast<Expression *>(storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::COUNT));
    }
    if (function_name == Aggregation::kMin) {
      return static_cast<Expression *>(storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::MIN));
    }
    if (function_name == Aggregation::kMax) {
      return static_cast<Expression *>(storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::MAX));
    }
    if (function_name == Aggregation::kSum) {
      return static_cast<Expression *>(storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::SUM));
    }
    if (function_name == Aggregation::kAvg) {
      return static_cast<Expression *>(storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::AVG));
    }
    if (function_name == Aggregation::kCollect) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::COLLECT_LIST));
    }
  }

  if (expressions.size() == 2U && function_name == Aggregation::kCollect) {
    return static_cast<Expression *>(
        storage_->Create<Aggregation>(expressions[1], expressions[0], Aggregation::Op::COLLECT_MAP));
  }

  auto function = NameToFunction(function_name);
  if (!function) throw SemanticException("Function '{}' doesn't exist.", function_name);
  return static_cast<Expression *>(storage_->Create<Function>(function_name, expressions));
}

antlrcpp::Any CypherMainVisitor::visitFunctionName(MemgraphCypher::FunctionNameContext *ctx) {
  return utils::ToUpperCase(ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitDoubleLiteral(MemgraphCypher::DoubleLiteralContext *ctx) {
  return ParseDoubleLiteral(ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitIntegerLiteral(MemgraphCypher::IntegerLiteralContext *ctx) {
  return ParseIntegerLiteral(ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitStringLiteral(const std::string &escaped) { return ParseStringLiteral(escaped); }

antlrcpp::Any CypherMainVisitor::visitBooleanLiteral(MemgraphCypher::BooleanLiteralContext *ctx) {
  if (ctx->getTokens(MemgraphCypher::TRUE).size()) {
    return true;
  }
  if (ctx->getTokens(MemgraphCypher::FALSE).size()) {
    return false;
  }
  DLOG_FATAL("Shouldn't happend");
  throw std::exception();
}

antlrcpp::Any CypherMainVisitor::visitCypherDelete(MemgraphCypher::CypherDeleteContext *ctx) {
  auto *del = storage_->Create<Delete>();
  if (ctx->DETACH()) {
    del->detach_ = true;
  }
  for (auto *expression : ctx->expression()) {
    del->expressions_.push_back(expression->accept(this));
  }
  return del;
}

antlrcpp::Any CypherMainVisitor::visitWhere(MemgraphCypher::WhereContext *ctx) {
  auto *where = storage_->Create<Where>();
  where->expression_ = ctx->expression()->accept(this);
  return where;
}

antlrcpp::Any CypherMainVisitor::visitSet(MemgraphCypher::SetContext *ctx) {
  std::vector<Clause *> set_items;
  for (auto *set_item : ctx->setItem()) {
    set_items.push_back(set_item->accept(this));
  }
  return set_items;
}

antlrcpp::Any CypherMainVisitor::visitSetItem(MemgraphCypher::SetItemContext *ctx) {
  // SetProperty
  if (ctx->propertyExpression()) {
    auto *set_property = storage_->Create<SetProperty>();
    set_property->property_lookup_ = ctx->propertyExpression()->accept(this);
    set_property->expression_ = ctx->expression()->accept(this);
    return static_cast<Clause *>(set_property);
  }

  // SetProperties either assignment or update
  if (ctx->getTokens(MemgraphCypher::EQ).size() || ctx->getTokens(MemgraphCypher::PLUS_EQ).size()) {
    auto *set_properties = storage_->Create<SetProperties>();
    set_properties->identifier_ = storage_->Create<Identifier>(ctx->variable()->accept(this).as<std::string>());
    set_properties->expression_ = ctx->expression()->accept(this);
    if (ctx->getTokens(MemgraphCypher::PLUS_EQ).size()) {
      set_properties->update_ = true;
    }
    return static_cast<Clause *>(set_properties);
  }

  // SetLabels
  auto *set_labels = storage_->Create<SetLabels>();
  set_labels->identifier_ = storage_->Create<Identifier>(ctx->variable()->accept(this).as<std::string>());
  set_labels->labels_ = ctx->nodeLabels()->accept(this).as<std::vector<LabelIx>>();
  return static_cast<Clause *>(set_labels);
}

antlrcpp::Any CypherMainVisitor::visitRemove(MemgraphCypher::RemoveContext *ctx) {
  std::vector<Clause *> remove_items;
  for (auto *remove_item : ctx->removeItem()) {
    remove_items.push_back(remove_item->accept(this));
  }
  return remove_items;
}

antlrcpp::Any CypherMainVisitor::visitRemoveItem(MemgraphCypher::RemoveItemContext *ctx) {
  // RemoveProperty
  if (ctx->propertyExpression()) {
    auto *remove_property = storage_->Create<RemoveProperty>();
    remove_property->property_lookup_ = ctx->propertyExpression()->accept(this);
    return static_cast<Clause *>(remove_property);
  }

  // RemoveLabels
  auto *remove_labels = storage_->Create<RemoveLabels>();
  remove_labels->identifier_ = storage_->Create<Identifier>(ctx->variable()->accept(this).as<std::string>());
  remove_labels->labels_ = ctx->nodeLabels()->accept(this).as<std::vector<LabelIx>>();
  return static_cast<Clause *>(remove_labels);
}

antlrcpp::Any CypherMainVisitor::visitPropertyExpression(MemgraphCypher::PropertyExpressionContext *ctx) {
  Expression *expression = ctx->atom()->accept(this);
  for (auto *lookup : ctx->propertyLookup()) {
    PropertyIx key = lookup->accept(this);
    auto property_lookup = storage_->Create<PropertyLookup>(expression, key);
    expression = property_lookup;
  }
  // It is guaranteed by grammar that there is at least one propertyLookup.
  return static_cast<PropertyLookup *>(expression);
}

antlrcpp::Any CypherMainVisitor::visitCaseExpression(MemgraphCypher::CaseExpressionContext *ctx) {
  Expression *test_expression = ctx->test ? ctx->test->accept(this).as<Expression *>() : nullptr;
  auto alternatives = ctx->caseAlternatives();
  // Reverse alternatives so that tree of IfOperators can be built bottom-up.
  std::reverse(alternatives.begin(), alternatives.end());
  Expression *else_expression = ctx->else_expression ? ctx->else_expression->accept(this).as<Expression *>()
                                                     : storage_->Create<PrimitiveLiteral>(TypedValue());
  for (auto *alternative : alternatives) {
    Expression *condition =
        test_expression ? storage_->Create<EqualOperator>(test_expression, alternative->when_expression->accept(this))
                        : alternative->when_expression->accept(this).as<Expression *>();
    Expression *then_expression = alternative->then_expression->accept(this);
    else_expression = storage_->Create<IfOperator>(condition, then_expression, else_expression);
  }
  return else_expression;
}

antlrcpp::Any CypherMainVisitor::visitCaseAlternatives(MemgraphCypher::CaseAlternativesContext *) {
  DLOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitWith(MemgraphCypher::WithContext *ctx) {
  auto *with = storage_->Create<With>();
  in_with_ = true;
  with->body_ = ctx->returnBody()->accept(this);
  in_with_ = false;
  if (ctx->DISTINCT()) {
    with->body_.distinct = true;
  }
  if (ctx->where()) {
    with->where_ = ctx->where()->accept(this);
  }
  return with;
}

antlrcpp::Any CypherMainVisitor::visitMerge(MemgraphCypher::MergeContext *ctx) {
  auto *merge = storage_->Create<Merge>();
  merge->pattern_ = ctx->patternPart()->accept(this);
  for (auto &merge_action : ctx->mergeAction()) {
    auto set = merge_action->set()->accept(this).as<std::vector<Clause *>>();
    if (merge_action->MATCH()) {
      merge->on_match_.insert(merge->on_match_.end(), set.begin(), set.end());
    } else {
      DMG_ASSERT(merge_action->CREATE(), "Expected ON MATCH or ON CREATE");
      merge->on_create_.insert(merge->on_create_.end(), set.begin(), set.end());
    }
  }
  return merge;
}

antlrcpp::Any CypherMainVisitor::visitUnwind(MemgraphCypher::UnwindContext *ctx) {
  auto *named_expr = storage_->Create<NamedExpression>();
  named_expr->expression_ = ctx->expression()->accept(this);
  named_expr->name_ = std::string(ctx->variable()->accept(this).as<std::string>());
  return storage_->Create<Unwind>(named_expr);
}

antlrcpp::Any CypherMainVisitor::visitFilterExpression(MemgraphCypher::FilterExpressionContext *) {
  LOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

LabelIx CypherMainVisitor::AddLabel(const std::string &name) { return storage_->GetLabelIx(name); }

PropertyIx CypherMainVisitor::AddProperty(const std::string &name) { return storage_->GetPropertyIx(name); }

EdgeTypeIx CypherMainVisitor::AddEdgeType(const std::string &name) { return storage_->GetEdgeTypeIx(name); }

}  // namespace query::frontend
