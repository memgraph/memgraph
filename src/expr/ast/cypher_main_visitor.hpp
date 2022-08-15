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

#pragma once

#include <vector>

#include <antlr4-runtime.h>

#include <algorithm>
#include <climits>
#include <codecvt>
#include <cstring>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <boost/preprocessor/cat.hpp>

#include "expr/ast.hpp"
#include "expr/ast/ast_visitor.hpp"
#include "expr/exceptions.hpp"
#include "expr/parsing.hpp"
#include "parser/opencypher/generated/MemgraphCypher.h"
#include "parser/opencypher/generated/MemgraphCypherBaseVisitor.h"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/typeinfo.hpp"

constexpr char kStartsWith[] = "STARTSWITH";
constexpr char kEndsWith[] = "ENDSWITH";
constexpr char kContains[] = "CONTAINS";
constexpr char kId[] = "ID";

namespace MG_INJECTED_NAMESPACE_NAME {
namespace detail {
using antlropencypher::MemgraphCypher;

template <typename TVisitor>
std::optional<std::pair<Expression *, size_t>> VisitMemoryLimit(MemgraphCypher::MemoryLimitContext *memory_limit_ctx,
                                                                TVisitor *visitor) {
  MG_ASSERT(memory_limit_ctx);
  if (memory_limit_ctx->UNLIMITED()) {
    return std::nullopt;
  }

  auto *memory_limit = std::any_cast<Expression *>(memory_limit_ctx->literal()->accept(visitor));
  size_t memory_scale = 1024UL;
  if (memory_limit_ctx->MB()) {
    memory_scale = 1024UL * 1024UL;
  } else {
    MG_ASSERT(memory_limit_ctx->KB());
    memory_scale = 1024UL;
  }

  return std::make_pair(memory_limit, memory_scale);
}

inline std::string JoinTokens(const auto &tokens, const auto &string_projection, const auto &separator) {
  std::vector<std::string> tokens_string;
  tokens_string.reserve(tokens.size());
  for (auto *token : tokens) {
    tokens_string.emplace_back(string_projection(token));
  }
  return utils::Join(tokens_string, separator);
}

inline std::string JoinSymbolicNames(antlr4::tree::ParseTreeVisitor *visitor,
                                     const std::vector<MemgraphCypher::SymbolicNameContext *> symbolicNames,
                                     const std::string &separator = ".") {
  return JoinTokens(
      symbolicNames, [&](auto *token) { return std::any_cast<std::string>(token->accept(visitor)); }, separator);
}

inline std::string JoinSymbolicNamesWithDotsAndMinus(antlr4::tree::ParseTreeVisitor &visitor,
                                                     MemgraphCypher::SymbolicNameWithDotsAndMinusContext &ctx) {
  return JoinTokens(
      ctx.symbolicNameWithMinus(), [&](auto *token) { return JoinSymbolicNames(&visitor, token->symbolicName(), "-"); },
      ".");
}

inline std::vector<std::string> TopicNamesFromSymbols(
    antlr4::tree::ParseTreeVisitor &visitor,
    const std::vector<MemgraphCypher::SymbolicNameWithDotsAndMinusContext *> &topic_name_symbols) {
  MG_ASSERT(!topic_name_symbols.empty());
  std::vector<std::string> topic_names;
  topic_names.reserve(topic_name_symbols.size());
  std::transform(topic_name_symbols.begin(), topic_name_symbols.end(), std::back_inserter(topic_names),
                 [&visitor](auto *topic_name) { return JoinSymbolicNamesWithDotsAndMinus(visitor, *topic_name); });
  return topic_names;
}

template <typename T>
concept EnumUint8 = std::is_enum_v<T> && std::same_as<uint8_t, std::underlying_type_t<T>>;

template <bool required, typename... ValueTypes>
void MapConfig(auto &memory, const EnumUint8 auto &enum_key, auto &destination) {
  const auto key = static_cast<uint8_t>(enum_key);
  if (!memory.contains(key)) {
    if constexpr (required) {
      throw memgraph::expr::SemanticException("Config {} is required.", ToString(enum_key));
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
  memory.erase(key);
}

enum class CommonStreamConfigKey : uint8_t { TRANSFORM, BATCH_INTERVAL, BATCH_SIZE, END };

inline std::string_view ToString(const CommonStreamConfigKey key) {
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

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_STREAM_CONFIG_KEY_ENUM(stream, first_config, ...)   \
  enum class BOOST_PP_CAT(stream, ConfigKey) : uint8_t {             \
    first_config = static_cast<uint8_t>(CommonStreamConfigKey::END), \
    __VA_ARGS__                                                      \
  };

GENERATE_STREAM_CONFIG_KEY_ENUM(Kafka, TOPICS, CONSUMER_GROUP, BOOTSTRAP_SERVERS, CONFIGS, CREDENTIALS);

inline std::string_view ToString(const KafkaConfigKey key) {
  switch (key) {
    case KafkaConfigKey::TOPICS:
      return "TOPICS";
    case KafkaConfigKey::CONSUMER_GROUP:
      return "CONSUMER_GROUP";
    case KafkaConfigKey::BOOTSTRAP_SERVERS:
      return "BOOTSTRAP_SERVERS";
    case KafkaConfigKey::CONFIGS:
      return "CONFIGS";
    case KafkaConfigKey::CREDENTIALS:
      return "CREDENTIALS";
  }
}

inline void MapCommonStreamConfigs(auto &memory, StreamQuery &stream_query) {
  MapConfig<true, std::string>(memory, CommonStreamConfigKey::TRANSFORM, stream_query.transform_name_);
  MapConfig<false, Expression *>(memory, CommonStreamConfigKey::BATCH_INTERVAL, stream_query.batch_interval_);
  MapConfig<false, Expression *>(memory, CommonStreamConfigKey::BATCH_SIZE, stream_query.batch_size_);
}

inline void ThrowIfExists(const auto &map, const EnumUint8 auto &enum_key) {
  const auto key = static_cast<uint8_t>(enum_key);
  if (map.contains(key)) {
    throw memgraph::expr::SemanticException("{} defined multiple times in the query", ToString(enum_key));
  }
}

inline void GetTopicNames(auto &destination, MemgraphCypher::TopicNamesContext *topic_names_ctx,
                          antlr4::tree::ParseTreeVisitor &visitor) {
  MG_ASSERT(topic_names_ctx != nullptr);
  if (auto *symbolic_topic_names_ctx = topic_names_ctx->symbolicTopicNames()) {
    destination = TopicNamesFromSymbols(visitor, symbolic_topic_names_ctx->symbolicNameWithDotsAndMinus());
  } else {
    if (!topic_names_ctx->literal()->StringLiteral()) {
      throw memgraph::expr::SemanticException("Topic names should be defined as a string literal or as symbolic names");
    }
    destination = std::any_cast<Expression *>(topic_names_ctx->accept(&visitor));
  }
}

GENERATE_STREAM_CONFIG_KEY_ENUM(Pulsar, TOPICS, SERVICE_URL);

inline std::string_view ToString(const PulsarConfigKey key) {
  switch (key) {
    case PulsarConfigKey::TOPICS:
      return "TOPICS";
    case PulsarConfigKey::SERVICE_URL:
      return "SERVICE_URL";
  }
}
}  // namespace detail

using antlropencypher::MemgraphCypher;

struct ParsingContext {
  bool is_query_cached = false;
};

class CypherMainVisitor : public antlropencypher::MemgraphCypherBaseVisitor {
 public:
  explicit CypherMainVisitor(ParsingContext context, AstStorage *storage) : context_(context), storage_(storage) {}

 private:
  Expression *CreateBinaryOperatorByToken(size_t token, Expression *e1, Expression *e2) {
    switch (token) {
      case MemgraphCypher::OR:
        return storage_->Create<OrOperator>(e1, e2);
      case MemgraphCypher::XOR:
        return storage_->Create<XorOperator>(e1, e2);
      case MemgraphCypher::AND:
        return storage_->Create<AndOperator>(e1, e2);
      case MemgraphCypher::PLUS:
        return storage_->Create<AdditionOperator>(e1, e2);
      case MemgraphCypher::MINUS:
        return storage_->Create<SubtractionOperator>(e1, e2);
      case MemgraphCypher::ASTERISK:
        return storage_->Create<MultiplicationOperator>(e1, e2);
      case MemgraphCypher::SLASH:
        return storage_->Create<DivisionOperator>(e1, e2);
      case MemgraphCypher::PERCENT:
        return storage_->Create<ModOperator>(e1, e2);
      case MemgraphCypher::EQ:
        return storage_->Create<EqualOperator>(e1, e2);
      case MemgraphCypher::NEQ1:
      case MemgraphCypher::NEQ2:
        return storage_->Create<NotEqualOperator>(e1, e2);
      case MemgraphCypher::LT:
        return storage_->Create<LessOperator>(e1, e2);
      case MemgraphCypher::GT:
        return storage_->Create<GreaterOperator>(e1, e2);
      case MemgraphCypher::LTE:
        return storage_->Create<LessEqualOperator>(e1, e2);
      case MemgraphCypher::GTE:
        return storage_->Create<GreaterEqualOperator>(e1, e2);
      default:
        throw utils::NotYetImplemented("binary operator");
    }
  }

  Expression *CreateUnaryOperatorByToken(size_t token, Expression *e) {
    switch (token) {
      case MemgraphCypher::NOT:
        return storage_->Create<NotOperator>(e);
      case MemgraphCypher::PLUS:
        return storage_->Create<UnaryPlusOperator>(e);
      case MemgraphCypher::MINUS:
        return storage_->Create<UnaryMinusOperator>(e);
      default:
        throw utils::NotYetImplemented("unary operator");
    }
  }

  inline static auto ExtractOperators(std::vector<antlr4::tree::ParseTree *> &all_children,
                                      const std::vector<size_t> &allowed_operators) {
    std::vector<size_t> operators;
    for (auto *child : all_children) {
      antlr4::tree::TerminalNode *operator_node = nullptr;
      if ((operator_node = dynamic_cast<antlr4::tree::TerminalNode *>(child))) {
        if (std::find(allowed_operators.begin(), allowed_operators.end(), operator_node->getSymbol()->getType()) !=
            allowed_operators.end()) {
          operators.push_back(operator_node->getSymbol()->getType());
        }
      }
    }
    return operators;
  }

  /**
   * Convert opencypher's n-ary production to ast binary operators.
   *
   * @param _expressions Subexpressions of child for which we construct ast
   * operators, for example expression6 if we want to create ast nodes for
   * expression7.
   */
  template <typename TExpression>
  Expression *LeftAssociativeOperatorExpression(std::vector<TExpression *> _expressions,
                                                std::vector<antlr4::tree::ParseTree *> all_children,
                                                const std::vector<size_t> &allowed_operators) {
    DMG_ASSERT(!_expressions.empty(), "can't happen");
    std::vector<Expression *> expressions;
    expressions.reserve(_expressions.size());
    auto operators = ExtractOperators(all_children, allowed_operators);

    for (auto *expression : _expressions) {
      expressions.push_back(std::any_cast<Expression *>(expression->accept(this)));
    }

    Expression *first_operand = expressions[0];
    for (int i = 1; i < (int)expressions.size(); ++i) {
      first_operand = CreateBinaryOperatorByToken(operators[i - 1], first_operand, expressions[i]);
    }
    return first_operand;
  }

  template <typename TExpression>
  Expression *PrefixUnaryOperator(TExpression *_expression, std::vector<antlr4::tree::ParseTree *> all_children,
                                  const std::vector<size_t> &allowed_operators) {
    DMG_ASSERT(_expression, "can't happen");
    auto operators = ExtractOperators(all_children, allowed_operators);

    auto *expression = std::any_cast<Expression *>(_expression->accept(this));
    for (int i = (int)operators.size() - 1; i >= 0; --i) {
      expression = CreateUnaryOperatorByToken(operators[i], expression);
    }
    return expression;
  }

  /**
   * @return CypherQuery*
   */
  antlrcpp::Any visitCypherQuery(MemgraphCypher::CypherQueryContext *ctx) override {
    auto *cypher_query = storage_->Create<CypherQuery>();
    MG_ASSERT(ctx->singleQuery(), "Expected single query.");
    cypher_query->single_query_ = std::any_cast<SingleQuery *>(ctx->singleQuery()->accept(this));

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
        throw memgraph::expr::SemanticException("Invalid combination of UNION and UNION ALL.");
      }
      cypher_query->cypher_unions_.push_back(std::any_cast<CypherUnion *>(child->accept(this)));
    }

    if (auto *memory_limit_ctx = ctx->queryMemoryLimit()) {
      const auto memory_limit_info = detail::VisitMemoryLimit(memory_limit_ctx->memoryLimit(), this);
      if (memory_limit_info) {
        cypher_query->memory_limit_ = memory_limit_info->first;
        cypher_query->memory_scale_ = memory_limit_info->second;
      }
    }

    query_ = cypher_query;
    return cypher_query;
  }

  /**
   * @return IndexQuery*
   */
  antlrcpp::Any visitIndexQuery(MemgraphCypher::IndexQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "IndexQuery should have exactly one child!");
    auto *index_query = std::any_cast<IndexQuery *>(ctx->children[0]->accept(this));
    query_ = index_query;
    return index_query;
  }

  /**
   * @return ExplainQuery*
   */
  antlrcpp::Any visitExplainQuery(MemgraphCypher::ExplainQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 2, "ExplainQuery should have exactly two children!");
    auto *cypher_query = std::any_cast<CypherQuery *>(ctx->children[1]->accept(this));
    auto *explain_query = storage_->Create<ExplainQuery>();
    explain_query->cypher_query_ = cypher_query;
    query_ = explain_query;
    return explain_query;
  }

  /**
   * @return ProfileQuery*
   */
  antlrcpp::Any visitProfileQuery(MemgraphCypher::ProfileQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 2, "ProfileQuery should have exactly two children!");
    auto *cypher_query = std::any_cast<CypherQuery *>(ctx->children[1]->accept(this));
    auto *profile_query = storage_->Create<ProfileQuery>();
    profile_query->cypher_query_ = cypher_query;
    query_ = profile_query;
    return profile_query;
  }

  /**
   * @return InfoQuery*
   */
  antlrcpp::Any visitInfoQuery(MemgraphCypher::InfoQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 2, "InfoQuery should have exactly two children!");
    auto *info_query = storage_->Create<InfoQuery>();
    query_ = info_query;
    if (ctx->storageInfo()) {
      info_query->info_type_ = InfoQuery::InfoType::STORAGE;
      return info_query;
    }
    if (ctx->indexInfo()) {
      info_query->info_type_ = InfoQuery::InfoType::INDEX;
      return info_query;
    }
    if (ctx->constraintInfo()) {
      info_query->info_type_ = InfoQuery::InfoType::CONSTRAINT;
      return info_query;
    }
    throw utils::NotYetImplemented("Info query: '{}'", ctx->getText());
  }

  /**
   * @return Constraint
   */
  antlrcpp::Any visitConstraint(MemgraphCypher::ConstraintContext *ctx) override {
    Constraint constraint;
    MG_ASSERT(ctx->EXISTS() || ctx->UNIQUE() || (ctx->NODE() && ctx->KEY()));
    if (ctx->EXISTS()) {
      constraint.type = Constraint::Type::EXISTS;
    } else if (ctx->UNIQUE()) {
      constraint.type = Constraint::Type::UNIQUE;
    } else if (ctx->NODE() && ctx->KEY()) {
      constraint.type = Constraint::Type::NODE_KEY;
    }
    constraint.label = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
    auto node_name = std::any_cast<std::string>(ctx->nodeName->symbolicName()->accept(this));
    for (const auto &var_ctx : ctx->constraintPropertyList()->variable()) {
      auto var_name = std::any_cast<std::string>(var_ctx->symbolicName()->accept(this));
      if (var_name != node_name) {
        throw memgraph::expr::SemanticException("All constraint variable should reference node '{}'", node_name);
      }
    }
    for (const auto &prop_lookup : ctx->constraintPropertyList()->propertyLookup()) {
      constraint.properties.push_back(std::any_cast<PropertyIx>(prop_lookup->propertyKeyName()->accept(this)));
    }

    return constraint;
  }

  /**
   * @return ConstraintQuery*
   */
  antlrcpp::Any visitConstraintQuery(MemgraphCypher::ConstraintQueryContext *ctx) override {
    auto *constraint_query = storage_->Create<ConstraintQuery>();
    MG_ASSERT(ctx->CREATE() || ctx->DROP());
    if (ctx->CREATE()) {
      constraint_query->action_type_ = ConstraintQuery::ActionType::CREATE;
    } else if (ctx->DROP()) {
      constraint_query->action_type_ = ConstraintQuery::ActionType::DROP;
    }
    constraint_query->constraint_ = std::any_cast<Constraint>(ctx->constraint()->accept(this));
    query_ = constraint_query;
    return query_;
  }

  /**
   * @return DumpQuery*
   */
  antlrcpp::Any visitDumpQuery(MemgraphCypher::DumpQueryContext * /*ctx*/) override {
    auto *dump_query = storage_->Create<DumpQuery>();
    query_ = dump_query;
    return dump_query;
  }

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitReplicationQuery(MemgraphCypher::ReplicationQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "ReplicationQuery should have exactly one child!");
    auto *replication_query = std::any_cast<ReplicationQuery *>(ctx->children[0]->accept(this));
    query_ = replication_query;
    return replication_query;
  }

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *ctx) override {
    auto *replication_query = storage_->Create<ReplicationQuery>();
    replication_query->action_ = ReplicationQuery::Action::SET_REPLICATION_ROLE;
    if (ctx->MAIN()) {
      if (ctx->WITH() || ctx->PORT()) {
        throw memgraph::expr::SemanticException("Main can't set a port!");
      }
      replication_query->role_ = ReplicationQuery::ReplicationRole::MAIN;
    } else if (ctx->REPLICA()) {
      replication_query->role_ = ReplicationQuery::ReplicationRole::REPLICA;
      if (ctx->WITH() && ctx->PORT()) {
        if (ctx->port->numberLiteral() && ctx->port->numberLiteral()->integerLiteral()) {
          replication_query->port_ = std::any_cast<Expression *>(ctx->port->accept(this));
        } else {
          throw memgraph::expr::SyntaxException("Port must be an integer literal!");
        }
      }
    }
    return replication_query;
  }

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext * /*ctx*/) override {
    auto *replication_query = storage_->Create<ReplicationQuery>();
    replication_query->action_ = ReplicationQuery::Action::SHOW_REPLICATION_ROLE;
    return replication_query;
  }

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitRegisterReplica(MemgraphCypher::RegisterReplicaContext *ctx) override {
    auto *replication_query = storage_->Create<ReplicationQuery>();
    replication_query->action_ = ReplicationQuery::Action::REGISTER_REPLICA;
    replication_query->replica_name_ = std::any_cast<std::string>(ctx->replicaName()->symbolicName()->accept(this));
    if (ctx->SYNC()) {
      replication_query->sync_mode_ = ReplicationQuery::SyncMode::SYNC;
    } else if (ctx->ASYNC()) {
      replication_query->sync_mode_ = ReplicationQuery::SyncMode::ASYNC;
    }

    if (!ctx->socketAddress()->literal()->StringLiteral()) {
      throw memgraph::expr::SemanticException("Socket address should be a string literal!");
    }
    replication_query->socket_address_ = std::any_cast<Expression *>(ctx->socketAddress()->accept(this));

    return replication_query;
  }

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitDropReplica(MemgraphCypher::DropReplicaContext *ctx) override {
    auto *replication_query = storage_->Create<ReplicationQuery>();
    replication_query->action_ = ReplicationQuery::Action::DROP_REPLICA;
    replication_query->replica_name_ = std::any_cast<std::string>(ctx->replicaName()->symbolicName()->accept(this));
    return replication_query;
  }

  /**
   * @return ReplicationQuery*
   */
  antlrcpp::Any visitShowReplicas(MemgraphCypher::ShowReplicasContext * /*ctx*/) override {
    auto *replication_query = storage_->Create<ReplicationQuery>();
    replication_query->action_ = ReplicationQuery::Action::SHOW_REPLICAS;
    return replication_query;
  }

  /**
   * @return LockPathQuery*
   */
  antlrcpp::Any visitLockPathQuery(MemgraphCypher::LockPathQueryContext *ctx) override {
    auto *lock_query = storage_->Create<LockPathQuery>();
    if (ctx->LOCK()) {
      lock_query->action_ = LockPathQuery::Action::LOCK_PATH;
    } else if (ctx->UNLOCK()) {
      lock_query->action_ = LockPathQuery::Action::UNLOCK_PATH;
    } else {
      throw memgraph::expr::SyntaxException("Expected LOCK or UNLOCK");
    }

    query_ = lock_query;
    return lock_query;
  }

  /**
   * @return LoadCsvQuery*
   */
  antlrcpp::Any visitLoadCsv(MemgraphCypher::LoadCsvContext *ctx) override {
    query_info_.has_load_csv = true;

    auto *load_csv = storage_->Create<LoadCsv>();
    // handle file name
    if (ctx->csvFile()->literal()->StringLiteral()) {
      load_csv->file_ = std::any_cast<Expression *>(ctx->csvFile()->accept(this));
    } else {
      throw memgraph::expr::SemanticException("CSV file path should be a string literal");
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
        load_csv->delimiter_ = std::any_cast<Expression *>(ctx->delimiter()->accept(this));
      } else {
        throw memgraph::expr::SemanticException("Delimiter should be a string literal");
      }
    }

    // handle quote
    if (ctx->QUOTE()) {
      if (ctx->quote()->literal()->StringLiteral()) {
        load_csv->quote_ = std::any_cast<Expression *>(ctx->quote()->accept(this));
      } else {
        throw memgraph::expr::SemanticException("Quote should be a string literal");
      }
    }

    // handle row variable
    load_csv->row_var_ =
        storage_->Create<Identifier>(std::any_cast<std::string>(ctx->rowVar()->variable()->accept(this)));

    return load_csv;
  }

  /**
   * @return FreeMemoryQuery*
   */
  antlrcpp::Any visitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext * /*ctx*/) override {
    auto *free_memory_query = storage_->Create<FreeMemoryQuery>();
    query_ = free_memory_query;
    return free_memory_query;
  }

  /**
   * @return TriggerQuery*
   */
  antlrcpp::Any visitTriggerQuery(MemgraphCypher::TriggerQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "TriggerQuery should have exactly one child!");
    auto *trigger_query = std::any_cast<TriggerQuery *>(ctx->children[0]->accept(this));
    query_ = trigger_query;
    return trigger_query;
  }

  /**
   * @return CreateTrigger*
   */
  antlrcpp::Any visitCreateTrigger(MemgraphCypher::CreateTriggerContext *ctx) override {
    auto *trigger_query = storage_->Create<TriggerQuery>();
    trigger_query->action_ = TriggerQuery::Action::CREATE_TRIGGER;
    trigger_query->trigger_name_ = std::any_cast<std::string>(ctx->triggerName()->symbolicName()->accept(this));

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

  /**
   * @return DropTrigger*
   */
  antlrcpp::Any visitDropTrigger(MemgraphCypher::DropTriggerContext *ctx) override {
    auto *trigger_query = storage_->Create<TriggerQuery>();
    trigger_query->action_ = TriggerQuery::Action::DROP_TRIGGER;
    trigger_query->trigger_name_ = std::any_cast<std::string>(ctx->triggerName()->symbolicName()->accept(this));
    return trigger_query;
  }

  /**
   * @return ShowTriggers*
   */
  antlrcpp::Any visitShowTriggers(MemgraphCypher::ShowTriggersContext * /*ctx*/) override {
    auto *trigger_query = storage_->Create<TriggerQuery>();
    trigger_query->action_ = TriggerQuery::Action::SHOW_TRIGGERS;
    return trigger_query;
  }

  /**
   * @return IsolationLevelQuery*
   */
  antlrcpp::Any visitIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext *ctx) override {
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

  /**
   * @return CreateSnapshotQuery*
   */
  antlrcpp::Any visitCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext * /*ctx*/) override {
    query_ = storage_->Create<CreateSnapshotQuery>();
    return query_;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStreamQuery(MemgraphCypher::StreamQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "StreamQuery should have exactly one child!");
    auto *stream_query = std::any_cast<StreamQuery *>(ctx->children[0]->accept(this));
    query_ = stream_query;
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitCreateStream(MemgraphCypher::CreateStreamContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "CreateStreamQuery should have exactly one child!");
    auto *stream_query = std::any_cast<StreamQuery *>(ctx->children[0]->accept(this));
    query_ = stream_query;
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext *ctx) override {
    MG_ASSERT(ctx->literal().size() == 2);
    return std::pair{std::any_cast<Expression *>(ctx->literal(0)->accept(this)),
                     std::any_cast<Expression *>(ctx->literal(1)->accept(this))};
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitConfigMap(MemgraphCypher::ConfigMapContext *ctx) override {
    std::unordered_map<Expression *, Expression *> map;
    for (auto *key_value_pair : ctx->configKeyValuePair()) {
      // If the queries are cached, then only the stripped query is parsed, so the actual keys cannot be determined
      // here. That means duplicates cannot be checked.
      map.insert(std::any_cast<std::pair<Expression *, Expression *>>(key_value_pair->accept(this)));
    }
    return map;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext *ctx) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::CREATE_STREAM;
    stream_query->type_ = StreamQuery::Type::KAFKA;
    stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));

    for (auto *create_config_ctx : ctx->kafkaCreateStreamConfig()) {
      create_config_ctx->accept(this);
    }

    detail::MapConfig<true, std::vector<std::string>, Expression *>(memory_, detail::KafkaConfigKey::TOPICS,
                                                                    stream_query->topic_names_);
    detail::MapConfig<false, std::string>(memory_, detail::KafkaConfigKey::CONSUMER_GROUP,
                                          stream_query->consumer_group_);
    detail::MapConfig<false, Expression *>(memory_, detail::KafkaConfigKey::BOOTSTRAP_SERVERS,
                                           stream_query->bootstrap_servers_);
    detail::MapConfig<false, std::unordered_map<Expression *, Expression *>>(memory_, detail::KafkaConfigKey::CONFIGS,
                                                                             stream_query->configs_);
    detail::MapConfig<false, std::unordered_map<Expression *, Expression *>>(
        memory_, detail::KafkaConfigKey::CREDENTIALS, stream_query->credentials_);

    detail::MapCommonStreamConfigs(memory_, *stream_query);

    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext *ctx) override {
    if (ctx->commonCreateStreamConfig()) {
      return ctx->commonCreateStreamConfig()->accept(this);
    }

    if (ctx->TOPICS()) {
      detail::ThrowIfExists(memory_, detail::KafkaConfigKey::TOPICS);
      static constexpr auto topics_key = static_cast<uint8_t>(detail::KafkaConfigKey::TOPICS);
      detail::GetTopicNames(memory_[topics_key], ctx->topicNames(), *this);
      return {};
    }

    if (ctx->CONSUMER_GROUP()) {
      detail::ThrowIfExists(memory_, detail::KafkaConfigKey::CONSUMER_GROUP);
      static constexpr auto consumer_group_key = static_cast<uint8_t>(detail::KafkaConfigKey::CONSUMER_GROUP);
      memory_[consumer_group_key] = detail::JoinSymbolicNamesWithDotsAndMinus(*this, *ctx->consumerGroup);
      return {};
    }

    if (ctx->CONFIGS()) {
      detail::ThrowIfExists(memory_, detail::KafkaConfigKey::CONFIGS);
      static constexpr auto configs_key = static_cast<uint8_t>(detail::KafkaConfigKey::CONFIGS);
      memory_.emplace(configs_key,
                      std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->configsMap->accept(this)));
      return {};
    }

    if (ctx->CREDENTIALS()) {
      detail::ThrowIfExists(memory_, detail::KafkaConfigKey::CREDENTIALS);
      static constexpr auto credentials_key = static_cast<uint8_t>(detail::KafkaConfigKey::CREDENTIALS);
      memory_.emplace(credentials_key,
                      std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->credentialsMap->accept(this)));
      return {};
    }

    MG_ASSERT(ctx->BOOTSTRAP_SERVERS());
    detail::ThrowIfExists(memory_, detail::KafkaConfigKey::BOOTSTRAP_SERVERS);
    if (!ctx->bootstrapServers->StringLiteral()) {
      throw memgraph::expr::SemanticException("Bootstrap servers should be a string!");
    }

    const auto bootstrap_servers_key = static_cast<uint8_t>(detail::KafkaConfigKey::BOOTSTRAP_SERVERS);
    memory_[bootstrap_servers_key] = std::any_cast<Expression *>(ctx->bootstrapServers->accept(this));
    return {};
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext *ctx) override {
    if (ctx->commonCreateStreamConfig()) {
      return ctx->commonCreateStreamConfig()->accept(this);
    }

    if (ctx->TOPICS()) {
      detail::ThrowIfExists(memory_, detail::PulsarConfigKey::TOPICS);
      const auto topics_key = static_cast<uint8_t>(detail::PulsarConfigKey::TOPICS);
      detail::GetTopicNames(memory_[topics_key], ctx->topicNames(), *this);
      return {};
    }

    MG_ASSERT(ctx->SERVICE_URL());
    detail::ThrowIfExists(memory_, detail::PulsarConfigKey::SERVICE_URL);
    if (!ctx->serviceUrl->StringLiteral()) {
      throw memgraph::expr::SemanticException("Service URL must be a string!");
    }
    const auto service_url_key = static_cast<uint8_t>(detail::PulsarConfigKey::SERVICE_URL);
    memory_[service_url_key] = std::any_cast<Expression *>(ctx->serviceUrl->accept(this));
    return {};
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext *ctx) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::CREATE_STREAM;
    stream_query->type_ = StreamQuery::Type::PULSAR;
    stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));

    for (auto *create_config_ctx : ctx->pulsarCreateStreamConfig()) {
      create_config_ctx->accept(this);
    }

    detail::MapConfig<true, std::vector<std::string>, Expression *>(memory_, detail::PulsarConfigKey::TOPICS,
                                                                    stream_query->topic_names_);
    detail::MapConfig<false, Expression *>(memory_, detail::PulsarConfigKey::SERVICE_URL, stream_query->service_url_);

    detail::MapCommonStreamConfigs(memory_, *stream_query);

    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext *ctx) override {
    if (ctx->TRANSFORM()) {
      detail::ThrowIfExists(memory_, detail::CommonStreamConfigKey::TRANSFORM);
      const auto transform_key = static_cast<uint8_t>(detail::CommonStreamConfigKey::TRANSFORM);
      memory_[transform_key] = detail::JoinSymbolicNames(this, ctx->transformationName->symbolicName());
      return {};
    }

    if (ctx->BATCH_INTERVAL()) {
      detail::ThrowIfExists(memory_, detail::CommonStreamConfigKey::BATCH_INTERVAL);
      if (!ctx->batchInterval->numberLiteral() || !ctx->batchInterval->numberLiteral()->integerLiteral()) {
        throw memgraph::expr::SemanticException("Batch interval must be an integer literal!");
      }
      const auto batch_interval_key = static_cast<uint8_t>(detail::CommonStreamConfigKey::BATCH_INTERVAL);
      memory_[batch_interval_key] = std::any_cast<Expression *>(ctx->batchInterval->accept(this));
      return {};
    }

    MG_ASSERT(ctx->BATCH_SIZE());
    detail::ThrowIfExists(memory_, detail::CommonStreamConfigKey::BATCH_SIZE);
    if (!ctx->batchSize->numberLiteral() || !ctx->batchSize->numberLiteral()->integerLiteral()) {
      throw memgraph::expr::SemanticException("Batch size must be an integer literal!");
    }
    const auto batch_size_key = static_cast<uint8_t>(detail::CommonStreamConfigKey::BATCH_SIZE);
    memory_[batch_size_key] = std::any_cast<Expression *>(ctx->batchSize->accept(this));
    return {};
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitDropStream(MemgraphCypher::DropStreamContext *ctx) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::DROP_STREAM;
    stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStartStream(MemgraphCypher::StartStreamContext *ctx) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::START_STREAM;

    if (ctx->BATCH_LIMIT()) {
      if (!ctx->batchLimit->numberLiteral() || !ctx->batchLimit->numberLiteral()->integerLiteral()) {
        throw memgraph::expr::SemanticException("Batch limit should be an integer literal!");
      }
      stream_query->batch_limit_ = std::any_cast<Expression *>(ctx->batchLimit->accept(this));
    }
    if (ctx->TIMEOUT()) {
      if (!ctx->timeout->numberLiteral() || !ctx->timeout->numberLiteral()->integerLiteral()) {
        throw memgraph::expr::SemanticException("Timeout should be an integer literal!");
      }
      if (!ctx->BATCH_LIMIT()) {
        throw memgraph::expr::SemanticException("Parameter TIMEOUT can only be defined if BATCH_LIMIT is defined");
      }
      stream_query->timeout_ = std::any_cast<Expression *>(ctx->timeout->accept(this));
    }

    stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStartAllStreams(MemgraphCypher::StartAllStreamsContext * /*ctx*/) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::START_ALL_STREAMS;
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStopStream(MemgraphCypher::StopStreamContext *ctx) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::STOP_STREAM;
    stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitStopAllStreams(MemgraphCypher::StopAllStreamsContext * /*ctx*/) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::STOP_ALL_STREAMS;
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitShowStreams(MemgraphCypher::ShowStreamsContext * /*ctx*/) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::SHOW_STREAMS;
    return stream_query;
  }

  /**
   * @return StreamQuery*
   */
  antlrcpp::Any visitCheckStream(MemgraphCypher::CheckStreamContext *ctx) override {
    auto *stream_query = storage_->Create<StreamQuery>();
    stream_query->action_ = StreamQuery::Action::CHECK_STREAM;
    stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));

    if (ctx->BATCH_LIMIT()) {
      if (!ctx->batchLimit->numberLiteral() || !ctx->batchLimit->numberLiteral()->integerLiteral()) {
        throw memgraph::expr::SemanticException("Batch limit should be an integer literal!");
      }
      stream_query->batch_limit_ = std::any_cast<Expression *>(ctx->batchLimit->accept(this));
    }
    if (ctx->TIMEOUT()) {
      if (!ctx->timeout->numberLiteral() || !ctx->timeout->numberLiteral()->integerLiteral()) {
        throw memgraph::expr::SemanticException("Timeout should be an integer literal!");
      }
      stream_query->timeout_ = std::any_cast<Expression *>(ctx->timeout->accept(this));
    }
    return stream_query;
  }

  /**
   * @return SettingQuery*
   */
  antlrcpp::Any visitSettingQuery(MemgraphCypher::SettingQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "SettingQuery should have exactly one child!");
    auto *setting_query = std::any_cast<SettingQuery *>(ctx->children[0]->accept(this));
    query_ = setting_query;
    return setting_query;
  }

  /**
   * @return SetSetting*
   */
  antlrcpp::Any visitSetSetting(MemgraphCypher::SetSettingContext *ctx) override {
    auto *setting_query = storage_->Create<SettingQuery>();
    setting_query->action_ = SettingQuery::Action::SET_SETTING;

    if (!ctx->settingName()->literal()->StringLiteral()) {
      throw memgraph::expr::SemanticException("Setting name should be a string literal");
    }

    if (!ctx->settingValue()->literal()->StringLiteral()) {
      throw memgraph::expr::SemanticException("Setting value should be a string literal");
    }

    setting_query->setting_name_ = std::any_cast<Expression *>(ctx->settingName()->accept(this));
    MG_ASSERT(setting_query->setting_name_);

    setting_query->setting_value_ = std::any_cast<Expression *>(ctx->settingValue()->accept(this));
    MG_ASSERT(setting_query->setting_value_);
    return setting_query;
  }

  /**
   * @return ShowSetting*
   */
  antlrcpp::Any visitShowSetting(MemgraphCypher::ShowSettingContext *ctx) override {
    auto *setting_query = storage_->Create<SettingQuery>();
    setting_query->action_ = SettingQuery::Action::SHOW_SETTING;

    if (!ctx->settingName()->literal()->StringLiteral()) {
      throw memgraph::expr::SemanticException("Setting name should be a string literal");
    }

    setting_query->setting_name_ = std::any_cast<Expression *>(ctx->settingName()->accept(this));
    MG_ASSERT(setting_query->setting_name_);

    return setting_query;
  }

  /**
   * @return ShowSettings*
   */
  antlrcpp::Any visitShowSettings(MemgraphCypher::ShowSettingsContext * /*ctx*/) override {
    auto *setting_query = storage_->Create<SettingQuery>();
    setting_query->action_ = SettingQuery::Action::SHOW_ALL_SETTINGS;
    return setting_query;
  }

  /**
   * @return VersionQuery*
   */
  antlrcpp::Any visitVersionQuery(MemgraphCypher::VersionQueryContext * /*ctx*/) override {
    auto *version_query = storage_->Create<VersionQuery>();
    query_ = version_query;
    return version_query;
  }

  /**
   * @return CypherUnion*
   */
  antlrcpp::Any visitCypherUnion(MemgraphCypher::CypherUnionContext *ctx) override {
    bool distinct = !ctx->ALL();
    auto *cypher_union = storage_->Create<CypherUnion>(distinct);
    DMG_ASSERT(ctx->singleQuery(), "Expected single query.");
    cypher_union->single_query_ = std::any_cast<SingleQuery *>(ctx->singleQuery()->accept(this));
    return cypher_union;
  }

  /**
   * @return SingleQuery*
   */
  antlrcpp::Any visitSingleQuery(MemgraphCypher::SingleQueryContext *ctx) override {
    auto *single_query = storage_->Create<SingleQuery>();
    for (auto *child : ctx->clause()) {
      antlrcpp::Any got = child->accept(this);
      if (got.type() == typeid(Clause *)) {
        single_query->clauses_.push_back(std::any_cast<Clause *>(got));
      } else {
        auto child_clauses = std::any_cast<std::vector<Clause *>>(got);
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
        throw memgraph::expr::SemanticException(
            "{} can't be put after calling a writeable procedure, only RETURN clause can be put after.", clause);
      }
    };

    for (Clause *clause : single_query->clauses_) {
      const auto &clause_type = clause->GetTypeInfo();
      //    if (const auto *call_procedure = utils::Downcast<CallProcedure>(clause); call_procedure != nullptr) {
      //      if (has_return) {
      //        throw SemanticException("CALL can't be put after RETURN clause.");
      //      }
      //      check_write_procedure("CALL");
      //      has_call_procedure = true;
      //      if (call_procedure->is_write_) {
      //        calls_write_procedure = true;
      //        has_update = true;
      //      }
      //    }
      if (utils::IsSubtype(clause_type, Unwind::kType)) {
        check_write_procedure("UNWIND");
        if (has_update || has_return) {
          throw memgraph::expr::SemanticException("UNWIND can't be put after RETURN clause or after an update.");
        }
      } else if (utils::IsSubtype(clause_type, LoadCsv::kType)) {
        if (has_load_csv) {
          throw memgraph::expr::SemanticException("Can't have multiple LOAD CSV clauses in a single query.");
        }
        check_write_procedure("LOAD CSV");
        if (has_return) {
          throw memgraph::expr::SemanticException("LOAD CSV can't be put after RETURN clause.");
        }
        has_load_csv = true;
      } else if (auto *match = utils::Downcast<Match>(clause)) {
        if (has_update || has_return) {
          throw memgraph::expr::SemanticException("MATCH can't be put after RETURN clause or after an update.");
        }
        if (match->optional_) {
          has_optional_match = true;
        } else if (has_optional_match) {
          throw memgraph::expr::SemanticException("MATCH can't be put after OPTIONAL MATCH.");
        }
        check_write_procedure("MATCH");
      } else if (utils::IsSubtype(clause_type, Create::kType) || utils::IsSubtype(clause_type, Delete::kType) ||
                 utils::IsSubtype(clause_type, SetProperty::kType) ||
                 utils::IsSubtype(clause_type, SetProperties::kType) ||
                 utils::IsSubtype(clause_type, SetLabels::kType) ||
                 utils::IsSubtype(clause_type, RemoveProperty::kType) ||
                 utils::IsSubtype(clause_type, RemoveLabels::kType) || utils::IsSubtype(clause_type, Merge::kType) ||
                 utils::IsSubtype(clause_type, Foreach::kType)) {
        if (has_return) {
          throw memgraph::expr::SemanticException("Update clause can't be used after RETURN.");
        }
        check_write_procedure("Update clause");
        has_update = true;
        has_any_update = true;
      } else if (utils::IsSubtype(clause_type, Return::kType)) {
        if (has_return) {
          throw memgraph::expr::SemanticException("There can only be one RETURN in a clause.");
        }
        has_return = true;
      } else if (utils::IsSubtype(clause_type, With::kType)) {
        if (has_return) {
          throw memgraph::expr::SemanticException("RETURN can't be put before WITH.");
        }
        check_write_procedure("WITH");
        has_update = has_return = has_optional_match = false;
      } else {
        DLOG_FATAL("Can't happen");
      }
    }
    bool is_standalone_call_procedure = has_call_procedure && single_query->clauses_.size() == 1U;
    if (!has_update && !has_return && !is_standalone_call_procedure) {
      throw memgraph::expr::SemanticException("Query should either create or update something, or return results!");
    }

    if (has_any_update && calls_write_procedure) {
      throw memgraph::expr::SemanticException(
          "Write procedures cannot be used in queries that contains any update clauses!");
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

  /**
   * @return Clause* or vector<Clause*>!!!
   */
  antlrcpp::Any visitClause(MemgraphCypher::ClauseContext *ctx) override {
    if (ctx->cypherReturn()) {
      return static_cast<Clause *>(std::any_cast<Return *>(ctx->cypherReturn()->accept(this)));
    }
    if (ctx->cypherMatch()) {
      return static_cast<Clause *>(std::any_cast<Match *>(ctx->cypherMatch()->accept(this)));
    }
    if (ctx->create()) {
      return static_cast<Clause *>(std::any_cast<Create *>(ctx->create()->accept(this)));
    }
    if (ctx->cypherDelete()) {
      return static_cast<Clause *>(std::any_cast<Delete *>(ctx->cypherDelete()->accept(this)));
    }
    if (ctx->set()) {
      // Different return type!!!
      return std::any_cast<std::vector<Clause *>>(ctx->set()->accept(this));
    }
    if (ctx->remove()) {
      // Different return type!!!
      return std::any_cast<std::vector<Clause *>>(ctx->remove()->accept(this));
    }
    if (ctx->with()) {
      return static_cast<Clause *>(std::any_cast<With *>(ctx->with()->accept(this)));
    }
    if (ctx->merge()) {
      return static_cast<Clause *>(std::any_cast<Merge *>(ctx->merge()->accept(this)));
    }
    if (ctx->unwind()) {
      return static_cast<Clause *>(std::any_cast<Unwind *>(ctx->unwind()->accept(this)));
    }
    //  if (ctx->callProcedure()) {
    //    return static_cast<Clause *>(std::any_cast<CallProcedure *>(ctx->callProcedure()->accept(this)));
    //  }
    if (ctx->loadCsv()) {
      return static_cast<Clause *>(std::any_cast<LoadCsv *>(ctx->loadCsv()->accept(this)));
    }
    if (ctx->foreach ()) {
      return static_cast<Clause *>(std::any_cast<Foreach *>(ctx->foreach ()->accept(this)));
    }
    // TODO: implement other clauses.
    throw utils::NotYetImplemented("clause '{}'", ctx->getText());
    return 0;
  }

  /**
   * @return Match*
   */
  antlrcpp::Any visitCypherMatch(MemgraphCypher::CypherMatchContext *ctx) override {
    auto *match = storage_->Create<Match>();
    match->optional_ = !!ctx->OPTIONAL();
    if (ctx->where()) {
      match->where_ = std::any_cast<Where *>(ctx->where()->accept(this));
    }
    match->patterns_ = std::any_cast<std::vector<Pattern *>>(ctx->pattern()->accept(this));
    return match;
  }

  /**
   * @return Create*
   */
  antlrcpp::Any visitCreate(MemgraphCypher::CreateContext *ctx) override {
    auto *create = storage_->Create<Create>();
    create->patterns_ = std::any_cast<std::vector<Pattern *>>(ctx->pattern()->accept(this));
    return create;
  }

  /**
   * @return CallProcedure*
   */
  // TODO(kostasrim) Add support for this
  // antlrcpp::Any visitCallProcedure(MemgraphCypher::CallProcedureContext *ctx) override {
  //  // Don't cache queries which call procedures because the
  //  // procedure definition can affect the behaviour of the visitor and
  //  // the execution of the query.
  //  // If a user recompiles and reloads the procedure with different result
  //  // names, because of the cache, old result names will be expected while the
  //  // procedure will return results mapped to new names.
  //  query_info_.is_cacheable = false;
  //
  //  auto *call_proc = storage_->Create<CallProcedure>();
  //  MG_ASSERT(!ctx->procedureName()->symbolicName().empty());
  //  call_proc->procedure_name_ = JoinSymbolicNames(this, ctx->procedureName()->symbolicName());
  //  call_proc->arguments_.reserve(ctx->expression().size());
  //  for (auto *expr : ctx->expression()) {
  //    call_proc->arguments_.push_back(std::any_cast<Expression *>(expr->accept(this)));
  //  }
  //
  //  if (auto *memory_limit_ctx = ctx->procedureMemoryLimit()) {
  //    const auto memory_limit_info = VisitMemoryLimit(memory_limit_ctx->memoryLimit(), this);
  //    if (memory_limit_info) {
  //      call_proc->memory_limit_ = memory_limit_info->first;
  //      call_proc->memory_scale_ = memory_limit_info->second;
  //    }
  //  } else {
  //    // Default to 100 MB
  //    call_proc->memory_limit_ = storage_->Create<PrimitiveLiteral>(TypedValue(100));
  //    call_proc->memory_scale_ = 1024U * 1024U;
  //  }
  //
  //  const auto &maybe_found =
  //      procedure::FindProcedure(procedure::gModuleRegistry, call_proc->procedure_name_, utils::NewDeleteResource());
  //  if (!maybe_found) {
  //    throw SemanticException("There is no procedure named '{}'.", call_proc->procedure_name_);
  //  }
  //  call_proc->is_write_ = maybe_found->second->info.is_write;
  //
  //  auto *yield_ctx = ctx->yieldProcedureResults();
  //  if (!yield_ctx) {
  //    if (!maybe_found->second->results.empty()) {
  //      throw SemanticException(
  //          "CALL without YIELD may only be used on procedures which do not "
  //          "return any result fields.");
  //    }
  //    // When we return, we will release the lock on modules. This means that
  //    // someone may reload the procedure and change the result signature. But to
  //    // keep the implementation simple, we ignore the case as the rest of the
  //    // code doesn't really care whether we yield or not, so it should not break.
  //    return call_proc;
  //  }
  //  if (yield_ctx->getTokens(MemgraphCypher::ASTERISK).empty()) {
  //    call_proc->result_fields_.reserve(yield_ctx->procedureResult().size());
  //    call_proc->result_identifiers_.reserve(yield_ctx->procedureResult().size());
  //    for (auto *result : yield_ctx->procedureResult()) {
  //      MG_ASSERT(result->variable().size() == 1 || result->variable().size() == 2);
  //      call_proc->result_fields_.push_back(std::any_cast<std::string>(result->variable()[0]->accept(this)));
  //      std::string result_alias;
  //      if (result->variable().size() == 2) {
  //        result_alias = std::any_cast<std::string>(result->variable()[1]->accept(this));
  //      } else {
  //        result_alias = std::any_cast<std::string>(result->variable()[0]->accept(this));
  //      }
  //      call_proc->result_identifiers_.push_back(storage_->Create<Identifier>(result_alias));
  //    }
  //  } else {
  //    const auto &maybe_found =
  //        procedure::FindProcedure(procedure::gModuleRegistry, call_proc->procedure_name_,
  //        utils::NewDeleteResource());
  //    if (!maybe_found) {
  //      throw SemanticException("There is no procedure named '{}'.", call_proc->procedure_name_);
  //    }
  //    const auto &[module, proc] = *maybe_found;
  //    call_proc->result_fields_.reserve(proc->results.size());
  //    call_proc->result_identifiers_.reserve(proc->results.size());
  //    for (const auto &[result_name, desc] : proc->results) {
  //      bool is_deprecated = desc.second;
  //      if (is_deprecated) continue;
  //      call_proc->result_fields_.emplace_back(result_name);
  //      call_proc->result_identifiers_.push_back(storage_->Create<Identifier>(std::string(result_name)));
  //    }
  //    // When we leave the scope, we will release the lock on modules. This means
  //    // that someone may reload the procedure and change its result signature. We
  //    // are fine with this, because if new result fields were added then we yield
  //    // the subset of those and that will appear to a user as if they used the
  //    // procedure before reload. Any subsequent `CALL ... YIELD *` will fetch the
  //    // new fields as well. In case the result signature has had some result
  //    // fields removed, then the query execution will report an error that we are
  //    // yielding missing fields. The user can then just retry the query.
  //  }
  //
  //  return call_proc;
  //
  // }

  /**
   * @return std::string
   */
  antlrcpp::Any visitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *ctx) override {
    return std::any_cast<std::string>(ctx->symbolicName()->accept(this));
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitAuthQuery(MemgraphCypher::AuthQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "AuthQuery should have exactly one child!");
    auto *auth_query = std::any_cast<AuthQuery *>(ctx->children[0]->accept(this));
    query_ = auth_query;
    return auth_query;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitCreateRole(MemgraphCypher::CreateRoleContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::CREATE_ROLE;
    auth->role_ = std::any_cast<std::string>(ctx->role->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitDropRole(MemgraphCypher::DropRoleContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::DROP_ROLE;
    auth->role_ = std::any_cast<std::string>(ctx->role->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowRoles(MemgraphCypher::ShowRolesContext * /*ctx*/) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::SHOW_ROLES;
    return auth;
  }

  /**
   * @return IndexQuery*
   */
  antlrcpp::Any visitCreateIndex(MemgraphCypher::CreateIndexContext *ctx) override {
    auto *index_query = storage_->Create<IndexQuery>();
    index_query->action_ = IndexQuery::Action::CREATE;
    index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
    if (ctx->propertyKeyName()) {
      auto name_key = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
      index_query->properties_ = {name_key};
    }
    return index_query;
  }

  /**
   * @return DropIndex*
   */
  antlrcpp::Any visitDropIndex(MemgraphCypher::DropIndexContext *ctx) override {
    auto *index_query = storage_->Create<IndexQuery>();
    index_query->action_ = IndexQuery::Action::DROP;
    if (ctx->propertyKeyName()) {
      auto key = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
      index_query->properties_ = {key};
    }
    index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
    return index_query;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitCreateUser(MemgraphCypher::CreateUserContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::CREATE_USER;
    auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
    if (ctx->password) {
      if (!ctx->password->StringLiteral() && !ctx->literal()->CYPHERNULL()) {
        throw memgraph::expr::SyntaxException("Password should be a string literal or null.");
      }
      auth->password_ = std::any_cast<Expression *>(ctx->password->accept(this));
    }
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitSetPassword(MemgraphCypher::SetPasswordContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::SET_PASSWORD;
    auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
    if (!ctx->password->StringLiteral() && !ctx->literal()->CYPHERNULL()) {
      throw memgraph::expr::SyntaxException("Password should be a string literal or null.");
    }
    auth->password_ = std::any_cast<Expression *>(ctx->password->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitDropUser(MemgraphCypher::DropUserContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::DROP_USER;
    auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowUsers(MemgraphCypher::ShowUsersContext * /*ctx*/) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::SHOW_USERS;
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitSetRole(MemgraphCypher::SetRoleContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::SET_ROLE;
    auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
    auth->role_ = std::any_cast<std::string>(ctx->role->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitClearRole(MemgraphCypher::ClearRoleContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::CLEAR_ROLE;
    auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::GRANT_PRIVILEGE;
    auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
    if (ctx->privilegeList()) {
      for (auto *privilege : ctx->privilegeList()->privilege()) {
        auth->privileges_.push_back(std::any_cast<AuthQuery::Privilege>(privilege->accept(this)));
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
  antlrcpp::Any visitDenyPrivilege(MemgraphCypher::DenyPrivilegeContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::DENY_PRIVILEGE;
    auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
    if (ctx->privilegeList()) {
      for (auto *privilege : ctx->privilegeList()->privilege()) {
        auth->privileges_.push_back(std::any_cast<AuthQuery::Privilege>(privilege->accept(this)));
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
  antlrcpp::Any visitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::REVOKE_PRIVILEGE;
    auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
    if (ctx->privilegeList()) {
      for (auto *privilege : ctx->privilegeList()->privilege()) {
        auth->privileges_.push_back(std::any_cast<AuthQuery::Privilege>(privilege->accept(this)));
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
  antlrcpp::Any visitPrivilege(MemgraphCypher::PrivilegeContext *ctx) override {
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
    if (ctx->MODULE_READ()) return AuthQuery::Privilege::MODULE_READ;
    if (ctx->MODULE_WRITE()) return AuthQuery::Privilege::MODULE_WRITE;
    if (ctx->WEBSOCKET()) return AuthQuery::Privilege::WEBSOCKET;
    if (ctx->SCHEMA()) return AuthQuery::Privilege::SCHEMA;
    LOG_FATAL("Should not get here - unknown privilege!");
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowPrivileges(MemgraphCypher::ShowPrivilegesContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::SHOW_PRIVILEGES;
    auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::SHOW_ROLE_FOR_USER;
    auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
    return auth;
  }

  /**
   * @return AuthQuery*
   */
  antlrcpp::Any visitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *ctx) override {
    auto *auth = storage_->Create<AuthQuery>();
    auth->action_ = AuthQuery::Action::SHOW_USERS_FOR_ROLE;
    auth->role_ = std::any_cast<std::string>(ctx->role->accept(this));
    return auth;
  }

  /**
   * @return Return*
   */
  antlrcpp::Any visitCypherReturn(MemgraphCypher::CypherReturnContext *ctx) override {
    auto *return_clause = storage_->Create<Return>();
    return_clause->body_ = std::any_cast<ReturnBody>(ctx->returnBody()->accept(this));
    if (ctx->DISTINCT()) {
      return_clause->body_.distinct = true;
    }
    return return_clause;
  }

  /**
   * @return Return*
   */
  antlrcpp::Any visitReturnBody(MemgraphCypher::ReturnBodyContext *ctx) override {
    ReturnBody body;
    if (ctx->order()) {
      body.order_by = std::any_cast<std::vector<SortItem>>(ctx->order()->accept(this));
    }
    if (ctx->skip()) {
      body.skip = static_cast<Expression *>(std::any_cast<Expression *>(ctx->skip()->accept(this)));
    }
    if (ctx->limit()) {
      body.limit = static_cast<Expression *>(std::any_cast<Expression *>(ctx->limit()->accept(this)));
    }
    std::tie(body.all_identifiers, body.named_expressions) =
        std::any_cast<std::pair<bool, std::vector<NamedExpression *>>>(ctx->returnItems()->accept(this));
    return body;
  }

  /**
   * @return pair<bool, vector<NamedExpression*>> first member is true if
   * asterisk was found in return
   * expressions.
   */
  antlrcpp::Any visitReturnItems(MemgraphCypher::ReturnItemsContext *ctx) override {
    std::vector<NamedExpression *> named_expressions;
    for (auto *item : ctx->returnItem()) {
      named_expressions.push_back(std::any_cast<NamedExpression *>(item->accept(this)));
    }
    return std::pair<bool, std::vector<NamedExpression *>>(ctx->getTokens(MemgraphCypher::ASTERISK).size(),
                                                           named_expressions);
  }

  /**
   * @return vector<NamedExpression*>
   */
  antlrcpp::Any visitReturnItem(MemgraphCypher::ReturnItemContext *ctx) override {
    auto *named_expr = storage_->Create<NamedExpression>();
    named_expr->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
    MG_ASSERT(named_expr->expression_);
    if (ctx->variable()) {
      named_expr->name_ = std::string(std::any_cast<std::string>(ctx->variable()->accept(this)));
      users_identifiers.insert(named_expr->name_);
    } else {
      if (in_with_ && !utils::IsSubtype(*named_expr->expression_, Identifier::kType)) {
        throw memgraph::expr::SemanticException("Only variables can be non-aliased in WITH.");
      }
      named_expr->name_ = std::string(ctx->getText());
      named_expr->token_position_ = static_cast<int32_t>(ctx->expression()->getStart()->getTokenIndex());
    }
    return named_expr;
  }

  /**
   * @return vector<SortItem>
   */
  antlrcpp::Any visitOrder(MemgraphCypher::OrderContext *ctx) override {
    std::vector<SortItem> order_by;
    order_by.reserve(ctx->sortItem().size());
    for (auto *sort_item : ctx->sortItem()) {
      order_by.push_back(std::any_cast<SortItem>(sort_item->accept(this)));
    }
    return order_by;
  }

  /**
   * @return SortItem
   */
  antlrcpp::Any visitSortItem(MemgraphCypher::SortItemContext *ctx) override {
    return SortItem{ctx->DESC() || ctx->DESCENDING() ? Ordering::DESC : Ordering::ASC,
                    std::any_cast<Expression *>(ctx->expression()->accept(this))};
  }

  /**
   * @return NodeAtom*
   */
  antlrcpp::Any visitNodePattern(MemgraphCypher::NodePatternContext *ctx) override {
    auto *node = storage_->Create<NodeAtom>();
    if (ctx->variable()) {
      auto variable = std::any_cast<std::string>(ctx->variable()->accept(this));
      node->identifier_ = storage_->Create<Identifier>(variable);
      users_identifiers.insert(variable);
    } else {
      anonymous_identifiers.push_back(&node->identifier_);
    }
    if (ctx->nodeLabels()) {
      node->labels_ = std::any_cast<std::vector<LabelIx>>(ctx->nodeLabels()->accept(this));
    }
    if (ctx->properties()) {
      // This can return either properties or parameters
      if (ctx->properties()->mapLiteral()) {
        node->properties_ =
            std::any_cast<std::unordered_map<PropertyIx, Expression *>>(ctx->properties()->accept(this));
      } else {
        node->properties_ = std::any_cast<ParameterLookup *>(ctx->properties()->accept(this));
      }
    }
    return node;
  }

  /**
   * @return vector<LabelIx>
   */
  antlrcpp::Any visitNodeLabels(MemgraphCypher::NodeLabelsContext *ctx) override {
    std::vector<LabelIx> labels;
    for (auto *node_label : ctx->nodeLabel()) {
      labels.push_back(AddLabel(std::any_cast<std::string>(node_label->accept(this))));
    }
    return labels;
  }

  /**
   * @return unordered_map<PropertyIx, Expression*>
   */
  antlrcpp::Any visitProperties(MemgraphCypher::PropertiesContext *ctx) override {
    if (ctx->mapLiteral()) {
      return ctx->mapLiteral()->accept(this);
    }
    // If child is not mapLiteral that means child is params.
    MG_ASSERT(ctx->parameter());
    return ctx->parameter()->accept(this);
  }

  /**
   * @return map<std::string, Expression*>
   */
  antlrcpp::Any visitMapLiteral(MemgraphCypher::MapLiteralContext *ctx) override {
    std::unordered_map<PropertyIx, Expression *> map;
    for (int i = 0; i < static_cast<int>(ctx->propertyKeyName().size()); ++i) {
      auto key = std::any_cast<PropertyIx>(ctx->propertyKeyName()[i]->accept(this));
      auto *value = std::any_cast<Expression *>(ctx->expression()[i]->accept(this));
      if (!map.insert({key, value}).second) {
        throw memgraph::expr::SemanticException("Same key can't appear twice in a map literal.");
      }
    }
    return map;
  }

  /**
   * @return vector<Expression*>
   */
  antlrcpp::Any visitListLiteral(MemgraphCypher::ListLiteralContext *ctx) override {
    std::vector<Expression *> expressions;
    for (auto *expr_ctx : ctx->expression()) {
      expressions.push_back(std::any_cast<Expression *>(expr_ctx->accept(this)));
    }
    return expressions;
  }

  /**
   * @return PropertyIx
   */
  antlrcpp::Any visitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *ctx) override {
    return AddProperty(std::any_cast<std::string>(visitChildren(ctx)));
  }

  /**
   * @return string
   */
  antlrcpp::Any visitSymbolicName(MemgraphCypher::SymbolicNameContext *ctx) override {
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

  /**
   * @return vector<Pattern*>
   */
  antlrcpp::Any visitPattern(MemgraphCypher::PatternContext *ctx) override {
    std::vector<Pattern *> patterns;
    for (auto *pattern_part : ctx->patternPart()) {
      patterns.push_back(std::any_cast<Pattern *>(pattern_part->accept(this)));
    }
    return patterns;
  }

  /**
   * @return Pattern*
   */
  antlrcpp::Any visitPatternPart(MemgraphCypher::PatternPartContext *ctx) override {
    auto *pattern = std::any_cast<Pattern *>(ctx->anonymousPatternPart()->accept(this));
    if (ctx->variable()) {
      auto variable = std::any_cast<std::string>(ctx->variable()->accept(this));
      pattern->identifier_ = storage_->Create<Identifier>(variable);
      users_identifiers.insert(variable);
    } else {
      anonymous_identifiers.push_back(&pattern->identifier_);
    }
    return pattern;
  }

  /**
   * @return Pattern*
   */
  antlrcpp::Any visitPatternElement(MemgraphCypher::PatternElementContext *ctx) override {
    if (ctx->patternElement()) {
      return ctx->patternElement()->accept(this);
    }
    auto *pattern = storage_->Create<Pattern>();
    pattern->atoms_.push_back(std::any_cast<NodeAtom *>(ctx->nodePattern()->accept(this)));
    for (auto *pattern_element_chain : ctx->patternElementChain()) {
      auto element = std::any_cast<std::pair<PatternAtom *, PatternAtom *>>(pattern_element_chain->accept(this));
      pattern->atoms_.push_back(element.first);
      pattern->atoms_.push_back(element.second);
    }
    return pattern;
  }

  /**
   * @return vector<pair<EdgeAtom*, NodeAtom*>>
   */
  antlrcpp::Any visitPatternElementChain(MemgraphCypher::PatternElementChainContext *ctx) override {
    return std::pair<PatternAtom *, PatternAtom *>(std::any_cast<EdgeAtom *>(ctx->relationshipPattern()->accept(this)),
                                                   std::any_cast<NodeAtom *>(ctx->nodePattern()->accept(this)));
  }

  /**
   *@return EdgeAtom*
   */
  antlrcpp::Any visitRelationshipPattern(MemgraphCypher::RelationshipPatternContext *ctx) override {
    auto *edge = storage_->Create<EdgeAtom>();

    auto *relationshipDetail = ctx->relationshipDetail();
    auto *variableExpansion = relationshipDetail ? relationshipDetail->variableExpansion() : nullptr;
    edge->type_ = EdgeAtom::Type::SINGLE;
    if (variableExpansion)
      std::tie(edge->type_, edge->lower_bound_, edge->upper_bound_) =
          std::any_cast<std::tuple<EdgeAtom::Type, Expression *, Expression *>>(variableExpansion->accept(this));

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
      auto variable = std::any_cast<std::string>(relationshipDetail->name->accept(this));
      edge->identifier_ = storage_->Create<Identifier>(variable);
      users_identifiers.insert(variable);
    } else {
      anonymous_identifiers.push_back(&edge->identifier_);
    }

    if (relationshipDetail->relationshipTypes()) {
      edge->edge_types_ =
          std::any_cast<std::vector<EdgeTypeIx>>(ctx->relationshipDetail()->relationshipTypes()->accept(this));
    }

    auto relationshipLambdas = relationshipDetail->relationshipLambda();
    if (variableExpansion) {
      if (relationshipDetail->total_weight && edge->type_ != EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
        throw memgraph::expr::SemanticException(
            "Variable for total weight is allowed only with weighted shortest "
            "path expansion.");
      auto visit_lambda = [this](auto *lambda) {
        EdgeAtom::Lambda edge_lambda;
        auto traversed_edge_variable = std::any_cast<std::string>(lambda->traversed_edge->accept(this));
        edge_lambda.inner_edge = storage_->Create<Identifier>(traversed_edge_variable);
        auto traversed_node_variable = std::any_cast<std::string>(lambda->traversed_node->accept(this));
        edge_lambda.inner_node = storage_->Create<Identifier>(traversed_node_variable);
        edge_lambda.expression = std::any_cast<Expression *>(lambda->expression()->accept(this));
        return edge_lambda;
      };
      auto visit_total_weight = [&]() {
        if (relationshipDetail->total_weight) {
          auto total_weight_name = std::any_cast<std::string>(relationshipDetail->total_weight->accept(this));
          edge->total_weight_ = storage_->Create<Identifier>(total_weight_name);
        } else {
          anonymous_identifiers.push_back(&edge->total_weight_);
        }
      };
      switch (relationshipLambdas.size()) {
        case 0:
          if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
            throw memgraph::expr::SemanticException(
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
            throw memgraph::expr::SemanticException("Only one filter lambda can be supplied.");
          edge->weight_lambda_ = visit_lambda(relationshipLambdas[0]);
          visit_total_weight();
          edge->filter_lambda_ = visit_lambda(relationshipLambdas[1]);
          break;
        default:
          throw memgraph::expr::SemanticException("Only one filter lambda can be supplied.");
      }
    } else if (!relationshipLambdas.empty()) {
      throw memgraph::expr::SemanticException("Filter lambda is only allowed in variable length expansion.");
    }

    auto properties = relationshipDetail->properties();
    switch (properties.size()) {
      case 0:
        break;
      case 1: {
        if (properties[0]->mapLiteral()) {
          edge->properties_ = std::any_cast<std::unordered_map<PropertyIx, Expression *>>(properties[0]->accept(this));
          break;
        }
        MG_ASSERT(properties[0]->parameter());
        edge->properties_ = std::any_cast<ParameterLookup *>(properties[0]->accept(this));
        break;
      }
      default:
        throw memgraph::expr::SemanticException("Only one property map can be supplied for edge.");
    }

    return edge;
  }

  /**
   * This should never be called. Everything is done directly in
   * visitRelationshipPattern.
   */
  antlrcpp::Any visitRelationshipDetail(MemgraphCypher::RelationshipDetailContext * /*ctx*/) override {
    DLOG_FATAL("Should never be called. See documentation in hpp.");
    return 0;
  }

  /**
   * This should never be called. Everything is done directly in
   * visitRelationshipPattern.
   */
  antlrcpp::Any visitRelationshipLambda(MemgraphCypher::RelationshipLambdaContext * /*ctx*/) override {
    DLOG_FATAL("Should never be called. See documentation in hpp.");
    return 0;
  }

  /**
   * @return vector<EdgeTypeIx>
   */
  antlrcpp::Any visitRelationshipTypes(MemgraphCypher::RelationshipTypesContext *ctx) override {
    std::vector<EdgeTypeIx> types;
    for (auto *edge_type : ctx->relTypeName()) {
      types.push_back(AddEdgeType(std::any_cast<std::string>(edge_type->accept(this))));
    }
    return types;
  }

  /**
   * @return std::tuple<EdgeAtom::Type, int64_t, int64_t>.
   */
  antlrcpp::Any visitVariableExpansion(MemgraphCypher::VariableExpansionContext *ctx) override {
    DMG_ASSERT(ctx->expression().size() <= 2U, "Expected 0, 1 or 2 bounds in range literal.");

    EdgeAtom::Type edge_type = EdgeAtom::Type::DEPTH_FIRST;
    if (!ctx->getTokens(MemgraphCypher::BFS).empty())
      edge_type = EdgeAtom::Type::BREADTH_FIRST;
    else if (!ctx->getTokens(MemgraphCypher::WSHORTEST).empty())
      edge_type = EdgeAtom::Type::WEIGHTED_SHORTEST_PATH;
    Expression *lower = nullptr;
    Expression *upper = nullptr;

    if (ctx->expression().empty()) {
      // Case -[*]-
    } else if (ctx->expression().size() == 1U) {
      auto dots_tokens = ctx->getTokens(MemgraphCypher::DOTS);
      auto *bound = std::any_cast<Expression *>(ctx->expression()[0]->accept(this));
      if (dots_tokens.empty()) {
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
      lower = std::any_cast<Expression *>(ctx->expression()[0]->accept(this));
      upper = std::any_cast<Expression *>(ctx->expression()[1]->accept(this));
    }
    if (lower && edge_type == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH)
      throw memgraph::expr::SemanticException("Lower bound is not allowed in weighted shortest path expansion.");

    return std::make_tuple(edge_type, lower, upper);
  }

  /**
   * Top level expression, does nothing.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression(MemgraphCypher::ExpressionContext *ctx) override {
    return std::any_cast<Expression *>(ctx->expression12()->accept(this));
  }

  /**
   * OR.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression12(MemgraphCypher::Expression12Context *ctx) override {
    return LeftAssociativeOperatorExpression(ctx->expression11(), ctx->children, {MemgraphCypher::OR});
  }

  /**
   * XOR.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression11(MemgraphCypher::Expression11Context *ctx) override {
    return LeftAssociativeOperatorExpression(ctx->expression10(), ctx->children, {MemgraphCypher::XOR});
  }

  /**
   * AND.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression10(MemgraphCypher::Expression10Context *ctx) override {
    return LeftAssociativeOperatorExpression(ctx->expression9(), ctx->children, {MemgraphCypher::AND});
  }

  /**
   * NOT.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression9(MemgraphCypher::Expression9Context *ctx) override {
    return PrefixUnaryOperator(ctx->expression8(), ctx->children, {MemgraphCypher::NOT});
  }

  /**
   * Comparisons.
   *
   * @return Expression*
   */
  // Comparisons.
  // Expresion 1 < 2 < 3 is converted to 1 < 2 && 2 < 3 and then binary operator
  // ast node is constructed for each operator.
  antlrcpp::Any visitExpression8(MemgraphCypher::Expression8Context *ctx) override {
    if (ctx->partialComparisonExpression().empty()) {
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
    children.push_back(std::any_cast<Expression *>(ctx->expression7()->accept(this)));
    auto partial_comparison_expressions = ctx->partialComparisonExpression();
    for (auto *child : partial_comparison_expressions) {
      children.push_back(std::any_cast<Expression *>(child->expression7()->accept(this)));
    }
    // First production is comparison operator.
    std::vector<size_t> operators;
    operators.reserve(partial_comparison_expressions.size());
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

  /**
   * Never call this. Everything related to generating code for comparison
   * operators should be done in visitExpression8.
   */
  antlrcpp::Any visitPartialComparisonExpression(
      MemgraphCypher::PartialComparisonExpressionContext * /*ctx*/) override {
    DLOG_FATAL("Should never be called. See documentation in hpp.");
    return 0;
  }

  /**
   * Addition and subtraction.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression7(MemgraphCypher::Expression7Context *ctx) override {
    return LeftAssociativeOperatorExpression(ctx->expression6(), ctx->children,
                                             {MemgraphCypher::PLUS, MemgraphCypher::MINUS});
  }

  /**
   * Multiplication, division, modding.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression6(MemgraphCypher::Expression6Context *ctx) override {
    return LeftAssociativeOperatorExpression(
        ctx->expression5(), ctx->children, {MemgraphCypher::ASTERISK, MemgraphCypher::SLASH, MemgraphCypher::PERCENT});
  }

  /**
   * Power.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression5(MemgraphCypher::Expression5Context *ctx) override {
    if (ctx->expression4().size() > 1U) {
      // TODO: implement power operator. In neo4j power is left associative and
      // int^int -> float.
      throw utils::NotYetImplemented("power (^) operator");
    }
    return visitChildren(ctx);
  }

  /**
   * Unary minus and plus.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression4(MemgraphCypher::Expression4Context *ctx) override {
    return PrefixUnaryOperator(ctx->expression3a(), ctx->children, {MemgraphCypher::PLUS, MemgraphCypher::MINUS});
  }

  /**
   * IS NULL, IS NOT NULL, STARTS WITH, END WITH, =~, ...
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression3a(MemgraphCypher::Expression3aContext *ctx) override {
    auto *expression = std::any_cast<Expression *>(ctx->expression3b()->accept(this));

    for (auto *op : ctx->stringAndNullOperators()) {
      if (op->IS() && op->NOT() && op->CYPHERNULL()) {
        expression =
            static_cast<Expression *>(storage_->Create<NotOperator>(storage_->Create<IsNullOperator>(expression)));
      } else if (op->IS() && op->CYPHERNULL()) {
        expression = static_cast<Expression *>(storage_->Create<IsNullOperator>(expression));
      } else if (op->IN()) {
        expression = static_cast<Expression *>(storage_->Create<InListOperator>(
            expression, std::any_cast<Expression *>(op->expression3b()->accept(this))));
      } else if (utils::StartsWith(op->getText(), "=~")) {
        auto *regex_match = storage_->Create<RegexMatch>();
        regex_match->string_expr_ = expression;
        regex_match->regex_ = std::any_cast<Expression *>(op->expression3b()->accept(this));
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
        auto *expression2 = std::any_cast<Expression *>(op->expression3b()->accept(this));
        std::vector<Expression *> args = {expression, expression2};
        expression = static_cast<Expression *>(storage_->Create<Function>(function_name, args));
      }
    }
    return expression;
  }

  /**
   * Does nothing, everything is done in visitExpression3a.
   *
   * @return Expression*
   */
  antlrcpp::Any visitStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext * /*fctx*/) override {
    DLOG_FATAL("Should never be called. See documentation in hpp.");
    return 0;
  }

  /**
   * List indexing and slicing.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression3b(MemgraphCypher::Expression3bContext *ctx) override {
    auto *expression = std::any_cast<Expression *>(ctx->expression2a()->accept(this));
    for (auto *list_op : ctx->listIndexingOrSlicing()) {
      if (list_op->getTokens(MemgraphCypher::DOTS).empty()) {
        // If there is no '..' then we need to create list indexing operator.
        expression = storage_->Create<SubscriptOperator>(
            expression, std::any_cast<Expression *>(list_op->expression()[0]->accept(this)));
      } else if (!list_op->lower_bound && !list_op->upper_bound) {
        throw memgraph::expr::SemanticException("List slicing operator requires at least one bound.");
      } else {
        Expression *lower_bound_ast =
            list_op->lower_bound ? std::any_cast<Expression *>(list_op->lower_bound->accept(this)) : nullptr;
        Expression *upper_bound_ast =
            list_op->upper_bound ? std::any_cast<Expression *>(list_op->upper_bound->accept(this)) : nullptr;
        expression = storage_->Create<ListSlicingOperator>(expression, lower_bound_ast, upper_bound_ast);
      }
    }
    return expression;
  }

  /**
   * Does nothing, everything is done in visitExpression3b.
   */
  antlrcpp::Any visitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext * /*ctx*/) override {
    DLOG_FATAL("Should never be called. See documentation in hpp.");
    return 0;
  }

  /**
   * Node labels test.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression2a(MemgraphCypher::Expression2aContext *ctx) override {
    auto *expression = std::any_cast<Expression *>(ctx->expression2b()->accept(this));
    if (ctx->nodeLabels()) {
      auto labels = std::any_cast<std::vector<LabelIx>>(ctx->nodeLabels()->accept(this));
      expression = storage_->Create<LabelsTest>(expression, labels);
    }
    return expression;
  }

  /**
   * Property lookup.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression2b(MemgraphCypher::Expression2bContext *ctx) override {
    auto *expression = std::any_cast<Expression *>(ctx->atom()->accept(this));
    for (auto *lookup : ctx->propertyLookup()) {
      auto key = std::any_cast<PropertyIx>(lookup->accept(this));
      auto *property_lookup = storage_->Create<PropertyLookup>(expression, key);
      expression = property_lookup;
    }
    return expression;
  }

  /**
   * Literals, params, list comprehension...
   *
   * @return Expression*
   */
  antlrcpp::Any visitAtom(MemgraphCypher::AtomContext *ctx) override {
    if (ctx->literal()) {
      return ctx->literal()->accept(this);
    }
    if (ctx->parameter()) {
      return static_cast<Expression *>(std::any_cast<ParameterLookup *>(ctx->parameter()->accept(this)));
    }
    if (ctx->parenthesizedExpression()) {
      return static_cast<Expression *>(std::any_cast<Expression *>(ctx->parenthesizedExpression()->accept(this)));
    }
    if (ctx->variable()) {
      auto variable = std::any_cast<std::string>(ctx->variable()->accept(this));
      users_identifiers.insert(variable);
      return static_cast<Expression *>(storage_->Create<Identifier>(variable));
    }
    if (ctx->functionInvocation()) {
      return std::any_cast<Expression *>(ctx->functionInvocation()->accept(this));
    }
    if (ctx->COALESCE()) {
      std::vector<Expression *> exprs;
      for (auto *expr_context : ctx->expression()) {
        exprs.emplace_back(std::any_cast<Expression *>(expr_context->accept(this)));
      }
      return static_cast<Expression *>(storage_->Create<Coalesce>(std::move(exprs)));
    }
    if (ctx->COUNT()) {
      // Here we handle COUNT(*). COUNT(expression) is handled in
      // visitFunctionInvocation with other aggregations. This is visible in
      // functionInvocation and atom producions in opencypher grammar.
      return static_cast<Expression *>(storage_->Create<Aggregation>(nullptr, nullptr, Aggregation::Op::COUNT));
    }
    if (ctx->ALL()) {
      auto *ident = storage_->Create<Identifier>(
          std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
      auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
      if (!ctx->filterExpression()->where()) {
        throw memgraph::expr::SyntaxException("ALL(...) requires a WHERE predicate.");
      }
      auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
      return static_cast<Expression *>(storage_->Create<All>(ident, list_expr, where));
    }
    if (ctx->SINGLE()) {
      auto *ident = storage_->Create<Identifier>(
          std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
      auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
      if (!ctx->filterExpression()->where()) {
        throw memgraph::expr::SyntaxException("SINGLE(...) requires a WHERE predicate.");
      }
      auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
      return static_cast<Expression *>(storage_->Create<Single>(ident, list_expr, where));
    }
    if (ctx->ANY()) {
      auto *ident = storage_->Create<Identifier>(
          std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
      auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
      if (!ctx->filterExpression()->where()) {
        throw memgraph::expr::SyntaxException("ANY(...) requires a WHERE predicate.");
      }
      auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
      return static_cast<Expression *>(storage_->Create<Any>(ident, list_expr, where));
    }
    if (ctx->NONE()) {
      auto *ident = storage_->Create<Identifier>(
          std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
      auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
      if (!ctx->filterExpression()->where()) {
        throw memgraph::expr::SyntaxException("NONE(...) requires a WHERE predicate.");
      }
      auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
      return static_cast<Expression *>(storage_->Create<None>(ident, list_expr, where));
    }
    if (ctx->REDUCE()) {
      auto *accumulator =
          storage_->Create<Identifier>(std::any_cast<std::string>(ctx->reduceExpression()->accumulator->accept(this)));
      auto *initializer = std::any_cast<Expression *>(ctx->reduceExpression()->initial->accept(this));
      auto *ident = storage_->Create<Identifier>(
          std::any_cast<std::string>(ctx->reduceExpression()->idInColl()->variable()->accept(this)));
      auto *list = std::any_cast<Expression *>(ctx->reduceExpression()->idInColl()->expression()->accept(this));
      auto *expr = std::any_cast<Expression *>(ctx->reduceExpression()->expression().back()->accept(this));
      return static_cast<Expression *>(storage_->Create<Reduce>(accumulator, initializer, ident, list, expr));
    }
    if (ctx->caseExpression()) {
      return std::any_cast<Expression *>(ctx->caseExpression()->accept(this));
    }
    if (ctx->extractExpression()) {
      auto *ident = storage_->Create<Identifier>(
          std::any_cast<std::string>(ctx->extractExpression()->idInColl()->variable()->accept(this)));
      auto *list = std::any_cast<Expression *>(ctx->extractExpression()->idInColl()->expression()->accept(this));
      auto *expr = std::any_cast<Expression *>(ctx->extractExpression()->expression()->accept(this));
      return static_cast<Expression *>(storage_->Create<Extract>(ident, list, expr));
    }
    // TODO: Implement this. We don't support comprehensions, filtering... at
    // the moment.
    throw utils::NotYetImplemented("atom expression '{}'", ctx->getText());
  }

  /**
   * @return ParameterLookup*
   */
  antlrcpp::Any visitParameter(MemgraphCypher::ParameterContext *ctx) override {
    return storage_->Create<ParameterLookup>(ctx->getStart()->getTokenIndex());
  }

  /**
   * @return Expression*
   */
  antlrcpp::Any visitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *ctx) override {
    return std::any_cast<Expression *>(ctx->expression()->accept(this));
  }

  /**
   * @return Expression*
   */
  antlrcpp::Any visitFunctionInvocation(MemgraphCypher::FunctionInvocationContext *ctx) override {
    if (ctx->DISTINCT()) {
      throw utils::NotYetImplemented("DISTINCT function call");
    }
    auto function_name = std::any_cast<std::string>(ctx->functionName()->accept(this));
    std::vector<Expression *> expressions;
    for (auto *expression : ctx->expression()) {
      expressions.push_back(std::any_cast<Expression *>(expression->accept(this)));
    }
    if (expressions.size() == 1U) {
      if (function_name == Aggregation::kCount) {
        return static_cast<Expression *>(
            storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::COUNT));
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

    auto is_user_defined_function = [](const std::string &function_name) {
      // Dots are present only in user-defined functions, since modules are case-sensitive, so must be user-defined
      // functions. Builtin functions should be case insensitive.
      return function_name.find('.') != std::string::npos;
    };

    // Don't cache queries which call user-defined functions. User-defined function's return
    // types can vary depending on whether the module is reloaded, therefore the cache would
    // be invalid.
    if (is_user_defined_function(function_name)) {
      throw utils::NotYetImplemented("User defined functions not allowed");
      query_info_.is_cacheable = false;
    }

    return static_cast<Expression *>(storage_->Create<Function>(function_name, expressions));
  }

  /**
   * @return string - uppercased
   */
  antlrcpp::Any visitFunctionName(MemgraphCypher::FunctionNameContext *ctx) override {
    auto function_name = ctx->getText();
    // TODO(kostasrim) Add user defined functions request
    //
    // Dots are present only in user-defined functions, since modules are case-sensitive, so must be user-defined
    // functions. Builtin functions should be case insensitive.
    // if (function_name.find('.') != std::string::npos) {
    //  return function_name;
    //}
    return utils::ToUpperCase(function_name);
  }

  /**
   * @return Expression*
   */
  antlrcpp::Any visitLiteral(MemgraphCypher::LiteralContext *ctx) override {
    if (ctx->CYPHERNULL() || ctx->StringLiteral() || ctx->booleanLiteral() || ctx->numberLiteral()) {
      int token_position = static_cast<int>(ctx->getStart()->getTokenIndex());
      if (ctx->CYPHERNULL()) {
        return static_cast<Expression *>(storage_->Create<PrimitiveLiteral>(TypedValue(), token_position));
      }
      if (context_.is_query_cached) {
        // Instead of generating PrimitiveLiteral, we generate a
        // ParameterLookup, so that the AST can be cached. This allows for
        // varying literals, which are then looked up in the parameters table
        // (even though they are not user provided). Note, that NULL always
        // generates a PrimitiveLiteral.
        return static_cast<Expression *>(storage_->Create<ParameterLookup>(token_position));
      }
      if (ctx->StringLiteral()) {
        return static_cast<Expression *>(storage_->Create<PrimitiveLiteral>(
            std::any_cast<std::string>(visitStringLiteral(std::any_cast<std::string>(ctx->StringLiteral()->getText()))),
            token_position));
      }
      if (ctx->booleanLiteral()) {
        return static_cast<Expression *>(storage_->Create<PrimitiveLiteral>(
            std::any_cast<bool>(ctx->booleanLiteral()->accept(this)), token_position));
      }
      if (ctx->numberLiteral()) {
        return static_cast<Expression *>(storage_->Create<PrimitiveLiteral>(
            std::any_cast<TypedValue>(ctx->numberLiteral()->accept(this)), token_position));
      }
      LOG_FATAL("Expected to handle all cases above");
    }
    if (ctx->listLiteral()) {
      return static_cast<Expression *>(
          storage_->Create<ListLiteral>(std::any_cast<std::vector<Expression *>>(ctx->listLiteral()->accept(this))));
    }
    return static_cast<Expression *>(storage_->Create<MapLiteral>(
        std::any_cast<std::unordered_map<PropertyIx, Expression *>>(ctx->mapLiteral()->accept(this))));
  }

  /**
   * Convert escaped string from a query to unescaped utf8 string.
   *
   * @return string
   */
  inline static antlrcpp::Any visitStringLiteral(const std::string &escaped) {
    return expr::ParseStringLiteral(escaped);
  }

  /**
   * @return bool
   */
  antlrcpp::Any visitBooleanLiteral(MemgraphCypher::BooleanLiteralContext *ctx) override {
    if (!ctx->getTokens(MemgraphCypher::TRUE).empty()) {
      return true;
    }
    if (!ctx->getTokens(MemgraphCypher::FALSE).empty()) {
      return false;
    }
    DLOG_FATAL("Shouldn't happend");
    throw std::exception();
  }

  /**
   * @return TypedValue with either double or int
   */
  antlrcpp::Any visitNumberLiteral(MemgraphCypher::NumberLiteralContext *ctx) override {
    if (ctx->integerLiteral()) {
      return TypedValue(std::any_cast<int64_t>(ctx->integerLiteral()->accept(this)));
    }
    if (ctx->doubleLiteral()) {
      return TypedValue(std::any_cast<double>(ctx->doubleLiteral()->accept(this)));
    }
    // This should never happen, except grammar changes and we don't notice
    // change in this production.
    DLOG_FATAL("can't happen");
    throw std::exception();
  }

  /**
   * @return int64_t
   */
  antlrcpp::Any visitIntegerLiteral(MemgraphCypher::IntegerLiteralContext *ctx) override {
    return expr::ParseIntegerLiteral(ctx->getText());
  }

  /**
   * @return double
   */
  antlrcpp::Any visitDoubleLiteral(MemgraphCypher::DoubleLiteralContext *ctx) override {
    return expr::ParseDoubleLiteral(ctx->getText());
  }

  /**
   * @return Delete*
   */
  antlrcpp::Any visitCypherDelete(MemgraphCypher::CypherDeleteContext *ctx) override {
    auto *del = storage_->Create<Delete>();
    if (ctx->DETACH()) {
      del->detach_ = true;
    }
    for (auto *expression : ctx->expression()) {
      del->expressions_.push_back(std::any_cast<Expression *>(expression->accept(this)));
    }
    return del;
  }

  /**
   * @return Where*
   */
  antlrcpp::Any visitWhere(MemgraphCypher::WhereContext *ctx) override {
    auto *where = storage_->Create<Where>();
    where->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
    return where;
  }

  /**
   * return vector<Clause*>
   */
  antlrcpp::Any visitSet(MemgraphCypher::SetContext *ctx) override {
    std::vector<Clause *> set_items;
    for (auto *set_item : ctx->setItem()) {
      set_items.push_back(std::any_cast<Clause *>(set_item->accept(this)));
    }
    return set_items;
  }

  /**
   * @return Clause*
   */
  antlrcpp::Any visitSetItem(MemgraphCypher::SetItemContext *ctx) override {
    // SetProperty
    if (ctx->propertyExpression()) {
      auto *set_property = storage_->Create<SetProperty>();
      set_property->property_lookup_ = std::any_cast<PropertyLookup *>(ctx->propertyExpression()->accept(this));
      set_property->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
      return static_cast<Clause *>(set_property);
    }

    // SetProperties either assignment or update
    if (!ctx->getTokens(MemgraphCypher::EQ).empty() || !ctx->getTokens(MemgraphCypher::PLUS_EQ).empty()) {
      auto *set_properties = storage_->Create<SetProperties>();
      set_properties->identifier_ =
          storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
      set_properties->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
      if (!ctx->getTokens(MemgraphCypher::PLUS_EQ).empty()) {
        set_properties->update_ = true;
      }
      return static_cast<Clause *>(set_properties);
    }

    // SetLabels
    auto *set_labels = storage_->Create<SetLabels>();
    set_labels->identifier_ = storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
    set_labels->labels_ = std::any_cast<std::vector<LabelIx>>(ctx->nodeLabels()->accept(this));
    return static_cast<Clause *>(set_labels);
  }

  /**
   * return vector<Clause*>
   */
  antlrcpp::Any visitRemove(MemgraphCypher::RemoveContext *ctx) override {
    std::vector<Clause *> remove_items;
    for (auto *remove_item : ctx->removeItem()) {
      remove_items.push_back(std::any_cast<Clause *>(remove_item->accept(this)));
    }
    return remove_items;
  }

  /**
   * @return Clause*
   */
  antlrcpp::Any visitRemoveItem(MemgraphCypher::RemoveItemContext *ctx) override {
    // RemoveProperty
    if (ctx->propertyExpression()) {
      auto *remove_property = storage_->Create<RemoveProperty>();
      remove_property->property_lookup_ = std::any_cast<PropertyLookup *>(ctx->propertyExpression()->accept(this));
      return static_cast<Clause *>(remove_property);
    }

    // RemoveLabels
    auto *remove_labels = storage_->Create<RemoveLabels>();
    remove_labels->identifier_ =
        storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
    remove_labels->labels_ = std::any_cast<std::vector<LabelIx>>(ctx->nodeLabels()->accept(this));
    return static_cast<Clause *>(remove_labels);
  }

  /**
   * @return PropertyLookup*
   */
  antlrcpp::Any visitPropertyExpression(MemgraphCypher::PropertyExpressionContext *ctx) override {
    auto *expression = std::any_cast<Expression *>(ctx->atom()->accept(this));
    for (auto *lookup : ctx->propertyLookup()) {
      auto key = std::any_cast<PropertyIx>(lookup->accept(this));
      auto *property_lookup = storage_->Create<PropertyLookup>(expression, key);
      expression = property_lookup;
    }
    // It is guaranteed by grammar that there is at least one propertyLookup.
    return static_cast<PropertyLookup *>(expression);
  }

  /**
   * @return IfOperator*
   */
  antlrcpp::Any visitCaseExpression(MemgraphCypher::CaseExpressionContext *ctx) override {
    Expression *test_expression = ctx->test ? std::any_cast<Expression *>(ctx->test->accept(this)) : nullptr;
    auto alternatives = ctx->caseAlternatives();
    // Reverse alternatives so that tree of IfOperators can be built bottom-up.
    std::reverse(alternatives.begin(), alternatives.end());
    Expression *else_expression = ctx->else_expression ? std::any_cast<Expression *>(ctx->else_expression->accept(this))
                                                       : storage_->Create<PrimitiveLiteral>(TypedValue());
    for (auto *alternative : alternatives) {
      Expression *condition =
          test_expression
              ? storage_->Create<EqualOperator>(test_expression,
                                                std::any_cast<Expression *>(alternative->when_expression->accept(this)))
              : std::any_cast<Expression *>(alternative->when_expression->accept(this));
      auto *then_expression = std::any_cast<Expression *>(alternative->then_expression->accept(this));
      else_expression = storage_->Create<IfOperator>(condition, then_expression, else_expression);
    }
    return else_expression;
  }

  /**
   * Never call this. Ast generation for this production is done in
   * @c visitCaseExpression.
   */
  antlrcpp::Any visitCaseAlternatives(MemgraphCypher::CaseAlternativesContext * /*ctx*/) override {
    DLOG_FATAL("Should never be called. See documentation in hpp.");
    return 0;
  }

  /**
   * @return With*
   */
  antlrcpp::Any visitWith(MemgraphCypher::WithContext *ctx) override {
    auto *with = storage_->Create<With>();
    in_with_ = true;
    with->body_ = std::any_cast<ReturnBody>(ctx->returnBody()->accept(this));
    in_with_ = false;
    if (ctx->DISTINCT()) {
      with->body_.distinct = true;
    }
    if (ctx->where()) {
      with->where_ = std::any_cast<Where *>(ctx->where()->accept(this));
    }
    return with;
  }

  /**
   * @return Merge*
   */
  antlrcpp::Any visitMerge(MemgraphCypher::MergeContext *ctx) override {
    auto *merge = storage_->Create<Merge>();
    merge->pattern_ = std::any_cast<Pattern *>(ctx->patternPart()->accept(this));
    for (auto &merge_action : ctx->mergeAction()) {
      auto set = std::any_cast<std::vector<Clause *>>(merge_action->set()->accept(this));
      if (merge_action->MATCH()) {
        merge->on_match_.insert(merge->on_match_.end(), set.begin(), set.end());
      } else {
        DMG_ASSERT(merge_action->CREATE(), "Expected ON MATCH or ON CREATE");
        merge->on_create_.insert(merge->on_create_.end(), set.begin(), set.end());
      }
    }
    return merge;
  }

  /**
   * @return Unwind*
   */
  antlrcpp::Any visitUnwind(MemgraphCypher::UnwindContext *ctx) override {
    auto *named_expr = storage_->Create<NamedExpression>();
    named_expr->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
    named_expr->name_ = std::any_cast<std::string>(ctx->variable()->accept(this));
    return storage_->Create<Unwind>(named_expr);
  }

  /**
   * Never call this. Ast generation for these expressions should be done by
   * explicitly visiting the members of @c FilterExpressionContext.
   */
  antlrcpp::Any visitFilterExpression(MemgraphCypher::FilterExpressionContext * /*ctx*/) override {
    LOG_FATAL("Should never be called. See documentation in hpp.");
    return 0;
  }

  /**
   * @return Foreach*
   */
  antlrcpp::Any visitForeach(MemgraphCypher::ForeachContext *ctx) override {
    auto *for_each = storage_->Create<Foreach>();

    auto *named_expr = storage_->Create<NamedExpression>();
    named_expr->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
    named_expr->name_ = std::any_cast<std::string>(ctx->variable()->accept(this));
    for_each->named_expression_ = named_expr;

    for (auto *update_clause_ctx : ctx->updateClause()) {
      if (auto *set = update_clause_ctx->set(); set) {
        auto set_items = std::any_cast<std::vector<Clause *>>(visitSet(set));
        std::copy(set_items.begin(), set_items.end(), std::back_inserter(for_each->clauses_));
      } else if (auto *remove = update_clause_ctx->remove(); remove) {
        auto remove_items = std::any_cast<std::vector<Clause *>>(visitRemove(remove));
        std::copy(remove_items.begin(), remove_items.end(), std::back_inserter(for_each->clauses_));
      } else if (auto *merge = update_clause_ctx->merge(); merge) {
        for_each->clauses_.push_back(std::any_cast<Merge *>(visitMerge(merge)));
      } else if (auto *create = update_clause_ctx->create(); create) {
        for_each->clauses_.push_back(std::any_cast<Create *>(visitCreate(create)));
      } else if (auto *cypher_delete = update_clause_ctx->cypherDelete(); cypher_delete) {
        for_each->clauses_.push_back(std::any_cast<Delete *>(visitCypherDelete(cypher_delete)));
      } else {
        auto *nested_for_each = update_clause_ctx->foreach ();
        MG_ASSERT(nested_for_each != nullptr, "Unexpected clause in FOREACH");
        for_each->clauses_.push_back(std::any_cast<Foreach *>(visitForeach(nested_for_each)));
      }
    }

    return for_each;
  }

  /**
   * @return Schema*
   */
  antlrcpp::Any visitPropertyType(MemgraphCypher::PropertyTypeContext *ctx) override {
    MG_ASSERT(ctx->symbolicName());
    const auto property_type = utils::ToLowerCase(std::any_cast<std::string>(ctx->symbolicName()->accept(this)));
    if (property_type == "bool") {
      return common::SchemaType::BOOL;
    }
    if (property_type == "string") {
      return common::SchemaType::STRING;
    }
    if (property_type == "integer") {
      return common::SchemaType::INT;
    }
    if (property_type == "date") {
      return common::SchemaType::DATE;
    }
    if (property_type == "duration") {
      return common::SchemaType::DURATION;
    }
    if (property_type == "localdatetime") {
      return common::SchemaType::LOCALDATETIME;
    }
    if (property_type == "localtime") {
      return common::SchemaType::LOCALTIME;
    }
    throw memgraph::expr::SyntaxException("Property type must be one of the supported types!");
  }

  /**
   * @return Schema*
   */
  antlrcpp::Any visitSchemaPropertyMap(MemgraphCypher::SchemaPropertyMapContext *ctx) override {
    std::vector<std::pair<PropertyIx, common::SchemaType>> schema_property_map;
    for (auto *property_key_pair : ctx->propertyKeyTypePair()) {
      auto key = std::any_cast<PropertyIx>(property_key_pair->propertyKeyName()->accept(this));
      auto type = std::any_cast<common::SchemaType>(property_key_pair->propertyType()->accept(this));
      if (std::ranges::find_if(schema_property_map, [&key](const auto &elem) { return elem.first == key; }) !=
          schema_property_map.end()) {
        throw memgraph::expr::SemanticException("Same property name can't appear twice in a schema map.");
      }
      schema_property_map.emplace_back(key, type);
    }
    return schema_property_map;
  }

  /**
   * @return Schema*
   */
  antlrcpp::Any visitSchemaQuery(MemgraphCypher::SchemaQueryContext *ctx) override {
    MG_ASSERT(ctx->children.size() == 1, "SchemaQuery should have exactly one child!");
    auto *schema_query = std::any_cast<SchemaQuery *>(ctx->children[0]->accept(this));
    query_ = schema_query;
    return schema_query;
  }

  /**
   * @return Schema*
   */
  antlrcpp::Any visitShowSchema(MemgraphCypher::ShowSchemaContext *ctx) override {
    auto *schema_query = storage_->Create<SchemaQuery>();
    schema_query->action_ = SchemaQuery::Action::SHOW_SCHEMA;
    schema_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
    query_ = schema_query;
    return schema_query;
  }

  /**
   * @return Schema*
   */
  antlrcpp::Any visitShowSchemas(MemgraphCypher::ShowSchemasContext * /*ctx*/) override {
    auto *schema_query = storage_->Create<SchemaQuery>();
    schema_query->action_ = SchemaQuery::Action::SHOW_SCHEMAS;
    query_ = schema_query;
    return schema_query;
  }

  /**
   * @return Schema*
   */
  antlrcpp::Any visitCreateSchema(MemgraphCypher::CreateSchemaContext *ctx) override {
    auto *schema_query = storage_->Create<SchemaQuery>();
    schema_query->action_ = SchemaQuery::Action::CREATE_SCHEMA;
    schema_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
    schema_query->schema_type_map_ =
        std::any_cast<std::vector<std::pair<PropertyIx, common::SchemaType>>>(ctx->schemaPropertyMap()->accept(this));
    query_ = schema_query;
    return schema_query;
  }

  /**
   * @return Schema*
   */
  antlrcpp::Any visitDropSchema(MemgraphCypher::DropSchemaContext *ctx) override {
    auto *schema_query = storage_->Create<SchemaQuery>();
    schema_query->action_ = SchemaQuery::Action::DROP_SCHEMA;
    schema_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
    query_ = schema_query;
    return schema_query;
  }

 public:
  Query *query() { return query_; }
  inline const static std::string kAnonPrefix = "anon";

  struct QueryInfo {
    bool is_cacheable{true};
    bool has_load_csv{false};
  };

  const auto &GetQueryInfo() const { return query_info_; }

 private:
  LabelIx AddLabel(const std::string &name) { return storage_->GetLabelIx(name); }

  PropertyIx AddProperty(const std::string &name) { return storage_->GetPropertyIx(name); }

  EdgeTypeIx AddEdgeType(const std::string &name) { return storage_->GetEdgeTypeIx(name); }

  ParsingContext context_;
  AstStorage *storage_;

  std::unordered_map<uint8_t, std::variant<Expression *, std::string, std::vector<std::string>,
                                           std::unordered_map<Expression *, Expression *>>>
      memory_;
  // Set of identifiers from queries.
  std::unordered_set<std::string> users_identifiers;
  // Identifiers that user didn't name.
  std::vector<Identifier **> anonymous_identifiers;
  Query *query_ = nullptr;
  // All return items which are not variables must be aliased in with.
  // We use this variable in visitReturnItem to check if we are in with or
  // return.
  bool in_with_ = false;

  QueryInfo query_info_;
};
}  // namespace MG_INJECTED_NAMESPACE_NAME
