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

#include <algorithm>
#include <any>
#include <cstring>
#include <iterator>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <boost/preprocessor/cat.hpp>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/parsing.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/procedure/callable_alias_mapper.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "query/user_profile.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query::frontend {

namespace r = ranges;
// Need to use std::ranges::views transform because rv::transform does not
// support rvalues as viewable ranges on the lhs of a pipe.
namespace srv = std::ranges::views;

const std::string CypherMainVisitor::kAnonPrefix = "anon";

namespace {
enum class EntityType : uint8_t { LABELS, EDGE_TYPES };

template <typename TVisitor>
std::optional<std::pair<memgraph::query::Expression *, size_t>> VisitMemoryLimit(
    MemgraphCypher::MemoryLimitContext *memory_limit_ctx, TVisitor *visitor) {
  MG_ASSERT(memory_limit_ctx);
  if (memory_limit_ctx->UNLIMITED()) {
    return std::nullopt;
  }

  auto *memory_limit = std::any_cast<Expression *>(memory_limit_ctx->literal()->accept(visitor));
  size_t memory_scale = 1024U;
  if (memory_limit_ctx->MB()) {
    memory_scale = 1024U * 1024U;
  } else {
    MG_ASSERT(memory_limit_ctx->KB());
    memory_scale = 1024U;
  }

  return std::make_pair(memory_limit, memory_scale);
}

template <typename TVisitor>
UserProfileQuery::LimitValueResult VisitLimitValue(MemgraphCypher::LimitValueContext *limit_ctx, TVisitor *visitor) {
  MG_ASSERT(limit_ctx);
  UserProfileQuery::LimitValueResult result;

  if (limit_ctx->UNLIMITED()) {
    result.type = UserProfileQuery::LimitValueResult::Type::UNLIMITED;
  } else if (limit_ctx->mem_limit) {
    auto *memory_limit_ctx = limit_ctx->mem_limit;
    auto *memory_limit = std::any_cast<Expression *>(memory_limit_ctx->literal()->accept(visitor));
    size_t memory_scale = 1024U;
    if (memory_limit_ctx->MB()) {
      memory_scale = 1024UL * 1024UL;
    } else {
      MG_ASSERT(memory_limit_ctx->KB());
      memory_scale = 1024UL;
    }
    result.mem_limit.type = UserProfileQuery::LimitValueResult::Type::MEMORY_LIMIT;
    result.mem_limit.expr = memory_limit;
    result.mem_limit.scale = memory_scale;
  } else if (limit_ctx->quantity) {
    auto *quantity_limit_ctx = limit_ctx->quantity;
    auto *quantity = std::any_cast<Expression *>(quantity_limit_ctx->accept(visitor));
    result.quantity.type = UserProfileQuery::LimitValueResult::Type::QUANTITY;
    result.quantity.expr = quantity;
  }

  return result;
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
      symbolicNames, [&](auto *token) { return std::any_cast<std::string>(token->accept(visitor)); }, separator);
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
  auto *cypher_query = std::any_cast<CypherQuery *>(ctx->children[1]->accept(this));
  auto *explain_query = storage_->Create<ExplainQuery>();
  explain_query->cypher_query_ = cypher_query;
  query_ = explain_query;
  return explain_query;
}

antlrcpp::Any CypherMainVisitor::visitProfileQuery(MemgraphCypher::ProfileQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 2, "ProfileQuery should have exactly two children!");
  auto *cypher_query = std::any_cast<CypherQuery *>(ctx->children[1]->accept(this));
  auto *profile_query = storage_->Create<ProfileQuery>();
  profile_query->cypher_query_ = cypher_query;
  query_ = profile_query;
  return profile_query;
}

antlrcpp::Any CypherMainVisitor::visitDatabaseInfoQuery(MemgraphCypher::DatabaseInfoQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 2, "DatabaseInfoQuery should have exactly two children!");
  auto *info_query = storage_->Create<DatabaseInfoQuery>();
  query_ = info_query;
  if (ctx->indexInfo()) {
    info_query->info_type_ = DatabaseInfoQuery::InfoType::INDEX;
    return info_query;
  }
  if (ctx->constraintInfo()) {
    info_query->info_type_ = DatabaseInfoQuery::InfoType::CONSTRAINT;
    return info_query;
  }
  if (ctx->edgetypeInfo()) {
    info_query->info_type_ = DatabaseInfoQuery::InfoType::EDGE_TYPES;
    return info_query;
  }
  if (ctx->nodelabelInfo()) {
    info_query->info_type_ = DatabaseInfoQuery::InfoType::NODE_LABELS;
    return info_query;
  }
  if (ctx->metricsInfo()) {
    info_query->info_type_ = DatabaseInfoQuery::InfoType::METRICS;
    return info_query;
  }
  if (ctx->vectorIndexInfo()) {
    info_query->info_type_ = DatabaseInfoQuery::InfoType::VECTOR_INDEX;
    return info_query;
  }
  // Should never get here
  throw utils::NotYetImplemented("Database info query: '{}'", ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitSystemInfoQuery(MemgraphCypher::SystemInfoQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 2, "SystemInfoQuery should have exactly two children!");
  auto *info_query = storage_->Create<SystemInfoQuery>();
  query_ = info_query;
  if (ctx->storageInfo()) {
    info_query->info_type_ = SystemInfoQuery::InfoType::STORAGE;
    return info_query;
  }
  if (ctx->buildInfo()) {
    info_query->info_type_ = SystemInfoQuery::InfoType::BUILD;
    return info_query;
  }
  if (ctx->activeUsersInfo()) {
    info_query->info_type_ = SystemInfoQuery::InfoType::ACTIVE_USERS;
    return info_query;
  }
  if (ctx->licenseInfo()) {
    info_query->info_type_ = SystemInfoQuery::InfoType::LICENSE;
    return info_query;
  }
  // Should never get here
  throw utils::NotYetImplemented("System info query: '{}'", ctx->getText());
}

antlrcpp::Any CypherMainVisitor::visitConstraintQuery(MemgraphCypher::ConstraintQueryContext *ctx) {
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

antlrcpp::Any CypherMainVisitor::visitConstraint(MemgraphCypher::ConstraintContext *ctx) {
  Constraint constraint;
  MG_ASSERT(ctx->EXISTS() || ctx->UNIQUE() || (ctx->NODE() && ctx->KEY()) || (ctx->IS() && ctx->TYPED()));
  if (ctx->EXISTS()) {
    constraint.type = Constraint::Type::EXISTS;
  } else if (ctx->UNIQUE()) {
    constraint.type = Constraint::Type::UNIQUE;
  } else if (ctx->NODE() && ctx->KEY()) {
    constraint.type = Constraint::Type::NODE_KEY;
  } else if (ctx->IS() && ctx->TYPED()) {
    constraint.type = Constraint::Type::TYPE;
    constraint.type_constraint =
        std::any_cast<memgraph::storage::TypeConstraintKind>(visitTypeConstraintType(ctx->typeConstraintType()));
  }
  constraint.label = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  auto node_name = std::any_cast<std::string>(ctx->nodeName->symbolicName()->accept(this));

  auto check_equality = [](auto const &variable_name, auto const &node_name) {
    if (variable_name != node_name) {
      throw SemanticException("All constraint variable should reference node '{}'", node_name);
    }
  };

  if (constraint.type != Constraint::Type::TYPE) {
    for (const auto &var_ctx : ctx->constraintPropertyList()->variable()) {
      auto var_name = std::any_cast<std::string>(var_ctx->symbolicName()->accept(this));
      check_equality(var_name, node_name);
    }
    for (const auto &prop_lookup : ctx->constraintPropertyList()->propertyLookup()) {
      constraint.properties.push_back(std::any_cast<PropertyIx>(prop_lookup->propertyKeyName()->accept(this)));
    }
  } else {
    auto var_name = std::any_cast<std::string>(ctx->variable(0)->symbolicName()->accept(this));
    check_equality(var_name, node_name);
    constraint.properties.push_back(std::any_cast<PropertyIx>(ctx->propertyLookup()->propertyKeyName()->accept(this)));
  }

  return constraint;
}

antlrcpp::Any CypherMainVisitor::visitTypeConstraintType(MemgraphCypher::TypeConstraintTypeContext *ctx) {
  if (ctx->BOOLEAN()) {
    return memgraph::storage::TypeConstraintKind::BOOLEAN;
  }
  if (ctx->STRING()) {
    return memgraph::storage::TypeConstraintKind::STRING;
  }
  if (ctx->INTEGER()) {
    return memgraph::storage::TypeConstraintKind::INTEGER;
  }
  if (ctx->FLOAT()) {
    return memgraph::storage::TypeConstraintKind::FLOAT;
  }
  if (ctx->LIST()) {
    return memgraph::storage::TypeConstraintKind::LIST;
  }
  if (ctx->MAP()) {
    return memgraph::storage::TypeConstraintKind::MAP;
  }
  if (ctx->DATE()) {
    return memgraph::storage::TypeConstraintKind::DATE;
  }
  if (ctx->LOCALTIME()) {
    return memgraph::storage::TypeConstraintKind::LOCALTIME;
  }
  if (ctx->LOCALDATETIME()) {
    return memgraph::storage::TypeConstraintKind::LOCALDATETIME;
  }
  if (ctx->ZONEDDATETIME()) {
    return memgraph::storage::TypeConstraintKind::ZONEDDATETIME;
  }
  if (ctx->DURATION()) {
    return memgraph::storage::TypeConstraintKind::DURATION;
  }
  if (ctx->ENUM()) {
    return memgraph::storage::TypeConstraintKind::ENUM;
  }
  if (ctx->POINT()) {
    return memgraph::storage::TypeConstraintKind::POINT;
  }
  throw SyntaxException("Unknown type constraint type!");
}

antlrcpp::Any CypherMainVisitor::visitCypherQuery(MemgraphCypher::CypherQueryContext *ctx) {
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
      throw SemanticException("Invalid combination of UNION and UNION ALL.");
    }
    cypher_query->cypher_unions_.push_back(std::any_cast<CypherUnion *>(child->accept(this)));
  }

  if (auto *pre_query_directives_ctx = ctx->preQueryDirectives()) {
    cypher_query->pre_query_directives_ = std::any_cast<PreQueryDirectives>(pre_query_directives_ctx->accept(this));
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

auto get_index_properties(auto &&ctx, CypherMainVisitor &cypher_main_visitor) -> std::vector<PropertyIxPath> {
  auto const as_prop_ix = [&](auto &&property_key_name_ctx) {
    return std::any_cast<PropertyIx>(property_key_name_ctx->accept(&cypher_main_visitor));
  };

  auto const as_prop_ix_path = [&](auto &&nested_property_key_names) -> PropertyIxPath {
    return {nested_property_key_names->propertyKeyName() | srv::transform(as_prop_ix) | r::to_vector};
  };

  return ctx | srv::transform(as_prop_ix_path) | r::to_vector;
}

antlrcpp::Any CypherMainVisitor::visitPreQueryDirectives(MemgraphCypher::PreQueryDirectivesContext *ctx) {
  PreQueryDirectives pre_query_directives;
  for (auto *pre_query_directive : ctx->preQueryDirective()) {
    if (auto *index_hints_ctx = pre_query_directive->indexHints()) {
      for (auto *index_hint_ctx : index_hints_ctx->indexHint()) {
        auto label = AddLabel(std::any_cast<std::string>(index_hint_ctx->labelName()->accept(this)));
        if (index_hint_ctx->nestedPropertyKeyNames().empty()) {
          pre_query_directives.index_hints_.emplace_back(
              // NOLINTNEXTLINE(hicpp-use-emplace,modernize-use-emplace)
              IndexHint{.index_type_ = IndexHint::IndexType::LABEL, .label_ix_ = label});
          continue;
        }

        pre_query_directives.index_hints_.emplace_back(
            // NOLINTNEXTLINE(hicpp-use-emplace,modernize-use-emplace)
            IndexHint{.index_type_ = IndexHint::IndexType::LABEL_PROPERTIES,
                      .label_ix_ = label,
                      .property_ixs_ = get_index_properties(index_hint_ctx->nestedPropertyKeyNames(), *this)});
      }
    } else if (auto *periodic_commit = pre_query_directive->periodicCommit()) {
      if (pre_query_directives.commit_frequency_) {
        throw SyntaxException("Commit frequency can be set only once in the USING statement.");
      }
      auto *periodic_commit_number = periodic_commit->periodicCommitNumber;
      if (!periodic_commit_number->numberLiteral()) {
        throw SyntaxException("Periodic commit should be a number variable.");
      }
      if (!periodic_commit_number->numberLiteral()->integerLiteral()) {
        throw SyntaxException("Periodic commit should be an integer.");
      }

      pre_query_directives.commit_frequency_ = std::any_cast<Expression *>(periodic_commit_number->accept(this));
    } else if (pre_query_directive->hopsLimit()) {
      if (pre_query_directives.hops_limit_) {
        throw SyntaxException("Hops limit can be set only once in the USING statement.");
      }
      pre_query_directives.hops_limit_ = std::any_cast<Expression *>(pre_query_directive->hopsLimit()->accept(this));
    } else {
      throw SyntaxException("Unknown pre query directive!");
    }
  }

  return pre_query_directives;
}

antlrcpp::Any CypherMainVisitor::visitIndexQuery(MemgraphCypher::IndexQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "IndexQuery should have exactly one child!");
  auto *index_query = std::any_cast<IndexQuery *>(ctx->children[0]->accept(this));
  query_ = index_query;
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitPointIndexQuery(MemgraphCypher::PointIndexQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "PointIndexQuery should have exactly one child!");
  auto *point_index_query = std::any_cast<PointIndexQuery *>(ctx->children[0]->accept(this));
  query_ = point_index_query;
  return point_index_query;
}

antlrcpp::Any CypherMainVisitor::visitTextIndexQuery(MemgraphCypher::TextIndexQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "TextIndexQuery should have exactly one child!");
  auto *text_index_query = std::any_cast<TextIndexQuery *>(ctx->children[0]->accept(this));
  query_ = text_index_query;
  return text_index_query;
}

antlrcpp::Any CypherMainVisitor::visitVectorIndexQuery(MemgraphCypher::VectorIndexQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "VectorIndexQuery should have exactly one child!");
  auto *vector_index_query = std::any_cast<VectorIndexQuery *>(ctx->children[0]->accept(this));
  query_ = vector_index_query;
  return vector_index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateIndex(MemgraphCypher::CreateIndexContext *ctx) {
  auto *index_query = storage_->Create<IndexQuery>();

  index_query->action_ = IndexQuery::Action::CREATE;
  index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  index_query->properties_ = get_index_properties(ctx->nestedPropertyKeyNames(), *this);

  // Check composite properties are unique, and in the case of nested properties,
  // that the prefix is also unique (e.g. if we have `a.b`, `a.b.c` is
  // disallowed because the index already exists on the outer `a.b` property.)
  // By sorting, any potential prefix conflicts will be adjacent.
  std::vector<PropertyIxPath> sorted_properties = index_query->properties_;
  std::ranges::sort(sorted_properties);
  auto cmp = [](PropertyIxPath const &lhs, PropertyIxPath const &rhs) {
    auto min_length = std::min(lhs.path.size(), rhs.path.size());
    return std::ranges::equal(lhs.path.cbegin(), lhs.path.cbegin() + min_length, rhs.path.cbegin(),
                              rhs.path.cbegin() + min_length);
  };
  if (std::ranges::adjacent_find(sorted_properties, cmp) != sorted_properties.end()) {
    throw SemanticException("Properties cannot be repeated in a composite index.");
  }

  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitDropIndex(MemgraphCypher::DropIndexContext *ctx) {
  auto *index_query = storage_->Create<IndexQuery>();
  index_query->action_ = IndexQuery::Action::DROP;
  index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  index_query->properties_ = get_index_properties(ctx->nestedPropertyKeyNames(), *this);

  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitEdgeIndexQuery(MemgraphCypher::EdgeIndexQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "EdgeIndexQuery should have exactly one child!");
  auto *index_query = std::any_cast<EdgeIndexQuery *>(ctx->children[0]->accept(this));
  query_ = index_query;
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateEdgeIndex(MemgraphCypher::CreateEdgeIndexContext *ctx) {
  auto *index_query = storage_->Create<EdgeIndexQuery>();
  index_query->action_ = EdgeIndexQuery::Action::CREATE;
  index_query->edge_type_ = AddEdgeType(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  if (ctx->propertyKeyName()) {
    const auto name_key = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
    index_query->properties_ = {name_key};
  }
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitDropEdgeIndex(MemgraphCypher::DropEdgeIndexContext *ctx) {
  auto *index_query = storage_->Create<EdgeIndexQuery>();
  index_query->action_ = EdgeIndexQuery::Action::DROP;
  if (ctx->propertyKeyName()) {
    auto key = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
    index_query->properties_ = {key};
  }
  index_query->edge_type_ = AddEdgeType(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateGlobalEdgeIndex(MemgraphCypher::CreateGlobalEdgeIndexContext *ctx) {
  auto *index_query = storage_->Create<EdgeIndexQuery>();
  index_query->action_ = EdgeIndexQuery::Action::CREATE;
  index_query->global_ = true;
  if (ctx->propertyKeyName()) {
    const auto name_key = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
    index_query->properties_ = {name_key};
  }
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitDropGlobalEdgeIndex(MemgraphCypher::DropGlobalEdgeIndexContext *ctx) {
  auto *index_query = storage_->Create<EdgeIndexQuery>();
  index_query->action_ = EdgeIndexQuery::Action::DROP;
  index_query->global_ = true;
  if (ctx->propertyKeyName()) {
    auto key = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
    index_query->properties_ = {key};
  }
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreatePointIndex(MemgraphCypher::CreatePointIndexContext *ctx) {
  auto *index_query = storage_->Create<PointIndexQuery>();
  index_query->action_ = PointIndexQuery::Action::CREATE;
  index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  index_query->property_ = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitDropPointIndex(MemgraphCypher::DropPointIndexContext *ctx) {
  auto *index_query = storage_->Create<PointIndexQuery>();
  index_query->action_ = PointIndexQuery::Action::DROP;
  index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  index_query->property_ = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateTextIndex(MemgraphCypher::CreateTextIndexContext *ctx) {
  auto *index_query = storage_->Create<TextIndexQuery>();
  index_query->index_name_ = std::any_cast<std::string>(ctx->indexName()->accept(this));
  index_query->action_ = TextIndexQuery::Action::CREATE;
  index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  for (auto *property_key_name_ctx : ctx->propertyKeyName()) {
    index_query->properties_.emplace_back(std::any_cast<PropertyIx>(property_key_name_ctx->accept(this)));
  }
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitDropTextIndex(MemgraphCypher::DropTextIndexContext *ctx) {
  auto *index_query = storage_->Create<TextIndexQuery>();
  index_query->index_name_ = std::any_cast<std::string>(ctx->indexName()->accept(this));
  index_query->action_ = TextIndexQuery::Action::DROP;
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateTextEdgeIndex(MemgraphCypher::CreateTextEdgeIndexContext *ctx) {
  auto *index_query = storage_->Create<CreateTextEdgeIndexQuery>();
  index_query->index_name_ = std::any_cast<std::string>(ctx->indexName()->accept(this));
  index_query->edge_type_ = AddEdgeType(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  for (auto *property_key_name_ctx : ctx->propertyKeyName()) {
    index_query->properties_.emplace_back(std::any_cast<PropertyIx>(property_key_name_ctx->accept(this)));
  }
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateVectorIndex(MemgraphCypher::CreateVectorIndexContext *ctx) {
  auto *index_query = storage_->Create<VectorIndexQuery>();
  index_query->action_ = VectorIndexQuery::Action::CREATE;
  index_query->index_name_ = std::any_cast<std::string>(ctx->indexName()->accept(this));
  index_query->label_ = AddLabel(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  index_query->property_ = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
  index_query->configs_ = std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->configsMap->accept(this));
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateVectorEdgeIndex(MemgraphCypher::CreateVectorEdgeIndexContext *ctx) {
  auto *index_query = storage_->Create<CreateVectorEdgeIndexQuery>();
  index_query->index_name_ = std::any_cast<std::string>(ctx->indexName()->accept(this));
  index_query->edge_type_ = AddEdgeType(std::any_cast<std::string>(ctx->labelName()->accept(this)));
  index_query->property_ = std::any_cast<PropertyIx>(ctx->propertyKeyName()->accept(this));
  index_query->configs_ = std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->configsMap->accept(this));
  query_ = index_query;
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitDropVectorIndex(MemgraphCypher::DropVectorIndexContext *ctx) {
  auto *index_query = storage_->Create<VectorIndexQuery>();
  index_query->action_ = VectorIndexQuery::Action::DROP;
  index_query->index_name_ = std::any_cast<std::string>(ctx->indexName()->accept(this));
  return index_query;
}

antlrcpp::Any CypherMainVisitor::visitAuthQuery(MemgraphCypher::AuthQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "AuthQuery should have exactly one child!");
  auto *auth_query = std::any_cast<AuthQuery *>(ctx->children[0]->accept(this));
  query_ = auth_query;
  return auth_query;
}

antlrcpp::Any CypherMainVisitor::visitDumpQuery(MemgraphCypher::DumpQueryContext *ctx) {
  auto *dump_query = storage_->Create<DumpQuery>();
  query_ = dump_query;
  return dump_query;
}

antlrcpp::Any CypherMainVisitor::visitAnalyzeGraphQuery(MemgraphCypher::AnalyzeGraphQueryContext *ctx) {
  auto *analyze_graph_query = storage_->Create<AnalyzeGraphQuery>();
  if (ctx->listOfColonSymbolicNames()) {
    analyze_graph_query->labels_ =
        std::any_cast<std::vector<std::string>>(ctx->listOfColonSymbolicNames()->accept(this));
  } else {
    analyze_graph_query->labels_.emplace_back("*");
  }
  if (ctx->DELETE()) {
    analyze_graph_query->action_ = AnalyzeGraphQuery::Action::DELETE;
  } else {
    analyze_graph_query->action_ = AnalyzeGraphQuery::Action::ANALYZE;
  }
  query_ = analyze_graph_query;
  return analyze_graph_query;
}

antlrcpp::Any CypherMainVisitor::visitReplicationQuery(MemgraphCypher::ReplicationQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "ReplicationQuery should have exactly one child!");
  auto *replication_query = std::any_cast<ReplicationQuery *>(ctx->children[0]->accept(this));
  query_ = replication_query;
  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitReplicationInfoQuery(MemgraphCypher::ReplicationInfoQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "ReplicationInfoQuery should have exactly one child!");
  auto *replication_info_query = std::any_cast<ReplicationInfoQuery *>(ctx->children[0]->accept(this));
  query_ = replication_info_query;
  return replication_info_query;
}

antlrcpp::Any CypherMainVisitor::visitCoordinatorQuery(MemgraphCypher::CoordinatorQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "CoordinatorQuery should have exactly one child!");
  auto *coordinator_query = std::any_cast<CoordinatorQuery *>(ctx->children[0]->accept(this));
  query_ = coordinator_query;
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitEdgeImportModeQuery(MemgraphCypher::EdgeImportModeQueryContext *ctx) {
  auto *edge_import_mode_query = storage_->Create<EdgeImportModeQuery>();
  if (ctx->ACTIVE()) {
    edge_import_mode_query->status_ = EdgeImportModeQuery::Status::ACTIVE;
  } else {
    edge_import_mode_query->status_ = EdgeImportModeQuery::Status::INACTIVE;
  }
  query_ = edge_import_mode_query;
  return edge_import_mode_query;
}

antlrcpp::Any CypherMainVisitor::visitDropGraphQuery(MemgraphCypher::DropGraphQueryContext * /*ctx*/) {
  auto *drop_graph_query = storage_->Create<DropGraphQuery>();
  query_ = drop_graph_query;
  return drop_graph_query;
}

antlrcpp::Any CypherMainVisitor::visitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::SET_REPLICATION_ROLE;

  auto set_replication_port = [replication_query, ctx, this]() -> void {
    if (ctx->port->numberLiteral() && ctx->port->numberLiteral()->integerLiteral()) {
      replication_query->port_ = std::any_cast<Expression *>(ctx->port->accept(this));
    } else {
      throw SyntaxException("Port must be an integer literal!");
    }
  };

  if (ctx->MAIN()) {
    replication_query->role_ = ReplicationQuery::ReplicationRole::MAIN;
    if (ctx->WITH() || ctx->PORT()) {
      throw SemanticException("Main can't set a port!");
    }

  } else if (ctx->REPLICA()) {
    replication_query->role_ = ReplicationQuery::ReplicationRole::REPLICA;
    if (ctx->WITH() && ctx->PORT()) {
      set_replication_port();
    } else {
      throw SemanticException("Replica must set a port!");
    }
  }

  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitRegisterReplica(MemgraphCypher::RegisterReplicaContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::REGISTER_REPLICA;
  replication_query->instance_name_ = std::any_cast<std::string>(ctx->instanceName()->symbolicName()->accept(this));
  if (ctx->SYNC()) {
    replication_query->sync_mode_ = ReplicationQuery::SyncMode::SYNC;
  } else if (ctx->ASYNC()) {
    replication_query->sync_mode_ = ReplicationQuery::SyncMode::ASYNC;
  } else if (ctx->STRICT_SYNC()) {
    replication_query->sync_mode_ = ReplicationQuery::SyncMode::STRICT_SYNC;
  }

  if (!ctx->socketAddress()->literal()->StringLiteral()) {
    throw SemanticException("Socket address should be a string literal!");
  }
  replication_query->socket_address_ = std::any_cast<Expression *>(ctx->socketAddress()->accept(this));

  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitRegisterInstanceOnCoordinator(
    MemgraphCypher::RegisterInstanceOnCoordinatorContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();

  coordinator_query->action_ = CoordinatorQuery::Action::REGISTER_INSTANCE;
  coordinator_query->instance_name_ = std::any_cast<std::string>(ctx->instanceName()->symbolicName()->accept(this));
  coordinator_query->configs_ =
      std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->configsMap->accept(this));
  coordinator_query->sync_mode_ = [ctx]() {
    if (ctx->ASYNC()) {
      return CoordinatorQuery::SyncMode::ASYNC;
    }
    if (ctx->STRICT_SYNC()) {
      return CoordinatorQuery::SyncMode::STRICT_SYNC;
    }
    return CoordinatorQuery::SyncMode::SYNC;
  }();

  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitUnregisterInstanceOnCoordinator(
    MemgraphCypher::UnregisterInstanceOnCoordinatorContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::UNREGISTER_INSTANCE;
  coordinator_query->instance_name_ = std::any_cast<std::string>(ctx->instanceName()->symbolicName()->accept(this));
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitForceResetClusterStateOnCoordinator(
    MemgraphCypher::ForceResetClusterStateOnCoordinatorContext * /*ctx*/) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::FORCE_RESET_CLUSTER_STATE;
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitDemoteInstanceOnCoordinator(
    MemgraphCypher::DemoteInstanceOnCoordinatorContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::DEMOTE_INSTANCE;
  coordinator_query->instance_name_ = std::any_cast<std::string>(ctx->instanceName()->symbolicName()->accept(this));
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitRemoveCoordinatorInstance(MemgraphCypher::RemoveCoordinatorInstanceContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();

  coordinator_query->action_ = CoordinatorQuery::Action::REMOVE_COORDINATOR_INSTANCE;
  coordinator_query->coordinator_id_ = std::any_cast<Expression *>(ctx->coordinatorServerId()->accept(this));

  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitAddCoordinatorInstance(MemgraphCypher::AddCoordinatorInstanceContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();

  coordinator_query->action_ = CoordinatorQuery::Action::ADD_COORDINATOR_INSTANCE;
  coordinator_query->coordinator_id_ = std::any_cast<Expression *>(ctx->coordinatorServerId()->accept(this));
  coordinator_query->configs_ =
      std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->configsMap->accept(this));

  return coordinator_query;
}

// License check is done in the interpreter
antlrcpp::Any CypherMainVisitor::visitShowInstance(MemgraphCypher::ShowInstanceContext * /*ctx*/) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::SHOW_INSTANCE;
  return coordinator_query;
}

// License check is done in the interpreter
antlrcpp::Any CypherMainVisitor::visitShowInstances(MemgraphCypher::ShowInstancesContext * /*ctx*/) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::SHOW_INSTANCES;
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitYieldLeadership(MemgraphCypher::YieldLeadershipContext * /*ctx*/) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::YIELD_LEADERSHIP;
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitSetCoordinatorSetting(MemgraphCypher::SetCoordinatorSettingContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::SET_COORDINATOR_SETTING;
  if (!ctx->settingName()->literal()->StringLiteral()) {
    throw SemanticException("Setting name should be a string literal");
  }

  if (!ctx->settingValue()->literal()->StringLiteral()) {
    throw SemanticException("Setting value should be a string literal");
  }

  coordinator_query->setting_name_ = std::any_cast<Expression *>(ctx->settingName()->accept(this));
  MG_ASSERT(coordinator_query->setting_name_);

  coordinator_query->setting_value_ = std::any_cast<Expression *>(ctx->settingValue()->accept(this));
  MG_ASSERT(coordinator_query->setting_value_);

  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitShowCoordinatorSettings(MemgraphCypher::ShowCoordinatorSettingsContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::SHOW_COORDINATOR_SETTINGS;
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitShowReplicationLag(MemgraphCypher::ShowReplicationLagContext * /*ctx*/) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::SHOW_REPLICATION_LAG;
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitDropReplica(MemgraphCypher::DropReplicaContext *ctx) {
  auto *replication_query = storage_->Create<ReplicationQuery>();
  replication_query->action_ = ReplicationQuery::Action::DROP_REPLICA;
  replication_query->instance_name_ = std::any_cast<std::string>(ctx->instanceName()->symbolicName()->accept(this));
  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext * /*ctx*/) {
  auto *replication_query = storage_->Create<ReplicationInfoQuery>();
  replication_query->action_ = ReplicationInfoQuery::Action::SHOW_REPLICATION_ROLE;
  return replication_query;
}

antlrcpp::Any CypherMainVisitor::visitShowReplicas(MemgraphCypher::ShowReplicasContext * /*ctx*/) {
  auto *replication_query = storage_->Create<ReplicationInfoQuery>();
  replication_query->action_ = ReplicationInfoQuery::Action::SHOW_REPLICAS;
  return replication_query;
}

// License check is done in the interpreter
antlrcpp::Any CypherMainVisitor::visitSetInstanceToMain(MemgraphCypher::SetInstanceToMainContext *ctx) {
  auto *coordinator_query = storage_->Create<CoordinatorQuery>();
  coordinator_query->action_ = CoordinatorQuery::Action::SET_INSTANCE_TO_MAIN;
  coordinator_query->instance_name_ = std::any_cast<std::string>(ctx->instanceName()->symbolicName()->accept(this));
  query_ = coordinator_query;
  return coordinator_query;
}

antlrcpp::Any CypherMainVisitor::visitLockPathQuery(MemgraphCypher::LockPathQueryContext *ctx) {
  auto *lock_query = storage_->Create<LockPathQuery>();
  if (ctx->STATUS()) {
    lock_query->action_ = LockPathQuery::Action::STATUS;
  } else if (ctx->LOCK()) {
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
  if (ctx->csvFile()->literal() && ctx->csvFile()->literal()->StringLiteral()) {
    load_csv->file_ = std::any_cast<Expression *>(ctx->csvFile()->accept(this));
  } else if (ctx->csvFile()->parameter()) {
    load_csv->file_ = std::any_cast<ParameterLookup *>(ctx->csvFile()->accept(this));
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

  // handle character sequence which will correspond to nulls
  if (ctx->NULLIF()) {
    load_csv->nullif_ = std::any_cast<Expression *>(ctx->nullif()->accept(this));
  }

  // handle delimiter
  if (ctx->DELIMITER()) {
    if (ctx->delimiter()->literal()->StringLiteral()) {
      load_csv->delimiter_ = std::any_cast<Expression *>(ctx->delimiter()->accept(this));
    } else {
      throw SemanticException("Delimiter should be a string literal");
    }
  }

  // handle quote
  if (ctx->QUOTE()) {
    if (ctx->quote()->literal()->StringLiteral()) {
      load_csv->quote_ = std::any_cast<Expression *>(ctx->quote()->accept(this));
    } else {
      throw SemanticException("Quote should be a string literal");
    }
  }

  // handle row variable
  load_csv->row_var_ =
      storage_->Create<Identifier>(std::any_cast<std::string>(ctx->rowVar()->variable()->accept(this)));

  return load_csv;
}

antlrcpp::Any CypherMainVisitor::visitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext *ctx) {
  auto *free_memory_query = storage_->Create<FreeMemoryQuery>();
  query_ = free_memory_query;
  return free_memory_query;
}

antlrcpp::Any CypherMainVisitor::visitTriggerQuery(MemgraphCypher::TriggerQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "TriggerQuery should have exactly one child!");
  auto *trigger_query = std::any_cast<TriggerQuery *>(ctx->children[0]->accept(this));
  query_ = trigger_query;
  return trigger_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateTrigger(MemgraphCypher::CreateTriggerContext *ctx) {
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

antlrcpp::Any CypherMainVisitor::visitDropTrigger(MemgraphCypher::DropTriggerContext *ctx) {
  auto *trigger_query = storage_->Create<TriggerQuery>();
  trigger_query->action_ = TriggerQuery::Action::DROP_TRIGGER;
  trigger_query->trigger_name_ = std::any_cast<std::string>(ctx->triggerName()->symbolicName()->accept(this));
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

antlrcpp::Any CypherMainVisitor::visitStorageModeQuery(MemgraphCypher::StorageModeQueryContext *ctx) {
  auto *storage_mode_query = storage_->Create<StorageModeQuery>();

  storage_mode_query->storage_mode_ = std::invoke([mode = ctx->storageMode()]() {
    if (mode->IN_MEMORY_ANALYTICAL()) {
      return StorageModeQuery::StorageMode::IN_MEMORY_ANALYTICAL;
    }
    if (mode->IN_MEMORY_TRANSACTIONAL()) {
      return StorageModeQuery::StorageMode::IN_MEMORY_TRANSACTIONAL;
    }
    return StorageModeQuery::StorageMode::ON_DISK_TRANSACTIONAL;
  });

  query_ = storage_mode_query;
  return storage_mode_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext *ctx) {
  query_ = storage_->Create<CreateSnapshotQuery>();
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitRecoverSnapshotQuery(MemgraphCypher::RecoverSnapshotQueryContext *ctx) {
  auto *recover_snapshot = storage_->Create<RecoverSnapshotQuery>();
  if (!ctx->path || !ctx->path->StringLiteral()) {
    throw SemanticException("Snapshot path must be a string literal.");
  }
  recover_snapshot->snapshot_ = std::any_cast<Expression *>(ctx->path->accept(this));

  recover_snapshot->force_ = ctx->FORCE();
  query_ = recover_snapshot;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowSnapshotsQuery(MemgraphCypher::ShowSnapshotsQueryContext * /*ctx*/) {
  query_ = storage_->Create<ShowSnapshotsQuery>();
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowNextSnapshotQuery(MemgraphCypher::ShowNextSnapshotQueryContext * /*ctx*/) {
  query_ = storage_->Create<ShowNextSnapshotQuery>();
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitStreamQuery(MemgraphCypher::StreamQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "StreamQuery should have exactly one child!");
  auto *stream_query = std::any_cast<StreamQuery *>(ctx->children[0]->accept(this));
  query_ = stream_query;
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitCreateStream(MemgraphCypher::CreateStreamContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "CreateStreamQuery should have exactly one child!");
  auto *stream_query = std::any_cast<StreamQuery *>(ctx->children[0]->accept(this));
  query_ = stream_query;
  return stream_query;
}

namespace {
std::vector<std::string> TopicNamesFromSymbols(
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
  memory.erase(key);
}

enum class CommonStreamConfigKey : uint8_t { TRANSFORM, BATCH_INTERVAL, BATCH_SIZE, END };

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

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GENERATE_STREAM_CONFIG_KEY_ENUM(stream, first_config, ...)   \
  enum class BOOST_PP_CAT(stream, ConfigKey) : uint8_t {             \
    first_config = static_cast<uint8_t>(CommonStreamConfigKey::END), \
    __VA_ARGS__                                                      \
  };

GENERATE_STREAM_CONFIG_KEY_ENUM(Kafka, TOPICS, CONSUMER_GROUP, BOOTSTRAP_SERVERS, CONFIGS, CREDENTIALS);

std::string_view ToString(const KafkaConfigKey key) {
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

void MapCommonStreamConfigs(auto &memory, StreamQuery &stream_query) {
  MapConfig<true, std::string>(memory, CommonStreamConfigKey::TRANSFORM, stream_query.transform_name_);
  MapConfig<false, Expression *>(memory, CommonStreamConfigKey::BATCH_INTERVAL, stream_query.batch_interval_);
  MapConfig<false, Expression *>(memory, CommonStreamConfigKey::BATCH_SIZE, stream_query.batch_size_);
}
}  // namespace

antlrcpp::Any CypherMainVisitor::visitConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext *ctx) {
  MG_ASSERT(ctx->literal().size() == 2);
  return std::pair{std::any_cast<Expression *>(ctx->literal(0)->accept(this)),
                   std::any_cast<Expression *>(ctx->literal(1)->accept(this))};
}

antlrcpp::Any CypherMainVisitor::visitConfigMap(MemgraphCypher::ConfigMapContext *ctx) {
  std::unordered_map<Expression *, Expression *> map;
  for (auto *key_value_pair : ctx->configKeyValuePair()) {
    // If the queries are cached, then only the stripped query is parsed, so the actual keys cannot be determined
    // here. That means duplicates cannot be checked.
    map.insert(std::any_cast<std::pair<Expression *, Expression *>>(key_value_pair->accept(this)));
  }
  return map;
}

antlrcpp::Any CypherMainVisitor::visitKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::CREATE_STREAM;
  stream_query->type_ = StreamQuery::Type::KAFKA;
  stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));

  for (auto *create_config_ctx : ctx->kafkaCreateStreamConfig()) {
    create_config_ctx->accept(this);
  }

  MapConfig<true, std::vector<std::string>, Expression *>(memory_, KafkaConfigKey::TOPICS, stream_query->topic_names_);
  MapConfig<false, std::string>(memory_, KafkaConfigKey::CONSUMER_GROUP, stream_query->consumer_group_);
  MapConfig<false, Expression *>(memory_, KafkaConfigKey::BOOTSTRAP_SERVERS, stream_query->bootstrap_servers_);
  MapConfig<false, std::unordered_map<Expression *, Expression *>>(memory_, KafkaConfigKey::CONFIGS,
                                                                   stream_query->configs_);
  MapConfig<false, std::unordered_map<Expression *, Expression *>>(memory_, KafkaConfigKey::CREDENTIALS,
                                                                   stream_query->credentials_);

  MapCommonStreamConfigs(memory_, *stream_query);

  return stream_query;
}

namespace {
void ThrowIfExists(const auto &map, const EnumUint8 auto &enum_key) {
  const auto key = static_cast<uint8_t>(enum_key);
  if (map.contains(key)) {
    throw SemanticException("{} defined multiple times in the query", ToString(enum_key));
  }
}

void GetTopicNames(auto &destination, MemgraphCypher::TopicNamesContext *topic_names_ctx,
                   antlr4::tree::ParseTreeVisitor &visitor) {
  MG_ASSERT(topic_names_ctx != nullptr);
  if (auto *symbolic_topic_names_ctx = topic_names_ctx->symbolicTopicNames()) {
    destination = TopicNamesFromSymbols(visitor, symbolic_topic_names_ctx->symbolicNameWithDotsAndMinus());
  } else {
    if (!topic_names_ctx->literal()->StringLiteral()) {
      throw SemanticException("Topic names should be defined as a string literal or as symbolic names");
    }
    destination = std::any_cast<Expression *>(topic_names_ctx->accept(&visitor));
  }
}

}  // namespace

antlrcpp::Any CypherMainVisitor::visitKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext *ctx) {
  if (ctx->commonCreateStreamConfig()) {
    return ctx->commonCreateStreamConfig()->accept(this);
  }

  if (ctx->TOPICS()) {
    ThrowIfExists(memory_, KafkaConfigKey::TOPICS);
    static constexpr auto topics_key = static_cast<uint8_t>(KafkaConfigKey::TOPICS);
    GetTopicNames(memory_[topics_key], ctx->topicNames(), *this);
    return {};
  }

  if (ctx->CONSUMER_GROUP()) {
    ThrowIfExists(memory_, KafkaConfigKey::CONSUMER_GROUP);
    static constexpr auto consumer_group_key = static_cast<uint8_t>(KafkaConfigKey::CONSUMER_GROUP);
    memory_[consumer_group_key] = JoinSymbolicNamesWithDotsAndMinus(*this, *ctx->consumerGroup);
    return {};
  }

  if (ctx->CONFIGS()) {
    ThrowIfExists(memory_, KafkaConfigKey::CONFIGS);
    static constexpr auto configs_key = static_cast<uint8_t>(KafkaConfigKey::CONFIGS);
    memory_.emplace(configs_key,
                    std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->configsMap->accept(this)));
    return {};
  }

  if (ctx->CREDENTIALS()) {
    ThrowIfExists(memory_, KafkaConfigKey::CREDENTIALS);
    static constexpr auto credentials_key = static_cast<uint8_t>(KafkaConfigKey::CREDENTIALS);
    memory_.emplace(credentials_key,
                    std::any_cast<std::unordered_map<Expression *, Expression *>>(ctx->credentialsMap->accept(this)));
    return {};
  }

  MG_ASSERT(ctx->BOOTSTRAP_SERVERS());
  ThrowIfExists(memory_, KafkaConfigKey::BOOTSTRAP_SERVERS);
  if (!ctx->bootstrapServers->StringLiteral()) {
    throw SemanticException("Bootstrap servers should be a string!");
  }

  const auto bootstrap_servers_key = static_cast<uint8_t>(KafkaConfigKey::BOOTSTRAP_SERVERS);
  memory_[bootstrap_servers_key] = std::any_cast<Expression *>(ctx->bootstrapServers->accept(this));
  return {};
}

namespace {
GENERATE_STREAM_CONFIG_KEY_ENUM(Pulsar, TOPICS, SERVICE_URL);

std::string_view ToString(const PulsarConfigKey key) {
  switch (key) {
    case PulsarConfigKey::TOPICS:
      return "TOPICS";
    case PulsarConfigKey::SERVICE_URL:
      return "SERVICE_URL";
  }
}
}  // namespace

antlrcpp::Any CypherMainVisitor::visitPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::CREATE_STREAM;
  stream_query->type_ = StreamQuery::Type::PULSAR;
  stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));

  for (auto *create_config_ctx : ctx->pulsarCreateStreamConfig()) {
    create_config_ctx->accept(this);
  }

  MapConfig<true, std::vector<std::string>, Expression *>(memory_, PulsarConfigKey::TOPICS, stream_query->topic_names_);
  MapConfig<false, Expression *>(memory_, PulsarConfigKey::SERVICE_URL, stream_query->service_url_);

  MapCommonStreamConfigs(memory_, *stream_query);

  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext *ctx) {
  if (ctx->commonCreateStreamConfig()) {
    return ctx->commonCreateStreamConfig()->accept(this);
  }

  if (ctx->TOPICS()) {
    ThrowIfExists(memory_, PulsarConfigKey::TOPICS);
    const auto topics_key = static_cast<uint8_t>(PulsarConfigKey::TOPICS);
    GetTopicNames(memory_[topics_key], ctx->topicNames(), *this);
    return {};
  }

  MG_ASSERT(ctx->SERVICE_URL());
  ThrowIfExists(memory_, PulsarConfigKey::SERVICE_URL);
  if (!ctx->serviceUrl->StringLiteral()) {
    throw SemanticException("Service URL must be a string!");
  }
  const auto service_url_key = static_cast<uint8_t>(PulsarConfigKey::SERVICE_URL);
  memory_[service_url_key] = std::any_cast<Expression *>(ctx->serviceUrl->accept(this));
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
      throw SemanticException("Batch interval must be an integer literal!");
    }
    const auto batch_interval_key = static_cast<uint8_t>(CommonStreamConfigKey::BATCH_INTERVAL);
    memory_[batch_interval_key] = std::any_cast<Expression *>(ctx->batchInterval->accept(this));
    return {};
  }

  MG_ASSERT(ctx->BATCH_SIZE());
  ThrowIfExists(memory_, CommonStreamConfigKey::BATCH_SIZE);
  if (!ctx->batchSize->numberLiteral() || !ctx->batchSize->numberLiteral()->integerLiteral()) {
    throw SemanticException("Batch size must be an integer literal!");
  }
  const auto batch_size_key = static_cast<uint8_t>(CommonStreamConfigKey::BATCH_SIZE);
  memory_[batch_size_key] = std::any_cast<Expression *>(ctx->batchSize->accept(this));
  return {};
}

antlrcpp::Any CypherMainVisitor::visitDropStream(MemgraphCypher::DropStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::DROP_STREAM;
  stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitStartStream(MemgraphCypher::StartStreamContext *ctx) {
  auto *stream_query = storage_->Create<StreamQuery>();
  stream_query->action_ = StreamQuery::Action::START_STREAM;

  if (ctx->BATCH_LIMIT()) {
    if (!ctx->batchLimit->numberLiteral() || !ctx->batchLimit->numberLiteral()->integerLiteral()) {
      throw SemanticException("Batch limit should be an integer literal!");
    }
    stream_query->batch_limit_ = std::any_cast<Expression *>(ctx->batchLimit->accept(this));
  }
  if (ctx->TIMEOUT()) {
    if (!ctx->timeout->numberLiteral() || !ctx->timeout->numberLiteral()->integerLiteral()) {
      throw SemanticException("Timeout should be an integer literal!");
    }
    if (!ctx->BATCH_LIMIT()) {
      throw SemanticException("Parameter TIMEOUT can only be defined if BATCH_LIMIT is defined");
    }
    stream_query->timeout_ = std::any_cast<Expression *>(ctx->timeout->accept(this));
  }

  stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));
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
  stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));
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
  stream_query->stream_name_ = std::any_cast<std::string>(ctx->streamName()->symbolicName()->accept(this));

  if (ctx->BATCH_LIMIT()) {
    if (!ctx->batchLimit->numberLiteral() || !ctx->batchLimit->numberLiteral()->integerLiteral()) {
      throw SemanticException("Batch limit should be an integer literal!");
    }
    stream_query->batch_limit_ = std::any_cast<Expression *>(ctx->batchLimit->accept(this));
  }
  if (ctx->TIMEOUT()) {
    if (!ctx->timeout->numberLiteral() || !ctx->timeout->numberLiteral()->integerLiteral()) {
      throw SemanticException("Timeout should be an integer literal!");
    }
    stream_query->timeout_ = std::any_cast<Expression *>(ctx->timeout->accept(this));
  }
  return stream_query;
}

antlrcpp::Any CypherMainVisitor::visitSettingQuery(MemgraphCypher::SettingQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "SettingQuery should have exactly one child!");
  auto *setting_query = std::any_cast<SettingQuery *>(ctx->children[0]->accept(this));
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

  setting_query->setting_name_ = std::any_cast<Expression *>(ctx->settingName()->accept(this));
  MG_ASSERT(setting_query->setting_name_);

  setting_query->setting_value_ = std::any_cast<Expression *>(ctx->settingValue()->accept(this));
  MG_ASSERT(setting_query->setting_value_);
  return setting_query;
}

antlrcpp::Any CypherMainVisitor::visitShowSetting(MemgraphCypher::ShowSettingContext *ctx) {
  auto *setting_query = storage_->Create<SettingQuery>();
  setting_query->action_ = SettingQuery::Action::SHOW_SETTING;

  if (!ctx->settingName()->literal()->StringLiteral()) {
    throw SemanticException("Setting name should be a string literal");
  }

  setting_query->setting_name_ = std::any_cast<Expression *>(ctx->settingName()->accept(this));
  MG_ASSERT(setting_query->setting_name_);

  return setting_query;
}

antlrcpp::Any CypherMainVisitor::visitShowSettings(MemgraphCypher::ShowSettingsContext * /*ctx*/) {
  auto *setting_query = storage_->Create<SettingQuery>();
  setting_query->action_ = SettingQuery::Action::SHOW_ALL_SETTINGS;
  return setting_query;
}

antlrcpp::Any CypherMainVisitor::visitTransactionQueueQuery(MemgraphCypher::TransactionQueueQueryContext *ctx) {
  MG_ASSERT(ctx->children.size() == 1, "TransactionQueueQuery should have exactly one child!");
  auto *transaction_queue_query = std::any_cast<TransactionQueueQuery *>(ctx->children[0]->accept(this));
  query_ = transaction_queue_query;
  return transaction_queue_query;
}

antlrcpp::Any CypherMainVisitor::visitShowTransactions(MemgraphCypher::ShowTransactionsContext * /*ctx*/) {
  auto *transaction_shower = storage_->Create<TransactionQueueQuery>();
  transaction_shower->action_ = TransactionQueueQuery::Action::SHOW_TRANSACTIONS;
  return transaction_shower;
}

antlrcpp::Any CypherMainVisitor::visitTerminateTransactions(MemgraphCypher::TerminateTransactionsContext *ctx) {
  auto *terminator = storage_->Create<TransactionQueueQuery>();
  terminator->action_ = TransactionQueueQuery::Action::TERMINATE_TRANSACTIONS;
  terminator->transaction_id_list_ = std::any_cast<std::vector<Expression *>>(ctx->transactionIdList()->accept(this));
  return terminator;
}

antlrcpp::Any CypherMainVisitor::visitTransactionIdList(MemgraphCypher::TransactionIdListContext *ctx) {
  std::vector<Expression *> transaction_ids;
  for (auto *transaction_id : ctx->transactionId()) {
    transaction_ids.push_back(std::any_cast<Expression *>(transaction_id->accept(this)));
  }
  return transaction_ids;
}

antlrcpp::Any CypherMainVisitor::visitVersionQuery(MemgraphCypher::VersionQueryContext * /*ctx*/) {
  auto *version_query = storage_->Create<VersionQuery>();
  query_ = version_query;
  return version_query;
}

antlrcpp::Any CypherMainVisitor::visitCypherUnion(MemgraphCypher::CypherUnionContext *ctx) {
  bool distinct = !ctx->ALL();
  auto *cypher_union = storage_->Create<CypherUnion>(distinct);
  DMG_ASSERT(ctx->singleQuery(), "Expected single query.");
  cypher_union->single_query_ = std::any_cast<SingleQuery *>(ctx->singleQuery()->accept(this));
  return cypher_union;
}

antlrcpp::Any CypherMainVisitor::visitSingleQuery(MemgraphCypher::SingleQueryContext *ctx) {
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
    } else if (const auto *call_subquery = utils::Downcast<CallSubquery>(clause); call_subquery != nullptr) {
      if (has_return) {
        throw SemanticException("CALL can't be put after RETURN clause.");
      }
      const auto *single_query = call_subquery->cypher_query_->single_query_;
      if (single_query) {
        has_update |= single_query->has_update;
        for (auto *cypher_union : call_subquery->cypher_query_->cypher_unions_) {
          if (has_update) break;
          const auto *single_query = cypher_union->single_query_;
          if (single_query) {
            has_update |= single_query->has_update;
          }
        }
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
               utils::IsSubtype(clause_type, RemoveLabels::kType) || utils::IsSubtype(clause_type, Merge::kType) ||
               utils::IsSubtype(clause_type, Foreach::kType)) {
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
  if (!has_update && !has_return && !is_standalone_call_procedure && !parsing_exists_subquery_) {
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

  single_query->has_update = has_update;
  return single_query;
}

antlrcpp::Any CypherMainVisitor::visitClause(MemgraphCypher::ClauseContext *ctx) {
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
  if (ctx->callProcedure()) {
    return static_cast<Clause *>(std::any_cast<CallProcedure *>(ctx->callProcedure()->accept(this)));
  }
  if (ctx->loadCsv()) {
    return static_cast<Clause *>(std::any_cast<LoadCsv *>(ctx->loadCsv()->accept(this)));
  }
  if (ctx->foreach ()) {
    return static_cast<Clause *>(std::any_cast<Foreach *>(ctx->foreach ()->accept(this)));
  }
  if (ctx->callSubquery()) {
    return static_cast<Clause *>(std::any_cast<CallSubquery *>(ctx->callSubquery()->accept(this)));
  }
  // TODO: implement other clauses.
  throw utils::NotYetImplemented("clause '{}'", ctx->getText());
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitCypherMatch(MemgraphCypher::CypherMatchContext *ctx) {
  auto *match = storage_->Create<Match>();
  match->optional_ = !!ctx->OPTIONAL();
  if (ctx->where()) {
    match->where_ = std::any_cast<Where *>(ctx->where()->accept(this));
  }
  match->patterns_ = std::any_cast<std::vector<Pattern *>>(ctx->pattern()->accept(this));
  return match;
}

antlrcpp::Any CypherMainVisitor::visitCreate(MemgraphCypher::CreateContext *ctx) {
  auto *create = storage_->Create<Create>();
  create->patterns_ = std::any_cast<std::vector<Pattern *>>(ctx->pattern()->accept(this));
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
    call_proc->arguments_.push_back(std::any_cast<Expression *>(expr->accept(this)));
  }

  if (auto *memory_limit_ctx = ctx->procedureMemoryLimit()) {
    const auto memory_limit_info = VisitMemoryLimit(memory_limit_ctx->memoryLimit(), this);
    if (memory_limit_info) {
      call_proc->memory_limit_ = memory_limit_info->first;
      call_proc->memory_scale_ = memory_limit_info->second;
    }
  }

  const auto &maybe_found = procedure::FindProcedure(procedure::gModuleRegistry, call_proc->procedure_name_);
  if (!maybe_found) {
    // TODO remove this once void procedures are supported,
    // this will not be needed anymore.
    const auto mg_specific_name = procedure::gCallableAliasMapper.FindAlias(call_proc->procedure_name_);
    const bool void_procedure_required = (mg_specific_name && *mg_specific_name == "mgps.validate");
    if (void_procedure_required) {
      // This is a special case. Since void procedures currently are not supported,
      // we have to make sure that the non-memgraph native, void procedures that are
      // possibly used against a memgraph instance are handled correctly. As of now
      // this is the only known such case. This should be more generic, but the most
      // generic solution would be to implement void procedures.
      call_proc->void_procedure_ = true;
    } else {
      throw SemanticException("There is no procedure named '{}'.", call_proc->procedure_name_);
    }
  }
  if (maybe_found) {
    call_proc->is_write_ = maybe_found->second->info.is_write;
  }

  auto *yield_ctx = ctx->yieldProcedureResults();
  if (!yield_ctx) {
    if ((maybe_found && !maybe_found->second->results.empty()) && !call_proc->void_procedure_) {
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
      call_proc->result_fields_.push_back(std::any_cast<std::string>(result->variable()[0]->accept(this)));
      std::string result_alias;
      if (result->variable().size() == 2) {
        result_alias = std::any_cast<std::string>(result->variable()[1]->accept(this));
      } else {
        result_alias = std::any_cast<std::string>(result->variable()[0]->accept(this));
      }
      call_proc->result_identifiers_.push_back(storage_->Create<Identifier>(result_alias));
    }
  } else {
    call_proc->is_write_ = maybe_found->second->info.is_write;

    auto *yield_ctx = ctx->yieldProcedureResults();
    if (!yield_ctx) {
      if (!maybe_found->second->results.empty() && !call_proc->void_procedure_) {
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
        call_proc->result_fields_.push_back(std::any_cast<std::string>(result->variable()[0]->accept(this)));
        std::string result_alias;
        if (result->variable().size() == 2) {
          result_alias = std::any_cast<std::string>(result->variable()[1]->accept(this));
        } else {
          result_alias = std::any_cast<std::string>(result->variable()[0]->accept(this));
        }
        call_proc->result_identifiers_.push_back(storage_->Create<Identifier>(result_alias));
      }
    } else {
      const auto &maybe_found = procedure::FindProcedure(procedure::gModuleRegistry, call_proc->procedure_name_);
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
  }

  return call_proc;
}

/**
 * @return std::string
 */
antlrcpp::Any CypherMainVisitor::visitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *ctx) {
  return std::any_cast<std::string>(ctx->symbolicName()->accept(this));
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitCreateRole(MemgraphCypher::CreateRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::CREATE_ROLE;
  auth->if_not_exists_ = !!ctx->ifNotExists();
  auth->roles_ = {std::any_cast<std::string>(ctx->role->accept(this))};
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitDropRole(MemgraphCypher::DropRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DROP_ROLE;
  auth->roles_ = {std::any_cast<std::string>(ctx->role->accept(this))};
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowRoles(MemgraphCypher::ShowRolesContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_ROLES;
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitCreateUser(MemgraphCypher::CreateUserContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::CREATE_USER;
  auth->if_not_exists_ = !!ctx->ifNotExists();
  auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
  if (ctx->password) {
    if (!ctx->password->StringLiteral() && !ctx->literal()->CYPHERNULL()) {
      throw SyntaxException("Password should be a string literal or null.");
    }
    auth->password_ = std::any_cast<Expression *>(ctx->password->accept(this));
  }
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitSetPassword(MemgraphCypher::SetPasswordContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SET_PASSWORD;
  auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
  if (!ctx->password->StringLiteral() && !ctx->literal()->CYPHERNULL()) {
    throw SyntaxException("Password should be a string literal or null.");
  }
  auth->password_ = std::any_cast<Expression *>(ctx->password->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitChangePassword(MemgraphCypher::ChangePasswordContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::CHANGE_PASSWORD;
  if (!ctx->newPassword->StringLiteral()) {
    throw SyntaxException("New password should be a string literal or null.");
  }
  if (!ctx->oldPassword->StringLiteral()) {
    throw SyntaxException("Old password should be a string literal or null.");
  }
  auth->new_password_ = std::any_cast<Expression *>(ctx->newPassword->accept(this));
  auth->old_password_ = std::any_cast<Expression *>(ctx->oldPassword->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitDropUser(MemgraphCypher::DropUserContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DROP_USER;
  auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowCurrentUser(MemgraphCypher::ShowCurrentUserContext * /*ctx*/) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_CURRENT_USER;
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowCurrentRole(MemgraphCypher::ShowCurrentRoleContext * /*ctx*/) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_CURRENT_ROLE;
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowUsers(MemgraphCypher::ShowUsersContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_USERS;
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitSetRole(MemgraphCypher::SetRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SET_ROLE;
  auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
  auth->roles_ = std::any_cast<std::vector<std::string>>(ctx->roles->accept(this));
  // Optionally limit the role to specific databases
  if (ctx->db) {
    auto db_names = std::any_cast<std::vector<std::string>>(ctx->db->accept(this));
    auth->role_databases_ = std::unordered_set<std::string>(std::make_move_iterator(db_names.begin()),
                                                            std::make_move_iterator(db_names.end()));
  }
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitClearRole(MemgraphCypher::ClearRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::CLEAR_ROLE;
  auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));
  // Optionally limit the role to specific databases
  if (ctx->db) {
    auto db_names = std::any_cast<std::vector<std::string>>(ctx->db->accept(this));
    auth->role_databases_ = std::unordered_set<std::string>(std::make_move_iterator(db_names.begin()),
                                                            std::make_move_iterator(db_names.end()));
  }
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::GRANT_PRIVILEGE;
  auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  if (ctx->grantPrivilegesList()) {
    const auto [label_privileges, edge_type_privileges, privileges] = std::any_cast<
        std::tuple<std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>,
                   std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>,
                   std::vector<memgraph::query::AuthQuery::Privilege>>>(ctx->grantPrivilegesList()->accept(this));
    auth->label_privileges_ = label_privileges;
    auth->edge_type_privileges_ = edge_type_privileges;
    auth->privileges_ = privileges;
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
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DENY_PRIVILEGE;
  auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  if (ctx->privilegesList()) {
    auth->privileges_ = std::any_cast<std::vector<AuthQuery::Privilege>>(ctx->privilegesList()->accept(this));
  } else {
    /* deny all privileges */
    auth->privileges_ = kPrivilegesAll;
  }
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitPrivilegesList(MemgraphCypher::PrivilegesListContext *ctx) {
  std::vector<AuthQuery::Privilege> privileges{};
  for (const auto &privilege : ctx->privilege()) {
    privileges.push_back(std::any_cast<AuthQuery::Privilege>(privilege->accept(this)));
  }

  return privileges;
}

/**
 * @return std::tuple<std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>,
                    std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>,
                    std::vector<memgraph::query::AuthQuery::Privilege>>
 */
antlrcpp::Any CypherMainVisitor::visitGrantPrivilegesList(MemgraphCypher::GrantPrivilegesListContext *ctx) {
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> label_privileges;
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges;
  std::vector<memgraph::query::AuthQuery::Privilege> privileges;
  for (auto *it : ctx->privilegeOrEntityPrivileges()) {
    if (it->entityPrivilegeList()) {
      const auto result =
          std::any_cast<std::pair<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>,
                                  std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>>(
              it->entityPrivilegeList()->accept(this));
      if (!result.first.empty()) {
        label_privileges.emplace_back(result.first);
      }
      if (!result.second.empty()) {
        edge_type_privileges.emplace_back(result.second);
      }
    } else {
      privileges.push_back(std::any_cast<AuthQuery::Privilege>(it->privilege()->accept(this)));
    }
  }

  return std::tuple<std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>,
                    std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>,
                    std::vector<memgraph::query::AuthQuery::Privilege>>(label_privileges, edge_type_privileges,
                                                                        privileges);
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::REVOKE_PRIVILEGE;
  auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  if (ctx->revokePrivilegesList()) {
    for (auto *it : ctx->revokePrivilegesList()->privilegeOrEntities()) {
      if (it->entitiesList()) {
        const auto entity_type = std::any_cast<EntityType>(it->entityType()->accept(this));
        if (entity_type == EntityType::LABELS) {
          auth->label_privileges_.push_back(
              {{AuthQuery::FineGrainedPrivilege::CREATE_DELETE,
                std::any_cast<std::vector<std::string>>(it->entitiesList()->accept(this))}});
        } else {
          auth->edge_type_privileges_.push_back(
              {{AuthQuery::FineGrainedPrivilege::CREATE_DELETE,
                std::any_cast<std::vector<std::string>>(it->entitiesList()->accept(this))}});
        }
      } else {
        auth->privileges_.push_back(std::any_cast<AuthQuery::Privilege>(it->privilege()->accept(this)));
      }
    }
  } else {
    /* revoke all privileges */
    auth->privileges_ = kPrivilegesAll;
  }
  return auth;
}

/**
 * @return std::pair<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>,
                     std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
 */
antlrcpp::Any CypherMainVisitor::visitEntityPrivilegeList(MemgraphCypher::EntityPrivilegeListContext *ctx) {
  std::pair<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>,
            std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
      result;

  for (auto *it : ctx->entityPrivilege()) {
    const auto key = std::any_cast<AuthQuery::FineGrainedPrivilege>(it->granularPrivilege()->accept(this));
    const auto entityType = std::any_cast<EntityType>(it->entityType()->accept(this));
    auto value = std::any_cast<std::vector<std::string>>(it->entitiesList()->accept(this));

    switch (entityType) {
      case EntityType::LABELS:
        result.first[key] = std::move(value);
        break;
      case EntityType::EDGE_TYPES:
        result.second[key] = std::move(value);
        break;
    }
  }
  return result;
}

antlrcpp::Any CypherMainVisitor::visitListOfColonSymbolicNames(MemgraphCypher::ListOfColonSymbolicNamesContext *ctx) {
  std::vector<std::string> symbolic_names;
  for (auto *symbolic_name : ctx->colonSymbolicName()) {
    symbolic_names.push_back(std::any_cast<std::string>(symbolic_name->symbolicName()->accept(this)));
  }
  return symbolic_names;
}

antlrcpp::Any CypherMainVisitor::visitListOfSymbolicNames(MemgraphCypher::ListOfSymbolicNamesContext *ctx) {
  std::vector<std::string> symbolic_names;
  for (auto *symbolic_name : ctx->symbolicName()) {
    symbolic_names.push_back(std::any_cast<std::string>(symbolic_name->accept(this)));
  }
  return symbolic_names;
}

antlrcpp::Any CypherMainVisitor::visitWildcardListOfSymbolicNames(
    MemgraphCypher::WildcardListOfSymbolicNamesContext *ctx) {
  if (ctx->listOfSymbolicNames()) {
    return ctx->listOfSymbolicNames()->accept(this);
  }
  return std::vector<std::string>{"*"};
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitGrantImpersonateUser(MemgraphCypher::GrantImpersonateUserContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::GRANT_IMPERSONATE_USER;
  auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  auth->impersonation_targets_ = std::any_cast<std::vector<std::string>>(ctx->targets->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitDenyImpersonateUser(MemgraphCypher::DenyImpersonateUserContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DENY_IMPERSONATE_USER;
  auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  auth->impersonation_targets_ = std::any_cast<std::vector<std::string>>(ctx->targets->accept(this));
  return auth;
}

/**
 * @return std::vector<std::string>
 */
antlrcpp::Any CypherMainVisitor::visitEntitiesList(MemgraphCypher::EntitiesListContext *ctx) {
  std::vector<std::string> entities;
  if (ctx->listOfColonSymbolicNames()) {
    return ctx->listOfColonSymbolicNames()->accept(this);
  }
  entities.emplace_back("*");
  return entities;
}

/**
 * @return std::string
 */
antlrcpp::Any CypherMainVisitor::visitWildcardName(MemgraphCypher::WildcardNameContext *ctx) {
  if (ctx->symbolicName()) {
    return ctx->symbolicName()->accept(this);
  }
  return std::string("*");
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
  if (ctx->MODULE_READ()) return AuthQuery::Privilege::MODULE_READ;
  if (ctx->MODULE_WRITE()) return AuthQuery::Privilege::MODULE_WRITE;
  if (ctx->WEBSOCKET()) return AuthQuery::Privilege::WEBSOCKET;
  if (ctx->TRANSACTION_MANAGEMENT()) return AuthQuery::Privilege::TRANSACTION_MANAGEMENT;
  if (ctx->STORAGE_MODE()) return AuthQuery::Privilege::STORAGE_MODE;
  if (ctx->MULTI_DATABASE_EDIT()) return AuthQuery::Privilege::MULTI_DATABASE_EDIT;
  if (ctx->MULTI_DATABASE_USE()) return AuthQuery::Privilege::MULTI_DATABASE_USE;
  if (ctx->COORDINATOR()) return AuthQuery::Privilege::COORDINATOR;
  if (ctx->IMPERSONATE_USER()) return AuthQuery::Privilege::IMPERSONATE_USER;
  if (ctx->PROFILE_RESTRICTION()) return AuthQuery::Privilege::PROFILE_RESTRICTION;
  LOG_FATAL("Should not get here - unknown privilege!");
}

/**
 * @return AuthQuery::FineGrainedPrivilege
 */
antlrcpp::Any CypherMainVisitor::visitGranularPrivilege(MemgraphCypher::GranularPrivilegeContext *ctx) {
  if (ctx->NOTHING()) return AuthQuery::FineGrainedPrivilege::NOTHING;
  if (ctx->READ()) return AuthQuery::FineGrainedPrivilege::READ;
  if (ctx->UPDATE()) return AuthQuery::FineGrainedPrivilege::UPDATE;
  if (ctx->CREATE_DELETE()) return AuthQuery::FineGrainedPrivilege::CREATE_DELETE;
  LOG_FATAL("Should not get here - unknown fine grained privilege!");
}

/**
 * @return EntityType
 */
antlrcpp::Any CypherMainVisitor::visitEntityType(MemgraphCypher::EntityTypeContext *ctx) {
  if (ctx->LABELS()) return EntityType::LABELS;
  if (ctx->EDGE_TYPES()) return EntityType::EDGE_TYPES;
  LOG_FATAL("Should not get here - unknown entity type!");
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowPrivileges(MemgraphCypher::ShowPrivilegesContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_PRIVILEGES;
  auth->user_or_role_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));

  // Handle optional ON clause
  if (ctx->ON()) {
    if (ctx->DATABASE()) {
      auth->database_specification_ = AuthQuery::DatabaseSpecification::DATABASE;
      auth->database_ = std::any_cast<std::string>(ctx->db->accept(this));
    } else if (ctx->MAIN()) {
      auth->database_specification_ = AuthQuery::DatabaseSpecification::MAIN;
    } else if (ctx->CURRENT()) {
      auth->database_specification_ = AuthQuery::DatabaseSpecification::CURRENT;
    }
  } else {
    auth->database_specification_ = AuthQuery::DatabaseSpecification::NONE;
  }

  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_ROLE_FOR_USER;
  auth->user_ = std::any_cast<std::string>(ctx->user->accept(this));

  // Handle optional ON clause
  if (ctx->ON()) {
    if (ctx->DATABASE()) {
      auth->database_specification_ = AuthQuery::DatabaseSpecification::DATABASE;
      auth->database_ = std::any_cast<std::string>(ctx->db->accept(this));
    } else if (ctx->MAIN()) {
      auth->database_specification_ = AuthQuery::DatabaseSpecification::MAIN;
    } else if (ctx->CURRENT()) {
      auth->database_specification_ = AuthQuery::DatabaseSpecification::CURRENT;
    }
  }

  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_USERS_FOR_ROLE;
  auth->roles_ = {std::any_cast<std::string>(ctx->role->accept(this))};
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitGrantDatabaseToUserOrRole(MemgraphCypher::GrantDatabaseToUserOrRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::GRANT_DATABASE_TO_USER;
  auth->database_ = std::any_cast<std::string>(ctx->wildcardName()->accept(this));
  auth->user_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitDenyDatabaseFromUserOrRole(
    MemgraphCypher::DenyDatabaseFromUserOrRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::DENY_DATABASE_FROM_USER;
  auth->database_ = std::any_cast<std::string>(ctx->wildcardName()->accept(this));
  auth->user_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitRevokeDatabaseFromUserOrRole(
    MemgraphCypher::RevokeDatabaseFromUserOrRoleContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::REVOKE_DATABASE_FROM_USER;
  auth->database_ = std::any_cast<std::string>(ctx->wildcardName()->accept(this));
  auth->user_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitShowDatabasePrivileges(MemgraphCypher::ShowDatabasePrivilegesContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SHOW_DATABASE_PRIVILEGES;
  auth->user_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  return auth;
}

/**
 * @return AuthQuery*
 */
antlrcpp::Any CypherMainVisitor::visitSetMainDatabase(MemgraphCypher::SetMainDatabaseContext *ctx) {
  auto *auth = storage_->Create<AuthQuery>();
  auth->action_ = AuthQuery::Action::SET_MAIN_DATABASE;
  auth->database_ = std::any_cast<std::string>(ctx->db->accept(this));
  auth->user_ = std::any_cast<std::string>(ctx->userOrRole->accept(this));
  return auth;
}

antlrcpp::Any CypherMainVisitor::visitCypherReturn(MemgraphCypher::CypherReturnContext *ctx) {
  auto *return_clause = storage_->Create<Return>();
  return_clause->body_ = std::any_cast<ReturnBody>(ctx->returnBody()->accept(this));
  if (ctx->DISTINCT()) {
    return_clause->body_.distinct = true;
  }
  return return_clause;
}

antlrcpp::Any CypherMainVisitor::visitReturnBody(MemgraphCypher::ReturnBodyContext *ctx) {
  ReturnBody body;
  if (ctx->order()) {
    body.order_by = std::any_cast<std::vector<SortItem>>(ctx->order()->accept(this));
  }
  if (ctx->skip()) {
    body.skip = static_cast<Expression *>(std::any_cast<Expression *>(ctx->skip()->accept(this)));
  }
  if (ctx->limit()) {
    if (ctx->limit()->expression()) {
      body.limit = std::any_cast<Expression *>(ctx->limit()->accept(this));
    } else {
      body.limit = std::any_cast<ParameterLookup *>(ctx->limit()->accept(this));
    }
  }
  std::tie(body.all_identifiers, body.named_expressions) =
      std::any_cast<std::pair<bool, std::vector<NamedExpression *>>>(ctx->returnItems()->accept(this));
  return body;
}

antlrcpp::Any CypherMainVisitor::visitReturnItems(MemgraphCypher::ReturnItemsContext *ctx) {
  std::vector<NamedExpression *> named_expressions;
  for (auto *item : ctx->returnItem()) {
    named_expressions.push_back(std::any_cast<NamedExpression *>(item->accept(this)));
  }
  return std::pair<bool, std::vector<NamedExpression *>>(ctx->getTokens(MemgraphCypher::ASTERISK).size(),
                                                         named_expressions);
}

antlrcpp::Any CypherMainVisitor::visitReturnItem(MemgraphCypher::ReturnItemContext *ctx) {
  auto *named_expr = storage_->Create<NamedExpression>();
  named_expr->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
  MG_ASSERT(named_expr->expression_);
  if (ctx->variable()) {
    named_expr->is_aliased_ = true;
    named_expr->name_ = std::string(std::any_cast<std::string>(ctx->variable()->accept(this)));
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
    order_by.push_back(std::any_cast<SortItem>(sort_item->accept(this)));
  }
  return order_by;
}

antlrcpp::Any CypherMainVisitor::visitSortItem(MemgraphCypher::SortItemContext *ctx) {
  return SortItem{ctx->DESC() || ctx->DESCENDING() ? Ordering::DESC : Ordering::ASC,
                  std::any_cast<Expression *>(ctx->expression()->accept(this))};
}

antlrcpp::Any CypherMainVisitor::visitNodePattern(MemgraphCypher::NodePatternContext *ctx) {
  auto *node = storage_->Create<NodeAtom>();
  if (ctx->variable()) {
    auto variable = std::any_cast<std::string>(ctx->variable()->accept(this));
    node->identifier_ = storage_->Create<Identifier>(variable);
    users_identifiers.insert(variable);
  } else {
    anonymous_identifiers.push_back(&node->identifier_);
  }
  if (ctx->nodeLabels()) {
    node->labels_ = std::any_cast<std::vector<QueryLabelType>>(ctx->nodeLabels()->accept(this));
  }
  if (ctx->labelExpression()) {
    auto labels_set = std::any_cast<std::unordered_set<LabelIx>>(ctx->labelExpression()->accept(this));
    node->labels_.reserve(labels_set.size());
    node->labels_.insert(node->labels_.end(), labels_set.begin(), labels_set.end());
    node->label_expression_ = true;
  }
  if (ctx->properties()) {
    // This can return either properties or parameters
    if (ctx->properties()->mapLiteral()) {
      node->properties_ = std::any_cast<std::unordered_map<PropertyIx, Expression *>>(ctx->properties()->accept(this));
    } else {
      node->properties_ = std::any_cast<ParameterLookup *>(ctx->properties()->accept(this));
    }
  }
  return node;
}

antlrcpp::Any CypherMainVisitor::visitNodeLabels(MemgraphCypher::NodeLabelsContext *ctx) {
  std::vector<QueryLabelType> labels;
  for (auto *node_label : ctx->nodeLabel()) {
    auto *label_name = node_label->labelName();
    if (label_name->symbolicName()) {
      labels.emplace_back(AddLabel(std::any_cast<std::string>(node_label->accept(this))));
    } else if (label_name->parameter()) {
      // If we have a parameter, we have to resolve it.
      const auto *param_lookup = std::any_cast<ParameterLookup *>(node_label->accept(this));
      const auto &param_property = parameters_->AtTokenPosition(param_lookup->token_position_);

      if (param_property.IsString()) {
        const auto &label_name = param_property.ValueString();
        labels.emplace_back(storage_->GetLabelIx(label_name));
      } else if (param_property.IsList()) {
        const auto labels_list = param_property.ValueList();
        for (const auto &label_name : labels_list) {
          if (!label_name.IsString()) {
            throw SyntaxException("Dynamic node labels must be of type STRING!");
          }
          labels.emplace_back(storage_->GetLabelIx(label_name.ValueString()));
        }
      } else {
        throw SyntaxException("Parameter for dynamic node labels must be of type STRING or LIST[STRING]");
      }

      // We can't cache queries with label parameters because these parameters are resolved during the parsing stage.
      // The same parameter could be resolved to different values if the user changes its value.
      query_info_.is_cacheable = false;
    } else {
      auto variable = std::any_cast<std::string>(label_name->variable()->accept(this));
      users_identifiers.insert(variable);
      auto *expression = static_cast<Expression *>(storage_->Create<Identifier>(variable));
      for (auto *lookup : label_name->propertyLookup()) {
        auto key = std::any_cast<PropertyIx>(lookup->accept(this));
        auto *property_lookup = storage_->Create<PropertyLookup>(expression, key);
        expression = property_lookup;
      }
      labels.emplace_back(expression);
    }
  }
  return labels;
}

antlrcpp::Any CypherMainVisitor::visitLabelExpression(MemgraphCypher::LabelExpressionContext *ctx) {
  std::unordered_set<LabelIx> labels;
  for (auto *label : ctx->symbolicName()) {
    labels.emplace(AddLabel(std::any_cast<std::string>(label->accept(this))));
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
    auto key = std::any_cast<PropertyIx>(ctx->propertyKeyName()[i]->accept(this));
    auto *value = std::any_cast<Expression *>(ctx->expression()[i]->accept(this));
    if (!map.insert({key, value}).second) {
      throw SemanticException("Same key can't appear twice in a map literal.");
    }
  }
  return map;
}

antlrcpp::Any CypherMainVisitor::visitMapProjectionLiteral(MemgraphCypher::MapProjectionLiteralContext *ctx) {
  MapProjectionData map_projection_data;

  map_projection_data.map_variable =
      storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
  for (auto *map_el : ctx->mapElement()) {
    if (map_el->propertyLookup()) {
      auto key = std::any_cast<PropertyIx>(map_el->propertyLookup()->propertyKeyName()->accept(this));
      auto property = std::any_cast<PropertyIx>(map_el->propertyLookup()->accept(this));
      auto *property_lookup = storage_->Create<PropertyLookup>(map_projection_data.map_variable, property);
      map_projection_data.elements.insert_or_assign(key, property_lookup);
    }
    if (map_el->allPropertiesLookup()) {
      auto key = AddProperty("*");
      auto *all_properties_lookup = storage_->Create<AllPropertiesLookup>(map_projection_data.map_variable);
      map_projection_data.elements.insert_or_assign(key, all_properties_lookup);
    }
    if (map_el->variable()) {
      auto key = AddProperty(std::any_cast<std::string>(map_el->variable()->accept(this)));
      auto *variable = storage_->Create<Identifier>(std::any_cast<std::string>(map_el->variable()->accept(this)));
      map_projection_data.elements.insert_or_assign(key, variable);
    }
    if (map_el->propertyKeyValuePair()) {
      auto key = std::any_cast<PropertyIx>(map_el->propertyKeyValuePair()->propertyKeyName()->accept(this));
      auto *value = std::any_cast<Expression *>(map_el->propertyKeyValuePair()->expression()->accept(this));
      map_projection_data.elements.insert_or_assign(key, value);
    }
  }

  return map_projection_data;
}

antlrcpp::Any CypherMainVisitor::visitListLiteral(MemgraphCypher::ListLiteralContext *ctx) {
  std::vector<Expression *> expressions;
  for (auto *expr_ctx : ctx->expression()) {
    expressions.push_back(std::any_cast<Expression *>(expr_ctx->accept(this)));
  }
  return expressions;
}

antlrcpp::Any CypherMainVisitor::visitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *ctx) {
  return AddProperty(std::any_cast<std::string>(visitChildren(ctx)));
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
    patterns.push_back(std::any_cast<Pattern *>(pattern_part->accept(this)));
  }
  return patterns;
}

antlrcpp::Any CypherMainVisitor::visitPatternPart(MemgraphCypher::PatternPartContext *ctx) {
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

antlrcpp::Any CypherMainVisitor::visitForcePatternPart(MemgraphCypher::ForcePatternPartContext *ctx) {
  auto *pattern = std::any_cast<Pattern *>(ctx->relationshipsPattern()->accept(this));
  if (ctx->variable()) {
    auto variable = std::any_cast<std::string>(ctx->variable()->accept(this));
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
  auto *pattern = storage_->Create<Pattern>();
  pattern->atoms_.push_back(std::any_cast<NodeAtom *>(ctx->nodePattern()->accept(this)));
  for (auto *pattern_element_chain : ctx->patternElementChain()) {
    auto element = std::any_cast<std::pair<PatternAtom *, PatternAtom *>>(pattern_element_chain->accept(this));
    pattern->atoms_.push_back(element.first);
    pattern->atoms_.push_back(element.second);
  }
  return pattern;
}

antlrcpp::Any CypherMainVisitor::visitRelationshipsPattern(MemgraphCypher::RelationshipsPatternContext *ctx) {
  auto *pattern = storage_->Create<Pattern>();
  pattern->atoms_.push_back(std::any_cast<NodeAtom *>(ctx->nodePattern()->accept(this)));
  for (auto *pattern_element_chain : ctx->patternElementChain()) {
    auto element = std::any_cast<std::pair<PatternAtom *, PatternAtom *>>(pattern_element_chain->accept(this));
    pattern->atoms_.push_back(element.first);
    pattern->atoms_.push_back(element.second);
  }
  anonymous_identifiers.push_back(&pattern->identifier_);
  return pattern;
}

antlrcpp::Any CypherMainVisitor::visitPatternElementChain(MemgraphCypher::PatternElementChainContext *ctx) {
  return std::pair<PatternAtom *, PatternAtom *>(std::any_cast<EdgeAtom *>(ctx->relationshipPattern()->accept(this)),
                                                 std::any_cast<NodeAtom *>(ctx->nodePattern()->accept(this)));
}

antlrcpp::Any CypherMainVisitor::visitRelationshipPattern(MemgraphCypher::RelationshipPatternContext *ctx) {
  auto *edge = storage_->Create<EdgeAtom>();

  auto relationshipDetail = ctx->relationshipDetail();
  auto *variableExpansion = relationshipDetail ? relationshipDetail->variableExpansion() : nullptr;
  edge->type_ = EdgeAtom::Type::SINGLE;
  if (variableExpansion)
    std::tie(edge->type_, edge->lower_bound_, edge->upper_bound_, edge->limit_) =
        std::any_cast<std::tuple<EdgeAtom::Type, Expression *, Expression *, Expression *>>(
            variableExpansion->accept(this));

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
        std::any_cast<std::vector<QueryEdgeType>>(ctx->relationshipDetail()->relationshipTypes()->accept(this));
  }

  auto relationshipLambdas = relationshipDetail->relationshipLambda();
  if (variableExpansion) {
    if (relationshipDetail->total_weight && edge->type_ != EdgeAtom::Type::WEIGHTED_SHORTEST_PATH &&
        edge->type_ != EdgeAtom::Type::ALL_SHORTEST_PATHS)
      throw SemanticException(
          "Variable for total weight is allowed only with weighted and all shortest "
          "path expansion.");
    auto visit_lambda = [this](auto *lambda) {
      EdgeAtom::Lambda edge_lambda;
      auto traversed_edge_variable = std::any_cast<std::string>(lambda->traversed_edge->accept(this));
      edge_lambda.inner_edge = storage_->Create<Identifier>(traversed_edge_variable);
      auto traversed_node_variable = std::any_cast<std::string>(lambda->traversed_node->accept(this));
      edge_lambda.inner_node = storage_->Create<Identifier>(traversed_node_variable);
      if (lambda->accumulated_path) {
        auto accumulated_path_variable = std::any_cast<std::string>(lambda->accumulated_path->accept(this));
        edge_lambda.accumulated_path = storage_->Create<Identifier>(accumulated_path_variable);

        if (lambda->accumulated_weight) {
          auto accumulated_weight_variable = std::any_cast<std::string>(lambda->accumulated_weight->accept(this));
          edge_lambda.accumulated_weight = storage_->Create<Identifier>(accumulated_weight_variable);
        }
      }
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
          throw SemanticException(
              "Lambda for calculating weights is mandatory with weighted "
              "shortest path expansion.");
        else if (edge->type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS)
          throw SemanticException(
              "Lambda for calculating weights is mandatory with all "
              "shortest paths expansion.");
        // In variable expansion inner variables are mandatory.
        anonymous_identifiers.push_back(&edge->filter_lambda_.inner_edge);
        anonymous_identifiers.push_back(&edge->filter_lambda_.inner_node);

        // TODO: In what use case do we need accumulated path and weight here?
        if (edge->filter_lambda_.accumulated_path) {
          anonymous_identifiers.push_back(&edge->filter_lambda_.accumulated_path);

          if (edge->filter_lambda_.accumulated_weight) {
            anonymous_identifiers.push_back(&edge->filter_lambda_.accumulated_weight);
          }
        }
        break;
      case 1:
        if (edge->type_ == EdgeAtom::Type::KSHORTEST) {
          throw SemanticException("KSHORTEST expansion does not support filter lambda.");
        }
        if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH ||
            edge->type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
          // For wShortest and allShortest, the first (and required) lambda is
          // used for weight calculation.
          edge->weight_lambda_ = visit_lambda(relationshipLambdas[0]);
          visit_total_weight();
          // Add mandatory inner variables for filter lambda.
          anonymous_identifiers.push_back(&edge->filter_lambda_.inner_edge);
          anonymous_identifiers.push_back(&edge->filter_lambda_.inner_node);
          if (edge->filter_lambda_.accumulated_path) {
            anonymous_identifiers.push_back(&edge->filter_lambda_.accumulated_path);

            if (edge->filter_lambda_.accumulated_weight) {
              anonymous_identifiers.push_back(&edge->filter_lambda_.accumulated_weight);
            }
          }
        } else {
          // Other variable expands only have the filter lambda.
          edge->filter_lambda_ = visit_lambda(relationshipLambdas[0]);
          if (edge->filter_lambda_.accumulated_weight) {
            throw SemanticException(
                "Accumulated weight in filter lambda can be used only with "
                "shortest paths expansion.");
          }
        }
        break;
      case 2:
        if (edge->type_ != EdgeAtom::Type::WEIGHTED_SHORTEST_PATH && edge->type_ != EdgeAtom::Type::ALL_SHORTEST_PATHS)
          throw SemanticException("Only one filter lambda can be supplied.");
        edge->weight_lambda_ = visit_lambda(relationshipLambdas[0]);
        visit_total_weight();
        edge->filter_lambda_ = visit_lambda(relationshipLambdas[1]);
        break;
      default:
        throw SemanticException("Only one filter lambda can be supplied.");
    }
    if (edge->type_ != EdgeAtom::Type::KSHORTEST && edge->limit_ != nullptr) {
      throw SemanticException("Number of paths limit is only supported with KSHORTEST path expansion.");
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
        edge->properties_ = std::any_cast<std::unordered_map<PropertyIx, Expression *>>(properties[0]->accept(this));
        break;
      }
      MG_ASSERT(properties[0]->parameter());
      edge->properties_ = std::any_cast<ParameterLookup *>(properties[0]->accept(this));
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
  std::vector<QueryEdgeType> types;
  for (auto *edge_type : ctx->relTypeName()) {
    if (edge_type->symbolicName()) {
      types.emplace_back(AddEdgeType(std::any_cast<std::string>(edge_type->accept(this))));
    } else if (edge_type->parameter()) {
      // If we have a parameter, we have to resolve it.
      const auto *param_lookup = std::any_cast<ParameterLookup *>(edge_type->accept(this));
      const auto edge_type_name = parameters_->AtTokenPosition(param_lookup->token_position_).ValueString();
      types.emplace_back(storage_->GetEdgeTypeIx(edge_type_name));

      // We can't cache queries with edge type parameters because these parameters are resolved during the parsing
      // stage. The same parameter could be resolved to different values if the user changes its value.
      query_info_.is_cacheable = false;
    } else {
      auto variable = std::any_cast<std::string>(edge_type->variable()->accept(this));
      users_identifiers.insert(variable);
      auto *expression = static_cast<Expression *>(storage_->Create<Identifier>(variable));
      for (auto *lookup : edge_type->propertyLookup()) {
        auto key = std::any_cast<PropertyIx>(lookup->accept(this));
        auto *property_lookup = storage_->Create<PropertyLookup>(expression, key);
        expression = property_lookup;
      }
      types.emplace_back(expression);
    }
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
  else if (!ctx->getTokens(MemgraphCypher::ALLSHORTEST).empty())
    edge_type = EdgeAtom::Type::ALL_SHORTEST_PATHS;
  else if (!ctx->getTokens(MemgraphCypher::KSHORTEST).empty())
    edge_type = EdgeAtom::Type::KSHORTEST;
  Expression *lower = nullptr;
  Expression *upper = nullptr;

  Expression *limit = nullptr;
  size_t n_expressions = ctx->expression().size();
  if (ctx->k) {
    --n_expressions;  // Last expression is the limit
    limit = std::any_cast<Expression *>(ctx->k->accept(this));
  }

  if (n_expressions == 0U) {
    // Case -[*]-
  } else if (n_expressions == 1U) {
    auto dots_tokens = ctx->getTokens(MemgraphCypher::DOTS);
    auto *bound = std::any_cast<Expression *>(ctx->expression()[0]->accept(this));
    if (!dots_tokens.size()) {
      // Case -[*bound]-
      if (edge_type != EdgeAtom::Type::WEIGHTED_SHORTEST_PATH && edge_type != EdgeAtom::Type::ALL_SHORTEST_PATHS)
        lower = bound;
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
  if (lower &&
      (edge_type == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH || edge_type == EdgeAtom::Type::ALL_SHORTEST_PATHS)) {
    throw SemanticException("Lower bound is not allowed in weighted or all shortest path expansion.");
  }

  if (limit && edge_type != EdgeAtom::Type::KSHORTEST) {
    throw SemanticException("Limit parameter is only supported with KSHORTEST path expansion.");
  }

  return std::make_tuple(edge_type, lower, upper, limit);
}

antlrcpp::Any CypherMainVisitor::visitExpression(MemgraphCypher::ExpressionContext *ctx) {
  return std::any_cast<Expression *>(ctx->expression12()->accept(this));
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
  children.push_back(std::any_cast<Expression *>(ctx->expression7()->accept(this)));
  std::vector<size_t> operators;
  auto partial_comparison_expressions = ctx->partialComparisonExpression();
  for (auto *child : partial_comparison_expressions) {
    children.push_back(std::any_cast<Expression *>(child->expression7()->accept(this)));
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

// Exponentiation.
antlrcpp::Any CypherMainVisitor::visitExpression5(MemgraphCypher::Expression5Context *ctx) {
  if (ctx->expression4().size() > 1U) {
    return LeftAssociativeOperatorExpression(ctx->expression4(), ctx->children, {MemgraphCypher::CARET});
  }
  return visitChildren(ctx);
}

// Unary minus and plus.
antlrcpp::Any CypherMainVisitor::visitExpression4(MemgraphCypher::Expression4Context *ctx) {
  return PrefixUnaryOperator(ctx->expression3a(), ctx->children, {MemgraphCypher::PLUS, MemgraphCypher::MINUS});
}

// IS NULL, IS NOT NULL, STARTS WITH, ..
antlrcpp::Any CypherMainVisitor::visitExpression3a(MemgraphCypher::Expression3aContext *ctx) {
  auto *expression = std::any_cast<Expression *>(ctx->expression2a()->accept(this));

  for (auto *op : ctx->stringAndNullOperators()) {
    if (op->IS() && op->NOT() && op->CYPHERNULL()) {
      expression =
          static_cast<Expression *>(storage_->Create<NotOperator>(storage_->Create<IsNullOperator>(expression)));
    } else if (op->IS() && op->CYPHERNULL()) {
      expression = static_cast<Expression *>(storage_->Create<IsNullOperator>(expression));
    } else if (op->IN()) {
      expression = static_cast<Expression *>(
          storage_->Create<InListOperator>(expression, std::any_cast<Expression *>(op->expression2a()->accept(this))));
    } else if (utils::StartsWith(op->getText(), "=~")) {
      auto *regex_match = storage_->Create<RegexMatch>();
      regex_match->string_expr_ = expression;
      regex_match->regex_ = std::any_cast<Expression *>(op->expression2a()->accept(this));
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
      auto *expression2 = std::any_cast<Expression *>(op->expression2a()->accept(this));
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

antlrcpp::Any CypherMainVisitor::visitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext *) {
  DLOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitExpression2a(MemgraphCypher::Expression2aContext *ctx) {
  auto *expression = std::any_cast<Expression *>(ctx->expression2b()->accept(this));
  if (ctx->nodeLabels()) {
    auto labels = std::any_cast<std::vector<QueryLabelType>>(ctx->nodeLabels()->accept(this));
    expression = storage_->Create<LabelsTest>(expression, labels);
  }
  return expression;
}

antlrcpp::Any CypherMainVisitor::visitExpression2b(MemgraphCypher::Expression2bContext *ctx) {
  auto *expression = std::any_cast<Expression *>(ctx->atom()->accept(this));
  for (auto *member_access : ctx->memberAccess()) {
    if (auto *list_op = member_access->listIndexingOrSlicing()) {
      if (list_op->getTokens(MemgraphCypher::DOTS).size() == 0U) {
        // If there is no '..' then we need to create list indexing operator.
        expression = storage_->Create<SubscriptOperator>(
            expression, std::any_cast<Expression *>(list_op->expression()[0]->accept(this)));
      } else if (!list_op->lower_bound && !list_op->upper_bound) {
        throw SemanticException("List slicing operator requires at least one bound.");
      } else {
        Expression *lower_bound_ast =
            list_op->lower_bound ? std::any_cast<Expression *>(list_op->lower_bound->accept(this)) : nullptr;
        Expression *upper_bound_ast =
            list_op->upper_bound ? std::any_cast<Expression *>(list_op->upper_bound->accept(this)) : nullptr;
        expression = storage_->Create<ListSlicingOperator>(expression, lower_bound_ast, upper_bound_ast);
      }
    } else if (auto *lookup = member_access->propertyLookup()) {
      auto key = std::any_cast<PropertyIx>(lookup->accept(this));
      auto property_lookup = storage_->Create<PropertyLookup>(expression, key);
      expression = property_lookup;
    }
  }

  return expression;
}

antlrcpp::Any CypherMainVisitor::visitAtom(MemgraphCypher::AtomContext *ctx) {
  if (ctx->literal()) {
    return ctx->literal()->accept(this);
  } else if (ctx->parameter()) {
    return static_cast<Expression *>(std::any_cast<ParameterLookup *>(ctx->parameter()->accept(this)));
  } else if (ctx->parenthesizedExpression()) {
    return static_cast<Expression *>(std::any_cast<Expression *>(ctx->parenthesizedExpression()->accept(this)));
  } else if (ctx->variable()) {
    auto variable = std::any_cast<std::string>(ctx->variable()->accept(this));
    users_identifiers.insert(variable);
    return static_cast<Expression *>(storage_->Create<Identifier>(variable));
  } else if (ctx->existsExpression()) {
    return std::any_cast<Expression *>(ctx->existsExpression()->accept(this));
  } else if (ctx->existsSubquery()) {
    return std::any_cast<Expression *>(ctx->existsSubquery()->accept(this));
  } else if (ctx->functionInvocation()) {
    return std::any_cast<Expression *>(ctx->functionInvocation()->accept(this));
  } else if (ctx->COALESCE()) {
    std::vector<Expression *> exprs;
    for (auto *expr_context : ctx->expression()) {
      exprs.emplace_back(std::any_cast<Expression *>(expr_context->accept(this)));
    }
    return static_cast<Expression *>(storage_->Create<Coalesce>(std::move(exprs)));
  } else if (ctx->COUNT()) {
    // Here we handle COUNT(*). COUNT(expression) is handled in
    // visitFunctionInvocation with other aggregations. This is visible in
    // functionInvocation and atom production in opencypher grammar.
    return static_cast<Expression *>(storage_->Create<Aggregation>(nullptr, nullptr, Aggregation::Op::COUNT, false));
  } else if (ctx->ALL()) {
    auto *ident = storage_->Create<Identifier>(
        std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
    auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("ALL(...) requires a WHERE predicate.");
    }
    auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
    return static_cast<Expression *>(storage_->Create<All>(ident, list_expr, where));
  } else if (ctx->SINGLE()) {
    auto *ident = storage_->Create<Identifier>(
        std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
    auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("SINGLE(...) requires a WHERE predicate.");
    }
    auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
    return static_cast<Expression *>(storage_->Create<Single>(ident, list_expr, where));
  } else if (ctx->ANY()) {
    auto *ident = storage_->Create<Identifier>(
        std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
    auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("ANY(...) requires a WHERE predicate.");
    }
    auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
    return static_cast<Expression *>(storage_->Create<Any>(ident, list_expr, where));
  } else if (ctx->NONE()) {
    auto *ident = storage_->Create<Identifier>(
        std::any_cast<std::string>(ctx->filterExpression()->idInColl()->variable()->accept(this)));
    auto *list_expr = std::any_cast<Expression *>(ctx->filterExpression()->idInColl()->expression()->accept(this));
    if (!ctx->filterExpression()->where()) {
      throw SyntaxException("NONE(...) requires a WHERE predicate.");
    }
    auto *where = std::any_cast<Where *>(ctx->filterExpression()->where()->accept(this));
    return static_cast<Expression *>(storage_->Create<None>(ident, list_expr, where));
  } else if (ctx->REDUCE()) {
    auto *accumulator =
        storage_->Create<Identifier>(std::any_cast<std::string>(ctx->reduceExpression()->accumulator->accept(this)));
    auto *initializer = std::any_cast<Expression *>(ctx->reduceExpression()->initial->accept(this));
    auto *ident = storage_->Create<Identifier>(
        std::any_cast<std::string>(ctx->reduceExpression()->idInColl()->variable()->accept(this)));
    auto *list = std::any_cast<Expression *>(ctx->reduceExpression()->idInColl()->expression()->accept(this));
    auto *expr = std::any_cast<Expression *>(ctx->reduceExpression()->expression().back()->accept(this));
    return static_cast<Expression *>(storage_->Create<Reduce>(accumulator, initializer, ident, list, expr));
  } else if (ctx->caseExpression()) {
    return std::any_cast<Expression *>(ctx->caseExpression()->accept(this));
  } else if (ctx->extractExpression()) {
    auto *ident = storage_->Create<Identifier>(
        std::any_cast<std::string>(ctx->extractExpression()->idInColl()->variable()->accept(this)));
    auto *list = std::any_cast<Expression *>(ctx->extractExpression()->idInColl()->expression()->accept(this));
    auto *expr = std::any_cast<Expression *>(ctx->extractExpression()->expression()->accept(this));
    return static_cast<Expression *>(storage_->Create<Extract>(ident, list, expr));
  } else if (ctx->patternComprehension()) {
    return std::any_cast<Expression *>(ctx->patternComprehension()->accept(this));
  } else if (ctx->enumValueAccess()) {
    auto const enum_value_access_parts = ctx->enumValueAccess()->symbolicName();
    if (enum_value_access_parts.size() != 2) {
      throw SyntaxException("Enum value access should be in the form of 'enum_name::enum_value'");
    }
    auto enum_name = std::any_cast<std::string>(enum_value_access_parts[0]->accept(this));
    auto enum_value = std::any_cast<std::string>(enum_value_access_parts[1]->accept(this));

    return static_cast<Expression *>(storage_->Create<EnumValueAccess>(std::move(enum_name), std::move(enum_value)));
  } else if (ctx->listComprehension()) {
    auto *ident = storage_->Create<Identifier>(
        std::any_cast<std::string>(ctx->listComprehension()->filterExpression()->idInColl()->variable()->accept(this)));
    auto *list = std::any_cast<Expression *>(
        ctx->listComprehension()->filterExpression()->idInColl()->expression()->accept(this));
    auto *where = ctx->listComprehension()->filterExpression()->where()
                      ? std::any_cast<Where *>(ctx->listComprehension()->filterExpression()->where()->accept(this))
                      : nullptr;
    auto *expr = ctx->listComprehension()->expression()
                     ? std::any_cast<Expression *>(ctx->listComprehension()->expression()->accept(this))
                     : nullptr;
    return static_cast<Expression *>(storage_->Create<ListComprehension>(ident, list, where, expr));
  }

  // NOTE: Memgraph does NOT support patterns under filtering.
  // To test run, e.g. MATCH (c) WHERE NOT ((c)-[:EdgeType]->(d)) RETURN c;
  throw utils::NotYetImplemented(
      "atom expression '{}'. Try to rewrite the query by using OPTIONAL MATCH, WITH and WHERE clauses.",
      ctx->getText());
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
          std::any_cast<std::string>(visitStringLiteral(std::any_cast<std::string>(ctx->StringLiteral()->getText()))),
          token_position));
    } else if (ctx->booleanLiteral()) {
      return static_cast<Expression *>(
          storage_->Create<PrimitiveLiteral>(std::any_cast<bool>(ctx->booleanLiteral()->accept(this)), token_position));
    } else if (ctx->numberLiteral()) {
      return static_cast<Expression *>(storage_->Create<PrimitiveLiteral>(
          std::any_cast<TypedValue>(ctx->numberLiteral()->accept(this)), token_position));
    }
    LOG_FATAL("Expected to handle all cases above");
  } else if (ctx->listLiteral()) {
    return static_cast<Expression *>(
        storage_->Create<ListLiteral>(std::any_cast<std::vector<Expression *>>(ctx->listLiteral()->accept(this))));
  } else if (ctx->mapProjectionLiteral()) {
    auto map_projection_data = std::any_cast<MapProjectionData>(ctx->mapProjectionLiteral()->accept(this));
    return static_cast<Expression *>(storage_->Create<MapProjectionLiteral>(map_projection_data.map_variable,
                                                                            std::move(map_projection_data.elements)));
  } else {
    return static_cast<Expression *>(storage_->Create<MapLiteral>(
        std::any_cast<std::unordered_map<PropertyIx, Expression *>>(ctx->mapLiteral()->accept(this))));
  }
  return visitChildren(ctx);
}

antlrcpp::Any CypherMainVisitor::visitExistsExpression(MemgraphCypher::ExistsExpressionContext *ctx) {
  auto *exists = storage_->Create<Exists>();
  // Pattern form: ( ... ) or { ... } with forcePatternPart
  if (ctx->forcePatternPart()) {
    exists->content_ = std::any_cast<Pattern *>(ctx->forcePatternPart()->accept(this));
    if (exists->GetPattern()->identifier_) {
      throw SyntaxException("Identifiers are not supported in exists(...).");
    }
  } else {
    throw SyntaxException("EXISTS supports only a single relation or a subquery as its input.");
  }

  // Ensure only one of pattern_ or subquery_ is set
  const bool has_pattern = exists->HasPattern();
  const bool has_subquery = exists->HasSubquery();
  if ((has_pattern && has_subquery) || (!has_pattern && !has_subquery)) {
    throw SyntaxException(
        "EXISTS must have exactly one of pattern or subquery set. Please contact Memgraph support as this scenario "
        "should not happen!");
  }

  return static_cast<Expression *>(exists);
}

antlrcpp::Any CypherMainVisitor::visitExistsSubquery(MemgraphCypher::ExistsSubqueryContext *ctx) {
  auto *exists = storage_->Create<Exists>();
  // Pattern form: ( ... ) or { ... } with forcePatternPart
  if (ctx->forcePatternPart()) {
    exists->content_ = std::any_cast<Pattern *>(ctx->forcePatternPart()->accept(this));
    if (exists->GetPattern()->identifier_) {
      throw SyntaxException("Identifiers are not supported in exists(...).");
    }
  } else if (ctx->cypherQuery()) {
    // Curly-brace subquery form: { cypherQuery }
    // Set the flag to indicate we are parsing an EXISTS subquery
    auto old_flag = parsing_exists_subquery_;
    parsing_exists_subquery_ = true;
    auto *cypher_query = std::any_cast<CypherQuery *>(ctx->cypherQuery()->accept(this));
    parsing_exists_subquery_ = old_flag;
    exists->content_ = cypher_query;

    // 1. There must be at least one clause
    auto *single_query = cypher_query->single_query_;
    if (!single_query || single_query->clauses_.empty()) {
      throw SyntaxException("EXISTS subquery must contain at least one clause.");
    }

    // 2. Only MATCH, WHERE, WITH, RETURN allowed
    for (const auto *clause : single_query->clauses_) {
      const auto &type = clause->GetTypeInfo();
      if (!(utils::IsSubtype(type, Match::kType) || utils::IsSubtype(type, Where::kType) ||
            utils::IsSubtype(type, With::kType) || utils::IsSubtype(type, Return::kType))) {
        throw SyntaxException("Only MATCH, WHERE, WITH, and RETURN clauses are allowed in EXISTS subqueries.");
      }
    }

    // 3. No query memory limit
    if (cypher_query->memory_limit_ != nullptr) {
      throw SyntaxException("EXISTS subqueries cannot have a query memory limit.");
    }
  } else {
    throw SyntaxException("EXISTS supports only a single relation or a subquery as its input.");
  }

  // Ensure only one of pattern_ or subquery_ is set
  const bool has_pattern = exists->HasPattern();
  const bool has_subquery = exists->HasSubquery();
  if ((has_pattern && has_subquery) || (!has_pattern && !has_subquery)) {
    throw SyntaxException(
        "EXISTS must have exactly one of pattern or subquery set! Please contact Memgraph support as this scenario "
        "should not happen!");
  }

  return static_cast<Expression *>(exists);
}

antlrcpp::Any CypherMainVisitor::visitPatternComprehension(MemgraphCypher::PatternComprehensionContext *ctx) {
  auto *comprehension = storage_->Create<PatternComprehension>();
  if (ctx->variable()) {
    comprehension->variable_ = storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
  }
  comprehension->pattern_ = std::any_cast<Pattern *>(ctx->relationshipsPattern()->accept(this));
  if (ctx->where()) {
    comprehension->filter_ = std::any_cast<Where *>(ctx->where()->accept(this));
  }
  comprehension->resultExpr_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
  return static_cast<Expression *>(comprehension);
}

antlrcpp::Any CypherMainVisitor::visitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *ctx) {
  return std::any_cast<Expression *>(ctx->expression()->accept(this));
}

antlrcpp::Any CypherMainVisitor::visitNumberLiteral(MemgraphCypher::NumberLiteralContext *ctx) {
  if (ctx->integerLiteral()) {
    return TypedValue(std::any_cast<int64_t>(ctx->integerLiteral()->accept(this)));
  } else if (ctx->doubleLiteral()) {
    return TypedValue(std::any_cast<double>(ctx->doubleLiteral()->accept(this)));
  } else {
    // This should never happen, except grammar changes and we don't notice
    // change in this production.
    DLOG_FATAL("can't happen");
    throw std::exception();
  }
}

antlrcpp::Any CypherMainVisitor::visitFunctionInvocation(MemgraphCypher::FunctionInvocationContext *ctx) {
  const auto is_distinct = ctx->DISTINCT() != nullptr;
  auto function_name = std::any_cast<std::string>(ctx->functionName()->accept(this));
  auto upper_function_name = utils::ToUpperCase(function_name);
  std::vector<Expression *> expressions;
  for (auto *expression : ctx->expression()) {
    expressions.push_back(std::any_cast<Expression *>(expression->accept(this)));
  }

  if (expressions.size() == 1U) {
    if (upper_function_name == Aggregation::kCount) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::COUNT, is_distinct));
    }
    if (upper_function_name == Aggregation::kMin) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::MIN, is_distinct));
    }
    if (upper_function_name == Aggregation::kMax) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::MAX, is_distinct));
    }
    if (upper_function_name == Aggregation::kSum) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::SUM, is_distinct));
    }
    if (upper_function_name == Aggregation::kAvg) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::AVG, is_distinct));
    }
    if (upper_function_name == Aggregation::kCollect) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::COLLECT_LIST, is_distinct));
    }
    if (upper_function_name == Aggregation::kProject) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], nullptr, Aggregation::Op::PROJECT_PATH, is_distinct));
    }
  }

  if (expressions.size() == 2U) {
    if (upper_function_name == Aggregation::kCollect) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[1], expressions[0], Aggregation::Op::COLLECT_MAP, is_distinct));
    }
    if (upper_function_name == Aggregation::kProject) {
      return static_cast<Expression *>(
          storage_->Create<Aggregation>(expressions[0], expressions[1], Aggregation::Op::PROJECT_LISTS, is_distinct));
    }
  }

  auto *function_expr = storage_->Create<Function>(function_name, expressions);

  // Don't cache queries which call user-defined functions. For performance reasons we want to avoid
  // repeativly finding the function at call time, this means user-defined functions have there lifetime
  // tied to the ast Function operation. We do not want that cached, because we want to be able to reload
  // query module that is not currently being used.
  query_info_.is_cacheable &= !function_expr->IsUserDefined();

  // user defined functions are case sensitive, built-in functions work only with upper case
  if (!function_expr->IsUserDefined()) {
    function_expr->function_name_ = upper_function_name;
  }

  return static_cast<Expression *>(function_expr);
}

antlrcpp::Any CypherMainVisitor::visitFunctionName(MemgraphCypher::FunctionNameContext *ctx) { return ctx->getText(); }

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
    del->expressions_.push_back(std::any_cast<Expression *>(expression->accept(this)));
  }
  return del;
}

antlrcpp::Any CypherMainVisitor::visitWhere(MemgraphCypher::WhereContext *ctx) {
  auto *where = storage_->Create<Where>();
  where->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
  return where;
}

antlrcpp::Any CypherMainVisitor::visitSet(MemgraphCypher::SetContext *ctx) {
  std::vector<Clause *> set_items;
  for (auto *set_item : ctx->setItem()) {
    set_items.push_back(std::any_cast<Clause *>(set_item->accept(this)));
  }
  return set_items;
}

antlrcpp::Any CypherMainVisitor::visitSetItem(MemgraphCypher::SetItemContext *ctx) {
  // SetProperty
  if (ctx->propertyExpression()) {
    auto *set_property = storage_->Create<SetProperty>();
    set_property->property_lookup_ = std::any_cast<PropertyLookup *>(ctx->propertyExpression()->accept(this));
    set_property->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
    return static_cast<Clause *>(set_property);
  }

  // SetProperties either assignment or update
  if (ctx->getTokens(MemgraphCypher::EQ).size() || ctx->getTokens(MemgraphCypher::PLUS_EQ).size()) {
    auto *set_properties = storage_->Create<SetProperties>();
    set_properties->identifier_ =
        storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
    set_properties->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
    if (ctx->getTokens(MemgraphCypher::PLUS_EQ).size()) {
      set_properties->update_ = true;
    }
    return static_cast<Clause *>(set_properties);
  }

  // SetLabels
  auto *set_labels = storage_->Create<SetLabels>();
  set_labels->identifier_ = storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
  set_labels->labels_ = std::any_cast<std::vector<QueryLabelType>>(ctx->nodeLabels()->accept(this));
  return static_cast<Clause *>(set_labels);
}

antlrcpp::Any CypherMainVisitor::visitRemove(MemgraphCypher::RemoveContext *ctx) {
  std::vector<Clause *> remove_items;
  for (auto *remove_item : ctx->removeItem()) {
    remove_items.push_back(std::any_cast<Clause *>(remove_item->accept(this)));
  }
  return remove_items;
}

antlrcpp::Any CypherMainVisitor::visitRemoveItem(MemgraphCypher::RemoveItemContext *ctx) {
  // RemoveProperty
  if (ctx->propertyExpression()) {
    auto *remove_property = storage_->Create<RemoveProperty>();
    remove_property->property_lookup_ = std::any_cast<PropertyLookup *>(ctx->propertyExpression()->accept(this));
    return static_cast<Clause *>(remove_property);
  }

  // RemoveLabels
  auto *remove_labels = storage_->Create<RemoveLabels>();
  remove_labels->identifier_ = storage_->Create<Identifier>(std::any_cast<std::string>(ctx->variable()->accept(this)));
  remove_labels->labels_ = std::any_cast<std::vector<QueryLabelType>>(ctx->nodeLabels()->accept(this));
  return static_cast<Clause *>(remove_labels);
}

antlrcpp::Any CypherMainVisitor::visitPropertyExpression(MemgraphCypher::PropertyExpressionContext *ctx) {
  auto *expression = std::any_cast<Expression *>(ctx->atom()->accept(this));
  for (auto *lookup : ctx->propertyLookup()) {
    auto key = std::any_cast<PropertyIx>(lookup->accept(this));
    auto property_lookup = storage_->Create<PropertyLookup>(expression, key);
    expression = property_lookup;
  }
  // It is guaranteed by grammar that there is at least one propertyLookup.
  return static_cast<PropertyLookup *>(expression);
}

antlrcpp::Any CypherMainVisitor::visitCaseExpression(MemgraphCypher::CaseExpressionContext *ctx) {
  Expression *test_expression = ctx->test ? std::any_cast<Expression *>(ctx->test->accept(this)) : nullptr;
  auto alternatives = ctx->caseAlternatives();
  // Reverse alternatives so that tree of IfOperators can be built bottom-up.
  std::reverse(alternatives.begin(), alternatives.end());
  Expression *else_expression = ctx->else_expression ? std::any_cast<Expression *>(ctx->else_expression->accept(this))
                                                     : storage_->Create<PrimitiveLiteral>(TypedValue());
  for (auto *alternative : alternatives) {
    Expression *condition =
        test_expression ? storage_->Create<EqualOperator>(
                              test_expression, std::any_cast<Expression *>(alternative->when_expression->accept(this)))
                        : std::any_cast<Expression *>(alternative->when_expression->accept(this));
    auto *then_expression = std::any_cast<Expression *>(alternative->then_expression->accept(this));
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

antlrcpp::Any CypherMainVisitor::visitMerge(MemgraphCypher::MergeContext *ctx) {
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

antlrcpp::Any CypherMainVisitor::visitUnwind(MemgraphCypher::UnwindContext *ctx) {
  auto *named_expr = storage_->Create<NamedExpression>();
  named_expr->expression_ = std::any_cast<Expression *>(ctx->expression()->accept(this));
  named_expr->name_ = std::any_cast<std::string>(ctx->variable()->accept(this));
  return storage_->Create<Unwind>(named_expr);
}

antlrcpp::Any CypherMainVisitor::visitFilterExpression(MemgraphCypher::FilterExpressionContext *) {
  LOG_FATAL("Should never be called. See documentation in hpp.");
  return 0;
}

antlrcpp::Any CypherMainVisitor::visitForeach(MemgraphCypher::ForeachContext *ctx) {
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

antlrcpp::Any CypherMainVisitor::visitShowConfigQuery(MemgraphCypher::ShowConfigQueryContext * /*ctx*/) {
  query_ = storage_->Create<ShowConfigQuery>();
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitCallSubquery(MemgraphCypher::CallSubqueryContext *ctx) {
  auto *call_subquery = storage_->Create<CallSubquery>();

  MG_ASSERT(ctx->cypherQuery(), "Expected query inside subquery clause");

  if (ctx->cypherQuery()->queryMemoryLimit()) {
    throw SyntaxException("Memory limit cannot be set on subqueries!");
  }

  call_subquery->cypher_query_ = std::any_cast<CypherQuery *>(ctx->cypherQuery()->accept(this));

  PreQueryDirectives pre_query_directives;
  if (auto const *periodic_commit = ctx->periodicSubquery()) {
    auto *const periodic_commit_number = periodic_commit->periodicCommitNumber;
    if (!periodic_commit_number->numberLiteral()) {
      throw SyntaxException("Periodic commit should be a number variable.");
    }
    if (!periodic_commit_number->numberLiteral()->integerLiteral()) {
      throw SyntaxException("Periodic commit should be an integer.");
    }
    pre_query_directives.commit_frequency_ = std::any_cast<Expression *>(periodic_commit_number->accept(this));

    call_subquery->cypher_query_->pre_query_directives_ = pre_query_directives;
  }

  return call_subquery;
}

LabelIx CypherMainVisitor::AddLabel(const std::string &name) { return storage_->GetLabelIx(name); }

PropertyIx CypherMainVisitor::AddProperty(const std::string &name) { return storage_->GetPropertyIx(name); }

EdgeTypeIx CypherMainVisitor::AddEdgeType(const std::string &name) { return storage_->GetEdgeTypeIx(name); }

antlrcpp::Any CypherMainVisitor::visitCreateDatabase(MemgraphCypher::CreateDatabaseContext *ctx) {
  auto *mdb_query = storage_->Create<MultiDatabaseQuery>();
  mdb_query->db_name_ = std::any_cast<std::string>(ctx->databaseName()->accept(this));
  mdb_query->action_ = MultiDatabaseQuery::Action::CREATE;
  query_ = mdb_query;
  return mdb_query;
}

antlrcpp::Any CypherMainVisitor::visitDropDatabase(MemgraphCypher::DropDatabaseContext *ctx) {
  auto *mdb_query = storage_->Create<MultiDatabaseQuery>();
  mdb_query->db_name_ = std::any_cast<std::string>(ctx->databaseName()->accept(this));
  mdb_query->action_ = MultiDatabaseQuery::Action::DROP;
  query_ = mdb_query;
  return mdb_query;
}

antlrcpp::Any CypherMainVisitor::visitUseDatabase(MemgraphCypher::UseDatabaseContext *ctx) {
  auto *query = storage_->Create<UseDatabaseQuery>();
  query->db_name_ = std::any_cast<std::string>(ctx->databaseName()->accept(this));
  query_ = query;
  return query;
}

antlrcpp::Any CypherMainVisitor::visitShowDatabase(MemgraphCypher::ShowDatabaseContext * /*ctx*/) {
  query_ = storage_->Create<ShowDatabaseQuery>();
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowDatabases(MemgraphCypher::ShowDatabasesContext * /*ctx*/) {
  query_ = storage_->Create<ShowDatabasesQuery>();
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitCreateEnumQuery(MemgraphCypher::CreateEnumQueryContext *ctx) {
  auto *create_enum_query = storage_->Create<CreateEnumQuery>();
  create_enum_query->enum_name_ = std::any_cast<std::string>(ctx->enumName()->symbolicName()->accept(this));
  for (auto *enumVal : ctx->enumValue()) {
    create_enum_query->enum_values_.push_back(std::any_cast<std::string>(enumVal->symbolicName()->accept(this)));
  }
  query_ = create_enum_query;
  return create_enum_query;
}

antlrcpp::Any CypherMainVisitor::visitShowEnumsQuery(MemgraphCypher::ShowEnumsQueryContext * /*ctx*/) {
  auto *show_enums_query = storage_->Create<ShowEnumsQuery>();
  query_ = show_enums_query;
  return show_enums_query;
}

antlrcpp::Any CypherMainVisitor::visitAlterEnumAddValueQuery(MemgraphCypher::AlterEnumAddValueQueryContext *ctx) {
  auto *alter_enum_query = storage_->Create<AlterEnumAddValueQuery>();
  alter_enum_query->enum_name_ = std::any_cast<std::string>(ctx->enumName()->symbolicName()->accept(this));
  alter_enum_query->enum_value_ = std::any_cast<std::string>(ctx->enumValue()->symbolicName()->accept(this));
  query_ = alter_enum_query;
  return alter_enum_query;
}

antlrcpp::Any CypherMainVisitor::visitAlterEnumUpdateValueQuery(MemgraphCypher::AlterEnumUpdateValueQueryContext *ctx) {
  auto *alter_enum_query = storage_->Create<AlterEnumUpdateValueQuery>();
  alter_enum_query->enum_name_ = std::any_cast<std::string>(ctx->enumName()->symbolicName()->accept(this));
  alter_enum_query->old_enum_value_ = std::any_cast<std::string>(ctx->old_value->symbolicName()->accept(this));
  alter_enum_query->new_enum_value_ = std::any_cast<std::string>(ctx->new_value->symbolicName()->accept(this));
  query_ = alter_enum_query;
  return alter_enum_query;
}

antlrcpp::Any CypherMainVisitor::visitAlterEnumRemoveValueQuery(MemgraphCypher::AlterEnumRemoveValueQueryContext *ctx) {
  auto *alter_enum_query = storage_->Create<AlterEnumRemoveValueQuery>();
  alter_enum_query->enum_name_ = std::any_cast<std::string>(ctx->enumName()->symbolicName()->accept(this));
  alter_enum_query->removed_value_ = std::any_cast<std::string>(ctx->removed_value->symbolicName()->accept(this));
  query_ = alter_enum_query;
  return alter_enum_query;
}

antlrcpp::Any CypherMainVisitor::visitDropEnumQuery(MemgraphCypher::DropEnumQueryContext *ctx) {
  auto *drop_enum_query = storage_->Create<DropEnumQuery>();
  drop_enum_query->enum_name_ = std::any_cast<std::string>(ctx->enumName()->symbolicName()->accept(this));
  query_ = drop_enum_query;
  return drop_enum_query;
}

antlrcpp::Any CypherMainVisitor::visitShowSchemaInfoQuery(MemgraphCypher::ShowSchemaInfoQueryContext * /*ctx*/) {
  auto *show_schema_info_query = storage_->Create<ShowSchemaInfoQuery>();
  query_ = show_schema_info_query;
  return show_schema_info_query;
}

antlrcpp::Any CypherMainVisitor::visitTtlQuery(MemgraphCypher::TtlQueryContext *ctx) {
  auto *ttl_query = storage_->Create<TtlQuery>();
  if (auto *manip = ctx->stopTtlQuery()) {
    if (manip->DISABLE()) {
      ttl_query->type_ = TtlQuery::Type::DISABLE;
    } else if (manip->STOP()) {
      ttl_query->type_ = TtlQuery::Type::STOP;
    } else {
      DMG_ASSERT(false, "Unknown TTL command");
    }
  } else if (auto *execute = ctx->startTtlQuery()) {
    if (!execute->AT() && !execute->EVERY()) {
      ttl_query->type_ = TtlQuery::Type::START;
    } else {
      ttl_query->type_ = TtlQuery::Type::CONFIGURE;
      if (execute->AT()) {
        if (!execute->time || !execute->time->StringLiteral()) {
          throw SemanticException("Time has to be defined using a string literal. Ex: '12:32:07'");
        }
        ttl_query->specific_time_ = std::any_cast<Expression *>(execute->time->accept(this));
      }
      if (execute->EVERY()) {
        if (!execute->period || !execute->period->StringLiteral()) {
          throw SemanticException("Period has to be defined using a string literal. Ex: '3m5s'");
        }
        ttl_query->period_ = std::any_cast<Expression *>(execute->period->accept(this));
      }
    }
  } else {
    DMG_ASSERT(false, "Unknown ttl query type");
  }
  query_ = ttl_query;
  return ttl_query;
}

antlrcpp::Any CypherMainVisitor::visitSetSessionTraceQuery(MemgraphCypher::SetSessionTraceQueryContext *ctx) {
  auto *session_trace_query = storage_->Create<SessionTraceQuery>();

  session_trace_query->enabled_ = ctx->ON() != nullptr;
  query_ = session_trace_query;

  return session_trace_query;
}

antlrcpp::Any CypherMainVisitor::visitLimitKV(MemgraphCypher::LimitKVContext *ctx) {
  auto key = utils::ToLowerCase(std::any_cast<std::string>(ctx->key->accept(this)));
  auto value = VisitLimitValue(ctx->val, this);
  return std::pair{key, value};
}

antlrcpp::Any CypherMainVisitor::visitListOfLimits(MemgraphCypher::ListOfLimitsContext *ctx) {
  UserProfileQuery::limits_t limits;
  for (auto *limit : ctx->limitKV()) {
    limits.push_back(std::any_cast<UserProfileQuery::limit_t>(limit->accept(this)));
  }
  return limits;
}

antlrcpp::Any CypherMainVisitor::visitCreateUserProfile(MemgraphCypher::CreateUserProfileContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  if (ctx->CREATE()) {
    profile_query->action_ = UserProfileQuery::Action::CREATE;
  } else if (ctx->UPDATE()) {
    profile_query->action_ = UserProfileQuery::Action::UPDATE;
  } else {
    throw SemanticException("Unknown user profile action");
  }
  profile_query->profile_name_ = std::any_cast<std::string>(ctx->profile->accept(this));
  if (ctx->LIMIT()) {
    profile_query->limits_ =
        std::any_cast<std::vector<std::pair<std::string, UserProfileQuery::LimitValueResult>>>(ctx->list->accept(this));
  }

  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitDropUserProfile(MemgraphCypher::DropUserProfileContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::DROP;
  profile_query->profile_name_ = std::any_cast<std::string>(ctx->profile->accept(this));
  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowUserProfiles(MemgraphCypher::ShowUserProfilesContext * /* unused */) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::SHOW_ALL;
  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowUserProfile(MemgraphCypher::ShowUserProfileContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::SHOW_ONE;
  profile_query->profile_name_ = std::any_cast<std::string>(ctx->profile->accept(this));
  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowUserProfileForUser(MemgraphCypher::ShowUserProfileForUserContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::SHOW_FOR;
  profile_query->user_or_role_ = std::any_cast<std::string>(ctx->user->accept(this));
  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowUserProfileForProfile(MemgraphCypher::ShowUserProfileForProfileContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::SHOW_USERS;
  profile_query->show_user_.emplace(ctx->USERS());
  profile_query->profile_name_ = std::any_cast<std::string>(ctx->profile->accept(this));
  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitSetUserProfile(MemgraphCypher::SetUserProfileContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::SET;
  profile_query->profile_name_ = std::any_cast<std::string>(ctx->profile->accept(this));
  profile_query->user_or_role_ = std::any_cast<std::string>(ctx->user->accept(this));
  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitClearUserProfile(MemgraphCypher::ClearUserProfileContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::CLEAR;
  profile_query->user_or_role_ = std::any_cast<std::string>(ctx->user->accept(this));
  query_ = profile_query;
  return query_;
}

antlrcpp::Any CypherMainVisitor::visitShowResourceConsumption(MemgraphCypher::ShowResourceConsumptionContext *ctx) {
  auto *profile_query = storage_->Create<UserProfileQuery>();
  profile_query->action_ = UserProfileQuery::Action::SHOW_RESOURCE_USAGE;
  profile_query->user_or_role_ = std::any_cast<std::string>(ctx->user->accept(this));
  query_ = profile_query;
  return query_;
}

Expression *CypherMainVisitor::CreateBinaryOperatorByToken(size_t token, Expression *e1, Expression *e2) {
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
    case MemgraphCypher::CARET:
      return storage_->Create<ExponentiationOperator>(e1, e2);
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

Expression *CypherMainVisitor::CreateUnaryOperatorByToken(size_t token, Expression *e) {
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

auto CypherMainVisitor::ExtractOperators(std::vector<antlr4::tree::ParseTree *> &all_children,
                                         std::vector<size_t> const &allowed_operators) -> std::vector<size_t> {
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

}  // namespace memgraph::query::frontend
