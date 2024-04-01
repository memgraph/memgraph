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

// Copyright 2017 Memgraph
//
// Created by Teon Banek on 24-03-2017

#include "query/frontend/semantic/symbol_generator.hpp"

#include <algorithm>
#include <optional>
#include <ranges>
#include <unordered_set>
#include <variant>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"

namespace memgraph::query {

namespace {
std::unordered_map<std::string, Identifier *> GeneratePredefinedIdentifierMap(
    const std::vector<Identifier *> &predefined_identifiers) {
  std::unordered_map<std::string, Identifier *> identifier_map;
  for (const auto &identifier : predefined_identifiers) {
    identifier_map.emplace(identifier->name_, identifier);
  }

  return identifier_map;
}
}  // namespace

SymbolGenerator::SymbolGenerator(SymbolTable *symbol_table, const std::vector<Identifier *> &predefined_identifiers)
    : symbol_table_(symbol_table),
      predefined_identifiers_{GeneratePredefinedIdentifierMap(predefined_identifiers)},
      scopes_(1, Scope()) {}

std::optional<Symbol> SymbolGenerator::FindSymbolInScope(const std::string &name, const Scope &scope,
                                                         Symbol::Type type) {
  if (auto it = scope.symbols.find(name); it != scope.symbols.end()) {
    const auto &symbol = it->second;
    // Unless we have `ANY` type, check that types match.
    if (type != Symbol::Type::ANY && symbol.type() != Symbol::Type::ANY && type != symbol.type()) {
      throw TypeMismatchError(name, Symbol::TypeToString(symbol.type()), Symbol::TypeToString(type));
    }
    return symbol;
  }
  return std::nullopt;
}

auto SymbolGenerator::CreateSymbol(const std::string &name, bool user_declared, Symbol::Type type, int token_position) {
  auto symbol = symbol_table_->CreateSymbol(name, user_declared, type, token_position);
  scopes_.back().symbols[name] = symbol;
  return symbol;
}

auto SymbolGenerator::CreateAnonymousSymbol(Symbol::Type /*type*/) {
  auto symbol = symbol_table_->CreateAnonymousSymbol();
  return symbol;
}

auto SymbolGenerator::GetOrCreateSymbol(const std::string &name, bool user_declared, Symbol::Type type) {
  // NOLINTNEXTLINE
  for (auto scope = scopes_.rbegin(); scope != scopes_.rend(); ++scope) {
    if (auto maybe_symbol = FindSymbolInScope(name, *scope, type); maybe_symbol) {
      return *maybe_symbol;
    }
  }
  return CreateSymbol(name, user_declared, type);
}

void SymbolGenerator::VisitReturnBody(ReturnBody &body, Where *where) {
  auto &scope = scopes_.back();
  for (auto &expr : body.named_expressions) {
    expr->Accept(*this);
  }

  SetEvaluationModeOnPropertyLookups(body);

  std::vector<Symbol> user_symbols;
  if (body.all_identifiers) {
    // Carry over user symbols because '*' appeared.
    for (const auto &sym_pair : scope.symbols) {
      if (!sym_pair.second.user_declared()) {
        continue;
      }
      user_symbols.emplace_back(sym_pair.second);
    }
    if (user_symbols.empty()) {
      throw SemanticException("There are no variables in scope to use for '*'.");
    }
  }
  // WITH/RETURN clause removes declarations of all the previous variables and
  // declares only those established through named expressions. New declarations
  // must not be visible inside named expressions themselves.
  bool removed_old_names = false;
  if ((!where && body.order_by.empty()) || scope.has_aggregation) {
    // WHERE and ORDER BY need to see both the old and new symbols, unless we
    // have an aggregation. Therefore, we can clear the symbols immediately if
    // there is neither ORDER BY nor WHERE, or we have an aggregation.
    scope.symbols.clear();
    removed_old_names = true;
  }
  // Create symbols for named expressions.
  std::unordered_set<std::string> new_names;
  for (const auto &user_sym : user_symbols) {
    new_names.insert(user_sym.name());
    scope.symbols[user_sym.name()] = user_sym;
  }
  for (auto &named_expr : body.named_expressions) {
    const auto &name = named_expr->name_;
    if (!new_names.insert(name).second) {
      throw SemanticException("Multiple results with the same name '{}' are not allowed.", name);
    }
    // An improvement would be to infer the type of the expression, so that the
    // new symbol would have a more specific type.
    named_expr->MapTo(CreateSymbol(name, true, Symbol::Type::ANY, named_expr->token_position_));
  }
  scope.in_order_by = true;
  for (const auto &order_pair : body.order_by) {
    order_pair.expression->Accept(*this);
  }
  scope.in_order_by = false;
  if (body.skip) {
    scope.in_skip = true;
    body.skip->Accept(*this);
    scope.in_skip = false;
  }
  if (body.limit) {
    scope.in_limit = true;
    body.limit->Accept(*this);
    scope.in_limit = false;
  }
  if (where) where->Accept(*this);
  if (!removed_old_names) {
    // We have an ORDER BY or WHERE, but no aggregation, which means we didn't
    // clear the old symbols, so do it now. We cannot just call clear, because
    // we've added new symbols.
    for (auto sym_it = scope.symbols.begin(); sym_it != scope.symbols.end();) {
      if (new_names.find(sym_it->first) == new_names.end()) {
        sym_it = scope.symbols.erase(sym_it);
      } else {
        sym_it++;
      }
    }
  }
  scopes_.back().has_aggregation = false;
}

// Query

bool SymbolGenerator::PreVisit(SingleQuery &) {
  auto &scope = scopes_.back();

  scope.prev_return_names = scope.curr_return_names;
  scope.curr_return_names.clear();
  return true;
}

// Union

bool SymbolGenerator::PreVisit(CypherUnion &) {
  auto next_scope = Scope();
  next_scope.curr_return_names = scopes_.back().curr_return_names;

  scopes_.pop_back();
  scopes_.push_back(next_scope);

  return true;
}

bool SymbolGenerator::PostVisit(CypherUnion &cypher_union) {
  auto &scope = scopes_.back();

  if (scope.prev_return_names != scope.curr_return_names) {
    throw SemanticException("All subqueries in an UNION must have the same column names.");
  }

  // create new symbols for the result of the union
  for (const auto &name : scope.curr_return_names) {
    auto symbol = CreateSymbol(name, false);
    cypher_union.union_symbols_.push_back(symbol);
  }

  return true;
}

// Clauses

bool SymbolGenerator::PreVisit(Create &) {
  scopes_.back().in_create = true;
  return true;
}
bool SymbolGenerator::PostVisit(Create &) {
  scopes_.back().in_create = false;
  return true;
}

bool SymbolGenerator::PreVisit(CallProcedure &call_proc) {
  for (auto *expr : call_proc.arguments_) {
    expr->Accept(*this);
  }
  return false;
}

bool SymbolGenerator::PostVisit(CallProcedure &call_proc) {
  for (auto *ident : call_proc.result_identifiers_) {
    if (HasSymbol(ident->name_)) {
      throw RedeclareVariableError(ident->name_);
    }
    ident->MapTo(CreateSymbol(ident->name_, true));
  }
  return true;
}

bool SymbolGenerator::PreVisit(CallSubquery & /*call_sub*/) {
  scopes_.emplace_back(Scope{.in_call_subquery = true});
  return true;
}

bool SymbolGenerator::PostVisit(CallSubquery & /*call_sub*/) {
  // no need to set the flag to true as we are popping the scope
  auto subquery_scope = scopes_.back();
  scopes_.pop_back();
  auto &main_query_scope = scopes_.back();

  if (!subquery_scope.has_return) {
    return true;
  }

  // append symbols returned in from subquery to outer scope
  for (const auto &[symbol_name, symbol] : subquery_scope.symbols) {
    if (main_query_scope.symbols.find(symbol_name) != main_query_scope.symbols.end()) {
      throw SemanticException("Variable in subquery already declared in outer scope!");
    }

    main_query_scope.symbols[symbol_name] = symbol;
  }

  return true;
}

bool SymbolGenerator::PreVisit(LoadCsv &load_csv) { return false; }

bool SymbolGenerator::PostVisit(LoadCsv &load_csv) {
  if (HasSymbol(load_csv.row_var_->name_)) {
    throw RedeclareVariableError(load_csv.row_var_->name_);
  }
  load_csv.row_var_->MapTo(CreateSymbol(load_csv.row_var_->name_, true));
  return true;
}

bool SymbolGenerator::PreVisit(Return &ret) {
  auto &scope = scopes_.back();
  scope.in_return = true;
  scope.has_return = true;

  VisitReturnBody(ret.body_);
  scope.in_return = false;
  return false;  // We handled the traversal ourselves.
}

bool SymbolGenerator::PostVisit(Return &) {
  auto &scope = scopes_.back();

  for (const auto &name_symbol : scope.symbols) scope.curr_return_names.insert(name_symbol.first);
  return true;
}

bool SymbolGenerator::PreVisit(With &with) {
  auto &scope = scopes_.back();
  scope.in_with = true;
  VisitReturnBody(with.body_, with.where_);
  scope.in_with = false;
  return false;  // We handled the traversal ourselves.
}

bool SymbolGenerator::PreVisit(Where &) {
  scopes_.back().in_where = true;
  return true;
}
bool SymbolGenerator::PostVisit(Where &) {
  scopes_.back().in_where = false;
  return true;
}

bool SymbolGenerator::PreVisit(Merge &) {
  scopes_.back().in_merge = true;
  return true;
}
bool SymbolGenerator::PostVisit(Merge &) {
  scopes_.back().in_merge = false;
  return true;
}

bool SymbolGenerator::PostVisit(Unwind &unwind) {
  const auto &name = unwind.named_expression_->name_;
  if (HasSymbol(name)) {
    throw RedeclareVariableError(name);
  }
  unwind.named_expression_->MapTo(CreateSymbol(name, true));
  return true;
}

bool SymbolGenerator::PreVisit(Match &) {
  scopes_.back().in_match = true;
  return true;
}
bool SymbolGenerator::PostVisit(Match &) {
  auto &scope = scopes_.back();
  scope.in_match = false;
  // Check variables in property maps after visiting Match, so that they can
  // reference symbols out of bind order.
  for (auto &ident : scope.identifiers_in_match) {
    if (!HasSymbol(ident->name_) && !ConsumePredefinedIdentifier(ident->name_))
      throw UnboundVariableError(ident->name_);
    ident->MapTo(scope.symbols[ident->name_]);
  }
  scope.identifiers_in_match.clear();
  return true;
}

bool SymbolGenerator::PreVisit(Foreach &for_each) {
  const auto &name = for_each.named_expression_->name_;
  scopes_.emplace_back(Scope());
  scopes_.back().in_foreach = true;
  for_each.named_expression_->MapTo(
      CreateSymbol(name, true, Symbol::Type::ANY, for_each.named_expression_->token_position_));
  return true;
}
bool SymbolGenerator::PostVisit([[maybe_unused]] Foreach &for_each) {
  scopes_.pop_back();
  return true;
}

// Expressions

SymbolGenerator::ReturnType SymbolGenerator::Visit(Identifier &ident) {
  auto &scope = scopes_.back();
  if (scope.in_skip || scope.in_limit) {
    throw SemanticException("Variables are not allowed in {}.", scope.in_skip ? "SKIP" : "LIMIT");
  }

  if (scope.in_exists && (scope.visiting_edge || scope.in_node_atom)) {
    auto has_symbol = HasSymbol(ident.name_);
    if (!has_symbol && !ConsumePredefinedIdentifier(ident.name_) && ident.user_declared_) {
      throw SemanticException("Unbounded variables are not allowed in exists!");
    }
  }

  Symbol symbol;
  if (scope.in_pattern && !(scope.in_node_atom || scope.visiting_edge)) {
    // If we are in the pattern, and outside of a node or an edge, the
    // identifier is the pattern name.
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_, Symbol::Type::PATH);
  } else if (scope.in_pattern && scope.in_pattern_atom_identifier) {
    //  Patterns used to create nodes and edges cannot redeclare already
    //  established bindings. Declaration only happens in single node
    //  patterns and in edge patterns. OpenCypher example,
    //  `MATCH (n) CREATE (n)` should throw an error that `n` is already
    //  declared. While `MATCH (n) CREATE (n) -[:R]-> (n)` is allowed,
    //  since `n` now references the bound node instead of declaring it.
    if ((scope.in_create_node || scope.in_create_edge) && HasSymbol(ident.name_)) {
      throw RedeclareVariableError(ident.name_);
    }
    auto type = Symbol::Type::VERTEX;
    if (scope.visiting_edge) {
      // Edge referencing is not allowed (like in Neo4j):
      // `MATCH (n) - [r] -> (n) - [r] -> (n) RETURN r` is not allowed.
      if (HasSymbol(ident.name_)) {
        throw RedeclareVariableError(ident.name_);
      }
      type = scope.visiting_edge->IsVariable() ? Symbol::Type::EDGE_LIST : Symbol::Type::EDGE;
    }
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_, type);
  } else if (scope.in_pattern && !scope.in_pattern_atom_identifier && scope.in_match) {
    if (scope.in_edge_range && scope.visiting_edge && scope.visiting_edge->identifier_ &&
        scope.visiting_edge->identifier_->name_ == ident.name_) {
      // Prevent variable path bounds to reference the identifier which is bound
      // by the variable path itself.
      throw UnboundVariableError(ident.name_);
    }
    // Variables in property maps or bounds of variable length path during MATCH
    // can reference symbols bound later in the same MATCH. We collect them
    // here, so that they can be checked after visiting Match.
    scope.identifiers_in_match.emplace_back(&ident);
  } else if (scope.in_call_subquery && !scope.in_with) {
    if (!scope.symbols.contains(ident.name_) && !ConsumePredefinedIdentifier(ident.name_)) {
      throw UnboundVariableError(ident.name_);
    }
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_, Symbol::Type::ANY);
  } else {
    // Everything else references a bound symbol.
    if (!HasSymbol(ident.name_) && !ConsumePredefinedIdentifier(ident.name_)) {
      throw UnboundVariableError(ident.name_);
    }
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_, Symbol::Type::ANY);
  }
  ident.MapTo(symbol);
  return true;
}

bool SymbolGenerator::PreVisit(MapLiteral &map_literal) {
  SetEvaluationModeOnPropertyLookups(map_literal);
  return true;
}

bool SymbolGenerator::PreVisit(Aggregation &aggr) {
  auto &scope = scopes_.back();
  // Check if the aggregation can be used in this context. This check should
  // probably move to a separate phase, which checks if the query is well
  // formed.
  if ((!scope.in_return && !scope.in_with) || scope.in_order_by || scope.in_skip || scope.in_limit || scope.in_where) {
    throw SemanticException("Aggregation functions are only allowed in WITH and RETURN.");
  }
  if (scope.in_aggregation) {
    throw SemanticException(
        "Using aggregation functions inside aggregation functions is not "
        "allowed.");
  }
  if (scope.num_if_operators) {
    // Neo allows aggregations here and produces very interesting behaviors.
    // To simplify implementation at this moment we decided to completely
    // disallow aggregations inside of the CASE.
    // However, in some cases aggregation makes perfect sense, for example:
    //    CASE count(n) WHEN 10 THEN "YES" ELSE "NO" END.
    // TODO: Rethink of allowing aggregations in some parts of the CASE
    // construct.
    throw SemanticException("Using aggregation functions inside of CASE is not allowed.");
  }
  // Create a virtual symbol for aggregation result.
  // Currently, we only have aggregation operators which return numbers.
  auto aggr_name = Aggregation::OpToString(aggr.op_) + std::to_string(aggr.symbol_pos_);
  aggr.MapTo(CreateSymbol(aggr_name, false, Symbol::Type::NUMBER));
  scope.in_aggregation = true;
  scope.has_aggregation = true;
  return true;
}

bool SymbolGenerator::PostVisit(Aggregation &) {
  scopes_.back().in_aggregation = false;
  return true;
}

bool SymbolGenerator::PreVisit(IfOperator &) {
  ++scopes_.back().num_if_operators;
  return true;
}

bool SymbolGenerator::PostVisit(IfOperator &) {
  --scopes_.back().num_if_operators;
  return true;
}

bool SymbolGenerator::PreVisit(All &all) {
  all.list_expression_->Accept(*this);
  VisitWithIdentifiers(all.where_->expression_, {all.identifier_});
  return false;
}

bool SymbolGenerator::PreVisit(Single &single) {
  single.list_expression_->Accept(*this);
  VisitWithIdentifiers(single.where_->expression_, {single.identifier_});
  return false;
}

bool SymbolGenerator::PreVisit(Any &any) {
  any.list_expression_->Accept(*this);
  VisitWithIdentifiers(any.where_->expression_, {any.identifier_});
  return false;
}

bool SymbolGenerator::PreVisit(None &none) {
  none.list_expression_->Accept(*this);
  VisitWithIdentifiers(none.where_->expression_, {none.identifier_});
  return false;
}

bool SymbolGenerator::PreVisit(Reduce &reduce) {
  auto &scope = scopes_.back();
  scope.in_reduce = true;
  reduce.initializer_->Accept(*this);
  reduce.list_->Accept(*this);
  VisitWithIdentifiers(reduce.expression_, {reduce.accumulator_, reduce.identifier_});
  return false;
}

bool SymbolGenerator::PostVisit(Reduce & /*reduce*/) {
  auto &scope = scopes_.back();
  scope.in_reduce = false;
  return true;
}

bool SymbolGenerator::PreVisit(Extract &extract) {
  extract.list_->Accept(*this);
  VisitWithIdentifiers(extract.expression_, {extract.identifier_});
  return false;
}

bool SymbolGenerator::PreVisit(Exists &exists) {
  auto &scope = scopes_.back();

  if (scope.in_set_property) {
    throw utils::NotYetImplemented("Exists cannot be used within SET clause.!");
  }

  if (scope.in_with) {
    throw utils::NotYetImplemented("Exists cannot be used within WITH!");
  }

  if (scope.in_return) {
    throw utils::NotYetImplemented("Exists cannot be used within RETURN!");
  }

  if (scope.in_reduce) {
    throw utils::NotYetImplemented("Exists cannot be used within REDUCE!");
  }

  if (scope.num_if_operators) {
    throw utils::NotYetImplemented("IF operator cannot be used with exists, but only during matching!");
  }

  scope.in_exists = true;

  const auto &symbol = CreateAnonymousSymbol();
  exists.MapTo(symbol);

  return true;
}

bool SymbolGenerator::PostVisit(Exists & /*exists*/) {
  auto &scope = scopes_.back();
  scope.in_exists = false;

  return true;
}

bool SymbolGenerator::PreVisit(NamedExpression &named_expression) {
  if (auto &scope = scopes_.back(); scope.in_call_subquery && scope.in_return &&
                                    !utils::Downcast<Identifier>(named_expression.expression_) &&
                                    !named_expression.is_aliased_) {
    throw SemanticException("Expression returned from subquery must be aliased (use AS)!");
  }
  return true;
}

bool SymbolGenerator::PreVisit(SetProperty & /*set_property*/) {
  auto &scope = scopes_.back();
  scope.in_set_property = true;

  return true;
}

bool SymbolGenerator::PostVisit(SetProperty & /*set_property*/) {
  auto &scope = scopes_.back();
  scope.in_set_property = false;

  return true;
}

bool SymbolGenerator::PreVisit(SetLabels &set_labels) {
  auto &scope = scopes_.back();
  scope.in_set_labels = true;
  for (auto &label : set_labels.labels_) {
    if (auto *expression = std::get_if<Expression *>(&label)) {
      (*expression)->Accept(*this);
    }
  }

  return true;
}

bool SymbolGenerator::PostVisit(SetLabels & /*set_labels*/) {
  auto &scope = scopes_.back();
  scope.in_set_labels = false;

  return true;
}

bool SymbolGenerator::PreVisit(RemoveLabels &remove_labels) {
  auto &scope = scopes_.back();
  scope.in_remove_labels = true;
  for (auto &label : remove_labels.labels_) {
    if (auto *expression = std::get_if<Expression *>(&label)) {
      (*expression)->Accept(*this);
    }
  }

  return true;
}

bool SymbolGenerator::PostVisit(RemoveLabels & /*remove_labels*/) {
  auto &scope = scopes_.back();
  scope.in_remove_labels = false;

  return true;
}

// Pattern and its subparts.

bool SymbolGenerator::PreVisit(Pattern &pattern) {
  auto &scope = scopes_.back();
  scope.in_pattern = true;
  if ((scope.in_create || scope.in_merge) && pattern.atoms_.size() == 1U) {
    MG_ASSERT(utils::IsSubtype(*pattern.atoms_[0], NodeAtom::kType), "Expected a single NodeAtom in Pattern");
    scope.in_create_node = true;
  }

  return true;
}

bool SymbolGenerator::PostVisit(Pattern &) {
  auto &scope = scopes_.back();
  scope.in_pattern = false;
  scope.in_create_node = false;
  return true;
}

bool SymbolGenerator::PreVisit(NodeAtom &node_atom) {
  auto &scope = scopes_.back();
  auto check_node_semantic = [&node_atom, &scope, this](const bool props_or_labels) {
    const auto &node_name = node_atom.identifier_->name_;
    if ((scope.in_create || scope.in_merge) && props_or_labels && HasSymbol(node_name)) {
      throw SemanticException("Cannot create node '" + node_name +
                              "' with labels or properties, because it is already declared.");
    }
    scope.in_pattern_atom_identifier = true;
    node_atom.identifier_->Accept(*this);
    scope.in_pattern_atom_identifier = false;
  };

  scope.in_node_atom = true;

  if (scope.in_create) {  // you can use expressions with labels only in create
    for (auto &label : node_atom.labels_) {
      if (auto *expression = std::get_if<Expression *>(&label)) {
        (*expression)->Accept(*this);
      }
    }
  }

  if (auto *properties = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&node_atom.properties_)) {
    bool props_or_labels = !properties->empty() || !node_atom.labels_.empty();

    check_node_semantic(props_or_labels);
    for (auto kv : *properties) {
      kv.second->Accept(*this);
    }

    return false;
  }
  auto &properties_parameter = std::get<ParameterLookup *>(node_atom.properties_);
  bool props_or_labels = !properties_parameter || !node_atom.labels_.empty();

  check_node_semantic(props_or_labels);
  properties_parameter->Accept(*this);
  return false;
}

bool SymbolGenerator::PostVisit(NodeAtom &) {
  scopes_.back().in_node_atom = false;
  return true;
}

bool SymbolGenerator::PreVisit(EdgeAtom &edge_atom) {
  auto &scope = scopes_.back();
  scope.visiting_edge = &edge_atom;
  if (scope.in_create || scope.in_merge) {
    scope.in_create_edge = true;
    if (edge_atom.edge_types_.size() != 1U) {
      throw SemanticException(
          "A single relationship type must be specified "
          "when creating an edge.");
    }
    if (scope.in_create &&  // Merge allows bidirectionality
        edge_atom.direction_ == EdgeAtom::Direction::BOTH) {
      throw SemanticException(
          "Bidirectional relationship are not supported "
          "when creating an edge");
    }
    if (edge_atom.IsVariable()) {
      throw SemanticException(
          "Variable length relationships are not supported when creating an "
          "edge.");
    }
  }
  if (auto *properties = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&edge_atom.properties_)) {
    for (auto kv : *properties) {
      kv.second->Accept(*this);
    }
  } else {
    std::get<ParameterLookup *>(edge_atom.properties_)->Accept(*this);
  }
  if (edge_atom.IsVariable()) {
    scope.in_edge_range = true;
    if (edge_atom.lower_bound_) {
      edge_atom.lower_bound_->Accept(*this);
    }
    if (edge_atom.upper_bound_) {
      edge_atom.upper_bound_->Accept(*this);
    }
    scope.in_edge_range = false;
    scope.in_pattern = false;
    if (edge_atom.filter_lambda_.expression) {
      std::vector<Identifier *> filter_lambda_identifiers{edge_atom.filter_lambda_.inner_edge,
                                                          edge_atom.filter_lambda_.inner_node};
      if (edge_atom.filter_lambda_.accumulated_path) {
        filter_lambda_identifiers.emplace_back(edge_atom.filter_lambda_.accumulated_path);

        if (edge_atom.filter_lambda_.accumulated_weight) {
          filter_lambda_identifiers.emplace_back(edge_atom.filter_lambda_.accumulated_weight);
        }
      }
      VisitWithIdentifiers(edge_atom.filter_lambda_.expression, filter_lambda_identifiers);
    } else {
      // Create inner symbols, but don't bind them in scope, since they are to
      // be used in the missing filter expression.
      auto *inner_edge = edge_atom.filter_lambda_.inner_edge;
      inner_edge->MapTo(symbol_table_->CreateSymbol(inner_edge->name_, inner_edge->user_declared_, Symbol::Type::EDGE));
      auto *inner_node = edge_atom.filter_lambda_.inner_node;
      inner_node->MapTo(
          symbol_table_->CreateSymbol(inner_node->name_, inner_node->user_declared_, Symbol::Type::VERTEX));
      if (edge_atom.filter_lambda_.accumulated_path) {
        auto *accumulated_path = edge_atom.filter_lambda_.accumulated_path;
        accumulated_path->MapTo(
            symbol_table_->CreateSymbol(accumulated_path->name_, accumulated_path->user_declared_, Symbol::Type::PATH));

        if (edge_atom.filter_lambda_.accumulated_weight) {
          auto *accumulated_weight = edge_atom.filter_lambda_.accumulated_weight;
          accumulated_weight->MapTo(symbol_table_->CreateSymbol(
              accumulated_weight->name_, accumulated_weight->user_declared_, Symbol::Type::NUMBER));
        }
      }
    }
    if (edge_atom.weight_lambda_.expression) {
      VisitWithIdentifiers(edge_atom.weight_lambda_.expression,
                           {edge_atom.weight_lambda_.inner_edge, edge_atom.weight_lambda_.inner_node});
    }
    scope.in_pattern = true;
  }
  scope.in_pattern_atom_identifier = true;
  edge_atom.identifier_->Accept(*this);
  scope.in_pattern_atom_identifier = false;
  if (edge_atom.total_weight_) {
    if (HasSymbol(edge_atom.total_weight_->name_)) {
      throw RedeclareVariableError(edge_atom.total_weight_->name_);
    }
    edge_atom.total_weight_->MapTo(GetOrCreateSymbol(edge_atom.total_weight_->name_,
                                                     edge_atom.total_weight_->user_declared_, Symbol::Type::NUMBER));
  }
  return false;
}

bool SymbolGenerator::PostVisit(EdgeAtom &) {
  auto &scope = scopes_.back();
  scope.visiting_edge = nullptr;
  scope.in_create_edge = false;
  return true;
}

bool SymbolGenerator::PreVisit(PatternComprehension &pc) {
  auto &scope = scopes_.back();

  if (scope.in_set_property) {
    throw utils::NotYetImplemented("Pattern Comprehension cannot be used within SET clause.!");
  }

  if (scope.in_with) {
    throw utils::NotYetImplemented("Pattern Comprehension cannot be used within WITH!");
  }

  if (scope.in_reduce) {
    throw utils::NotYetImplemented("Pattern Comprehension cannot be used within REDUCE!");
  }

  if (scope.num_if_operators) {
    throw utils::NotYetImplemented("IF operator cannot be used with Pattern Comprehension!");
  }

  scopes_.emplace_back(Scope{.in_pattern_comprehension = true});

  const auto &symbol = CreateAnonymousSymbol();
  pc.MapTo(symbol);
  return true;
}

bool SymbolGenerator::PostVisit(PatternComprehension & /*pc*/) {
  scopes_.pop_back();
  return true;
}

void SymbolGenerator::VisitWithIdentifiers(Expression *expr, const std::vector<Identifier *> &identifiers) {
  auto &scope = scopes_.back();
  std::vector<std::pair<std::optional<Symbol>, Identifier *>> prev_symbols;
  // Collect previous symbols if they exist.
  for (const auto &identifier : identifiers) {
    std::optional<Symbol> prev_symbol;
    auto prev_symbol_it = scope.symbols.find(identifier->name_);
    if (prev_symbol_it != scope.symbols.end()) {
      prev_symbol = prev_symbol_it->second;
    }
    identifier->MapTo(CreateSymbol(identifier->name_, identifier->user_declared_));
    prev_symbols.emplace_back(prev_symbol, identifier);
  }
  // Visit the expression with the new symbols bound.
  expr->Accept(*this);
  // Restore back to previous symbols.
  for (const auto &prev : prev_symbols) {
    const auto &prev_symbol = prev.first;
    const auto &identifier = prev.second;
    if (prev_symbol) {
      scope.symbols[identifier->name_] = *prev_symbol;
    } else {
      scope.symbols.erase(identifier->name_);
    }
  }
}

bool SymbolGenerator::HasSymbol(const std::string &name) const {
  return std::ranges::any_of(scopes_, [&name](const auto &scope) { return scope.symbols.contains(name); });
}

bool SymbolGenerator::ConsumePredefinedIdentifier(const std::string &name) {
  auto it = predefined_identifiers_.find(name);

  if (it == predefined_identifiers_.end()) {
    return false;
  }

  // we can only use the predefined identifier in a single scope so we remove it after creating
  // a symbol for it
  auto &identifier = it->second;
  MG_ASSERT(!identifier->user_declared_, "Predefined symbols cannot be user declared!");
  identifier->MapTo(CreateSymbol(identifier->name_, identifier->user_declared_));
  predefined_identifiers_.erase(it);
  return true;
}

void PropertyLookupEvaluationModeVisitor::Visit(PropertyLookup &property_lookup) {
  if (property_lookup.expression_->GetTypeInfo() != Identifier::kType) {
    return;
  }

  auto identifier_symbol = static_cast<Identifier *>(property_lookup.expression_)->name_;

  if (this->gather_property_lookup_counts) {
    if (!property_lookup_counts_by_symbol.contains(identifier_symbol)) {
      property_lookup_counts_by_symbol[identifier_symbol] = 0;
    }

    property_lookup_counts_by_symbol[identifier_symbol]++;

    return;
  }

  if (this->assign_property_lookup_evaluations) {
    if (property_lookup_counts_by_symbol.contains(identifier_symbol) &&
        property_lookup_counts_by_symbol[identifier_symbol] > 1) {
      property_lookup.evaluation_mode_ = PropertyLookup::EvaluationMode::GET_ALL_PROPERTIES;
    }

    return;
  }
}

}  // namespace memgraph::query
