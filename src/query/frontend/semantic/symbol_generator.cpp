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

// Copyright 2017 Memgraph
//
// Created by Teon Banek on 24-03-2017

#include "query/frontend/semantic/symbol_generator.hpp"

#include <optional>
#include <unordered_set>
#include <variant>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"

namespace query {

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
    : symbol_table_(symbol_table), predefined_identifiers_{GeneratePredefinedIdentifierMap(predefined_identifiers)} {}

auto SymbolGenerator::CreateSymbol(const std::string &name, bool user_declared, Symbol::Type type, int token_position) {
  auto symbol = symbol_table_->CreateSymbol(name, user_declared, type, token_position);
  scope_.symbols[name] = symbol;
  return symbol;
}

auto SymbolGenerator::GetOrCreateSymbol(const std::string &name, bool user_declared, Symbol::Type type) {
  auto search = scope_.symbols.find(name);
  if (search != scope_.symbols.end()) {
    auto symbol = search->second;
    // Unless we have `ANY` type, check that types match.
    if (type != Symbol::Type::ANY && symbol.type() != Symbol::Type::ANY && type != symbol.type()) {
      throw TypeMismatchError(name, Symbol::TypeToString(symbol.type()), Symbol::TypeToString(type));
    }
    return search->second;
  }
  return CreateSymbol(name, user_declared, type);
}

void SymbolGenerator::VisitReturnBody(ReturnBody &body, Where *where) {
  for (auto &expr : body.named_expressions) {
    expr->Accept(*this);
  }
  std::vector<Symbol> user_symbols;
  if (body.all_identifiers) {
    // Carry over user symbols because '*' appeared.
    for (auto sym_pair : scope_.symbols) {
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
  if ((!where && body.order_by.empty()) || scope_.has_aggregation) {
    // WHERE and ORDER BY need to see both the old and new symbols, unless we
    // have an aggregation. Therefore, we can clear the symbols immediately if
    // there is neither ORDER BY nor WHERE, or we have an aggregation.
    scope_.symbols.clear();
    removed_old_names = true;
  }
  // Create symbols for named expressions.
  std::unordered_set<std::string> new_names;
  for (const auto &user_sym : user_symbols) {
    new_names.insert(user_sym.name());
    scope_.symbols[user_sym.name()] = user_sym;
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
  scope_.in_order_by = true;
  for (const auto &order_pair : body.order_by) {
    order_pair.expression->Accept(*this);
  }
  scope_.in_order_by = false;
  if (body.skip) {
    scope_.in_skip = true;
    body.skip->Accept(*this);
    scope_.in_skip = false;
  }
  if (body.limit) {
    scope_.in_limit = true;
    body.limit->Accept(*this);
    scope_.in_limit = false;
  }
  if (where) where->Accept(*this);
  if (!removed_old_names) {
    // We have an ORDER BY or WHERE, but no aggregation, which means we didn't
    // clear the old symbols, so do it now. We cannot just call clear, because
    // we've added new symbols.
    for (auto sym_it = scope_.symbols.begin(); sym_it != scope_.symbols.end();) {
      if (new_names.find(sym_it->first) == new_names.end()) {
        sym_it = scope_.symbols.erase(sym_it);
      } else {
        sym_it++;
      }
    }
  }
  scope_.has_aggregation = false;
}

// Query

bool SymbolGenerator::PreVisit(SingleQuery &) {
  prev_return_names_ = curr_return_names_;
  curr_return_names_.clear();
  return true;
}

// Union

bool SymbolGenerator::PreVisit(CypherUnion &) {
  scope_ = Scope();
  return true;
}

bool SymbolGenerator::PostVisit(CypherUnion &cypher_union) {
  if (prev_return_names_ != curr_return_names_) {
    throw SemanticException("All subqueries in an UNION must have the same column names.");
  }

  // create new symbols for the result of the union
  for (const auto &name : curr_return_names_) {
    auto symbol = CreateSymbol(name, false);
    cypher_union.union_symbols_.push_back(symbol);
  }

  return true;
}

// Clauses

bool SymbolGenerator::PreVisit(Create &) {
  scope_.in_create = true;
  return true;
}
bool SymbolGenerator::PostVisit(Create &) {
  scope_.in_create = false;
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

bool SymbolGenerator::PreVisit(LoadCsv &load_csv) { return false; }

bool SymbolGenerator::PostVisit(LoadCsv &load_csv) {
  if (HasSymbol(load_csv.row_var_->name_)) {
    throw RedeclareVariableError(load_csv.row_var_->name_);
  }
  load_csv.row_var_->MapTo(CreateSymbol(load_csv.row_var_->name_, true));
  return true;
}

bool SymbolGenerator::PreVisit(Return &ret) {
  scope_.in_return = true;
  VisitReturnBody(ret.body_);
  scope_.in_return = false;
  return false;  // We handled the traversal ourselves.
}

bool SymbolGenerator::PostVisit(Return &) {
  for (const auto &name_symbol : scope_.symbols) curr_return_names_.insert(name_symbol.first);
  return true;
}

bool SymbolGenerator::PreVisit(With &with) {
  scope_.in_with = true;
  VisitReturnBody(with.body_, with.where_);
  scope_.in_with = false;
  return false;  // We handled the traversal ourselves.
}

bool SymbolGenerator::PreVisit(Where &) {
  scope_.in_where = true;
  return true;
}
bool SymbolGenerator::PostVisit(Where &) {
  scope_.in_where = false;
  return true;
}

bool SymbolGenerator::PreVisit(Merge &) {
  scope_.in_merge = true;
  return true;
}
bool SymbolGenerator::PostVisit(Merge &) {
  scope_.in_merge = false;
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
  scope_.in_match = true;
  return true;
}
bool SymbolGenerator::PostVisit(Match &) {
  scope_.in_match = false;
  // Check variables in property maps after visiting Match, so that they can
  // reference symbols out of bind order.
  for (auto &ident : scope_.identifiers_in_match) {
    if (!HasSymbol(ident->name_) && !ConsumePredefinedIdentifier(ident->name_))
      throw UnboundVariableError(ident->name_);
    ident->MapTo(scope_.symbols[ident->name_]);
  }
  scope_.identifiers_in_match.clear();
  return true;
}

// Expressions

SymbolGenerator::ReturnType SymbolGenerator::Visit(Identifier &ident) {
  if (scope_.in_skip || scope_.in_limit) {
    throw SemanticException("Variables are not allowed in {}.", scope_.in_skip ? "SKIP" : "LIMIT");
  }
  Symbol symbol;
  if (scope_.in_pattern && !(scope_.in_node_atom || scope_.visiting_edge)) {
    // If we are in the pattern, and outside of a node or an edge, the
    // identifier is the pattern name.
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_, Symbol::Type::PATH);
  } else if (scope_.in_pattern && scope_.in_pattern_atom_identifier) {
    //  Patterns used to create nodes and edges cannot redeclare already
    //  established bindings. Declaration only happens in single node
    //  patterns and in edge patterns. OpenCypher example,
    //  `MATCH (n) CREATE (n)` should throw an error that `n` is already
    //  declared. While `MATCH (n) CREATE (n) -[:R]-> (n)` is allowed,
    //  since `n` now references the bound node instead of declaring it.
    if ((scope_.in_create_node || scope_.in_create_edge) && HasSymbol(ident.name_)) {
      throw RedeclareVariableError(ident.name_);
    }
    auto type = Symbol::Type::VERTEX;
    if (scope_.visiting_edge) {
      // Edge referencing is not allowed (like in Neo4j):
      // `MATCH (n) - [r] -> (n) - [r] -> (n) RETURN r` is not allowed.
      if (HasSymbol(ident.name_)) {
        throw RedeclareVariableError(ident.name_);
      }
      type = scope_.visiting_edge->IsVariable() ? Symbol::Type::EDGE_LIST : Symbol::Type::EDGE;
    }
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_, type);
  } else if (scope_.in_pattern && !scope_.in_pattern_atom_identifier && scope_.in_match) {
    if (scope_.in_edge_range && scope_.visiting_edge->identifier_->name_ == ident.name_) {
      // Prevent variable path bounds to reference the identifier which is bound
      // by the variable path itself.
      throw UnboundVariableError(ident.name_);
    }
    // Variables in property maps or bounds of variable length path during MATCH
    // can reference symbols bound later in the same MATCH. We collect them
    // here, so that they can be checked after visiting Match.
    scope_.identifiers_in_match.emplace_back(&ident);
  } else {
    // Everything else references a bound symbol.
    if (!HasSymbol(ident.name_) && !ConsumePredefinedIdentifier(ident.name_)) throw UnboundVariableError(ident.name_);
    symbol = scope_.symbols[ident.name_];
  }
  ident.MapTo(symbol);
  return true;
}

bool SymbolGenerator::PreVisit(Aggregation &aggr) {
  // Check if the aggregation can be used in this context. This check should
  // probably move to a separate phase, which checks if the query is well
  // formed.
  if ((!scope_.in_return && !scope_.in_with) || scope_.in_order_by || scope_.in_skip || scope_.in_limit ||
      scope_.in_where) {
    throw SemanticException("Aggregation functions are only allowed in WITH and RETURN.");
  }
  if (scope_.in_aggregation) {
    throw SemanticException(
        "Using aggregation functions inside aggregation functions is not "
        "allowed.");
  }
  if (scope_.num_if_operators) {
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
  scope_.in_aggregation = true;
  scope_.has_aggregation = true;
  return true;
}

bool SymbolGenerator::PostVisit(Aggregation &) {
  scope_.in_aggregation = false;
  return true;
}

bool SymbolGenerator::PreVisit(IfOperator &) {
  ++scope_.num_if_operators;
  return true;
}

bool SymbolGenerator::PostVisit(IfOperator &) {
  --scope_.num_if_operators;
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
  reduce.initializer_->Accept(*this);
  reduce.list_->Accept(*this);
  VisitWithIdentifiers(reduce.expression_, {reduce.accumulator_, reduce.identifier_});
  return false;
}

bool SymbolGenerator::PreVisit(Extract &extract) {
  extract.list_->Accept(*this);
  VisitWithIdentifiers(extract.expression_, {extract.identifier_});
  return false;
}

// Pattern and its subparts.

bool SymbolGenerator::PreVisit(Pattern &pattern) {
  scope_.in_pattern = true;
  if ((scope_.in_create || scope_.in_merge) && pattern.atoms_.size() == 1U) {
    MG_ASSERT(utils::IsSubtype(*pattern.atoms_[0], NodeAtom::kType), "Expected a single NodeAtom in Pattern");
    scope_.in_create_node = true;
  }
  return true;
}

bool SymbolGenerator::PostVisit(Pattern &) {
  scope_.in_pattern = false;
  scope_.in_create_node = false;
  return true;
}

bool SymbolGenerator::PreVisit(NodeAtom &node_atom) {
  auto check_node_semantic = [&node_atom, this](const bool props_or_labels) {
    const auto &node_name = node_atom.identifier_->name_;
    if ((scope_.in_create || scope_.in_merge) && props_or_labels && HasSymbol(node_name)) {
      throw SemanticException("Cannot create node '" + node_name +
                              "' with labels or properties, because it is already declared.");
    }
    scope_.in_pattern_atom_identifier = true;
    node_atom.identifier_->Accept(*this);
    scope_.in_pattern_atom_identifier = false;
  };

  scope_.in_node_atom = true;
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
  scope_.in_node_atom = false;
  return true;
}

bool SymbolGenerator::PreVisit(EdgeAtom &edge_atom) {
  scope_.visiting_edge = &edge_atom;
  if (scope_.in_create || scope_.in_merge) {
    scope_.in_create_edge = true;
    if (edge_atom.edge_types_.size() != 1U) {
      throw SemanticException(
          "A single relationship type must be specified "
          "when creating an edge.");
    }
    if (scope_.in_create &&  // Merge allows bidirectionality
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
    scope_.in_edge_range = true;
    if (edge_atom.lower_bound_) {
      edge_atom.lower_bound_->Accept(*this);
    }
    if (edge_atom.upper_bound_) {
      edge_atom.upper_bound_->Accept(*this);
    }
    scope_.in_edge_range = false;
    scope_.in_pattern = false;
    if (edge_atom.filter_lambda_.expression) {
      VisitWithIdentifiers(edge_atom.filter_lambda_.expression,
                           {edge_atom.filter_lambda_.inner_edge, edge_atom.filter_lambda_.inner_node});
    } else {
      // Create inner symbols, but don't bind them in scope, since they are to
      // be used in the missing filter expression.
      auto *inner_edge = edge_atom.filter_lambda_.inner_edge;
      inner_edge->MapTo(symbol_table_->CreateSymbol(inner_edge->name_, inner_edge->user_declared_, Symbol::Type::EDGE));
      auto *inner_node = edge_atom.filter_lambda_.inner_node;
      inner_node->MapTo(
          symbol_table_->CreateSymbol(inner_node->name_, inner_node->user_declared_, Symbol::Type::VERTEX));
    }
    if (edge_atom.weight_lambda_.expression) {
      VisitWithIdentifiers(edge_atom.weight_lambda_.expression,
                           {edge_atom.weight_lambda_.inner_edge, edge_atom.weight_lambda_.inner_node});
    }
    scope_.in_pattern = true;
  }
  scope_.in_pattern_atom_identifier = true;
  edge_atom.identifier_->Accept(*this);
  scope_.in_pattern_atom_identifier = false;
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
  scope_.visiting_edge = nullptr;
  scope_.in_create_edge = false;
  return true;
}

void SymbolGenerator::VisitWithIdentifiers(Expression *expr, const std::vector<Identifier *> &identifiers) {
  std::vector<std::pair<std::optional<Symbol>, Identifier *>> prev_symbols;
  // Collect previous symbols if they exist.
  for (const auto &identifier : identifiers) {
    std::optional<Symbol> prev_symbol;
    auto prev_symbol_it = scope_.symbols.find(identifier->name_);
    if (prev_symbol_it != scope_.symbols.end()) {
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
      scope_.symbols[identifier->name_] = *prev_symbol;
    } else {
      scope_.symbols.erase(identifier->name_);
    }
  }
}

bool SymbolGenerator::HasSymbol(const std::string &name) { return scope_.symbols.find(name) != scope_.symbols.end(); }

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

}  // namespace query
