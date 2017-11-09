// Copyright 2017 Memgraph
//
// Created by Teon Banek on 24-03-2017

#include "query/frontend/semantic/symbol_generator.hpp"

#include <experimental/optional>
#include <unordered_set>

#include "glog/logging.h"

#include "utils/algorithm.hpp"

namespace query {

auto SymbolGenerator::CreateSymbol(const std::string &name, bool user_declared,
                                   Symbol::Type type, int token_position) {
  auto symbol =
      symbol_table_.CreateSymbol(name, user_declared, type, token_position);
  scope_.symbols[name] = symbol;
  return symbol;
}

auto SymbolGenerator::GetOrCreateSymbol(const std::string &name,
                                        bool user_declared, Symbol::Type type) {
  auto search = scope_.symbols.find(name);
  if (search != scope_.symbols.end()) {
    auto symbol = search->second;
    // Unless we have `Any` type, check that types match.
    if (type != Symbol::Type::Any && symbol.type() != Symbol::Type::Any &&
        type != symbol.type()) {
      throw TypeMismatchError(name, Symbol::TypeToString(symbol.type()),
                              Symbol::TypeToString(type));
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
      throw SemanticException("There are no variables in scope to use for '*'");
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
      throw SemanticException(
          "Multiple results with the same name '{}' are not allowed.", name);
    }
    // An improvement would be to infer the type of the expression, so that the
    // new symbol would have a more specific type.
    symbol_table_[*named_expr] = CreateSymbol(name, true, Symbol::Type::Any,
                                              named_expr->token_position_);
  }
  scope_.in_order_by = true;
  for (const auto &order_pair : body.order_by) {
    order_pair.second->Accept(*this);
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
    for (auto sym_it = scope_.symbols.begin();
         sym_it != scope_.symbols.end();) {
      if (new_names.find(sym_it->first) == new_names.end()) {
        sym_it = scope_.symbols.erase(sym_it);
      } else {
        sym_it++;
      }
    }
  }
  scope_.has_aggregation = false;
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

bool SymbolGenerator::PreVisit(Return &ret) {
  scope_.in_return = true;
  VisitReturnBody(ret.body_);
  scope_.in_return = false;
  return false;  // We handled the traversal ourselves.
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
  symbol_table_[*unwind.named_expression_] = CreateSymbol(name, true);
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
    if (!HasSymbol(ident->name_)) throw UnboundVariableError(ident->name_);
    symbol_table_[*ident] = scope_.symbols[ident->name_];
  }
  scope_.identifiers_in_match.clear();
  return true;
}

bool SymbolGenerator::Visit(CreateIndex &) { return true; }

// Expressions

SymbolGenerator::ReturnType SymbolGenerator::Visit(Identifier &ident) {
  if (scope_.in_skip || scope_.in_limit) {
    throw SemanticException("Variables are not allowed in {}",
                            scope_.in_skip ? "SKIP" : "LIMIT");
  }
  Symbol symbol;
  if (scope_.in_pattern && !(scope_.in_node_atom || scope_.visiting_edge)) {
    // If we are in the pattern, and outside of a node or an edge, the
    // identifier is the pattern name.
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_,
                               Symbol::Type::Path);
  } else if (scope_.in_pattern && scope_.in_pattern_atom_identifier) {
    //  Patterns used to create nodes and edges cannot redeclare already
    //  established bindings. Declaration only happens in single node
    //  patterns and in edge patterns. OpenCypher example,
    //  `MATCH (n) CREATE (n)` should throw an error that `n` is already
    //  declared. While `MATCH (n) CREATE (n) -[:R]-> (n)` is allowed,
    //  since `n` now references the bound node instead of declaring it.
    if ((scope_.in_create_node || scope_.in_create_edge) &&
        HasSymbol(ident.name_)) {
      throw RedeclareVariableError(ident.name_);
    }
    auto type = Symbol::Type::Vertex;
    if (scope_.visiting_edge) {
      // Edge referencing is not allowed (like in Neo4j):
      // `MATCH (n) - [r] -> (n) - [r] -> (n) RETURN r` is not allowed.
      if (HasSymbol(ident.name_)) {
        throw RedeclareVariableError(ident.name_);
      }
      type = scope_.visiting_edge->IsVariable() ? Symbol::Type::EdgeList
                                                : Symbol::Type::Edge;
    }
    symbol = GetOrCreateSymbol(ident.name_, ident.user_declared_, type);
  } else if (scope_.in_pattern && !scope_.in_pattern_atom_identifier &&
             scope_.in_match) {
    if (scope_.in_edge_range &&
        scope_.visiting_edge->identifier_->name_ == ident.name_) {
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
    if (!HasSymbol(ident.name_)) throw UnboundVariableError(ident.name_);
    symbol = scope_.symbols[ident.name_];
  }
  symbol_table_[ident] = symbol;
  return true;
}

bool SymbolGenerator::PreVisit(Aggregation &aggr) {
  // Check if the aggregation can be used in this context. This check should
  // probably move to a separate phase, which checks if the query is well
  // formed.
  if ((!scope_.in_return && !scope_.in_with) || scope_.in_order_by ||
      scope_.in_skip || scope_.in_limit || scope_.in_where) {
    throw SemanticException(
        "Aggregation functions are only allowed in WITH and RETURN");
  }
  if (scope_.in_aggregation) {
    throw SemanticException(
        "Using aggregation functions inside aggregation functions is not "
        "allowed");
  }
  if (scope_.num_if_operators) {
    // Neo allows aggregations here and produces very interesting behaviors.
    // To simplify implementation at this moment we decided to completely
    // disallow aggregations inside of the CASE.
    // However, in some cases aggregation makes perfect sense, for example:
    //    CASE count(n) WHEN 10 THEN "YES" ELSE "NO" END.
    // TODO: Rethink of allowing aggregations in some parts of the CASE
    // construct.
    throw SemanticException(
        "Using aggregation functions inside of CASE is not allowed");
  }
  // Create a virtual symbol for aggregation result.
  // Currently, we only have aggregation operators which return numbers.
  symbol_table_[aggr] =
      symbol_table_.CreateSymbol("", false, Symbol::Type::Number);
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
  VisitWithIdentifiers(*all.where_, {all.identifier_});
  return false;
}

// Pattern and its subparts.

bool SymbolGenerator::PreVisit(Pattern &pattern) {
  scope_.in_pattern = true;
  if ((scope_.in_create || scope_.in_merge) && pattern.atoms_.size() == 1U) {
    DCHECK(dynamic_cast<NodeAtom *>(pattern.atoms_[0]))
        << "Expected a single NodeAtom in Pattern";
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
  scope_.in_node_atom = true;
  bool props_or_labels =
      !node_atom.properties_.empty() || !node_atom.labels_.empty();
  const auto &node_name = node_atom.identifier_->name_;
  if ((scope_.in_create || scope_.in_merge) && props_or_labels &&
      HasSymbol(node_name)) {
    throw SemanticException(
        "Cannot create node '" + node_name +
        "' with labels or properties, because it is already declared.");
  }
  for (auto kv : node_atom.properties_) {
    kv.second->Accept(*this);
  }
  scope_.in_pattern_atom_identifier = true;
  node_atom.identifier_->Accept(*this);
  scope_.in_pattern_atom_identifier = false;
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
  for (auto kv : edge_atom.properties_) {
    kv.second->Accept(*this);
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
    if (edge_atom.filter_expression_) {
      VisitWithIdentifiers(*edge_atom.filter_expression_,
                           {edge_atom.inner_edge_, edge_atom.inner_node_});
    } else {
      // Create inner symbols, but don't bind them in scope, since they are to
      // be used in the missing filter expression.
      symbol_table_[*edge_atom.inner_edge_] = symbol_table_.CreateSymbol(
          edge_atom.inner_edge_->name_, edge_atom.inner_edge_->user_declared_,
          Symbol::Type::Edge);
      symbol_table_[*edge_atom.inner_node_] = symbol_table_.CreateSymbol(
          edge_atom.inner_node_->name_, edge_atom.inner_node_->user_declared_,
          Symbol::Type::Vertex);
    }
    scope_.in_pattern = true;
  }
  scope_.in_pattern_atom_identifier = true;
  edge_atom.identifier_->Accept(*this);
  scope_.in_pattern_atom_identifier = false;
  return false;
}

bool SymbolGenerator::PostVisit(EdgeAtom &) {
  scope_.visiting_edge = nullptr;
  scope_.in_create_edge = false;
  return true;
}

void SymbolGenerator::VisitWithIdentifiers(
    Tree &tree, const std::vector<Identifier *> &identifiers) {
  std::vector<std::pair<std::experimental::optional<Symbol>, Identifier *>>
      prev_symbols;
  // Collect previous symbols if they exist.
  for (const auto &identifier : identifiers) {
    std::experimental::optional<Symbol> prev_symbol;
    auto prev_symbol_it = scope_.symbols.find(identifier->name_);
    if (prev_symbol_it != scope_.symbols.end()) {
      prev_symbol = prev_symbol_it->second;
    }
    symbol_table_[*identifier] =
        CreateSymbol(identifier->name_, identifier->user_declared_);
    prev_symbols.emplace_back(prev_symbol, identifier);
  }
  // Visit the tree with the new symbols bound.
  tree.Accept(*this);
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

bool SymbolGenerator::HasSymbol(const std::string &name) {
  return scope_.symbols.find(name) != scope_.symbols.end();
}

}  // namespace query
