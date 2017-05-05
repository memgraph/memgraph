// Copyright 2017 Memgraph
//
// Created by Teon Banek on 24-03-2017

#include "query/frontend/semantic/symbol_generator.hpp"

#include <unordered_set>

namespace query {

auto SymbolGenerator::CreateSymbol(const std::string &name, Symbol::Type type) {
  auto symbol = symbol_table_.CreateSymbol(name, type);
  scope_.symbols[name] = symbol;
  return symbol;
}

auto SymbolGenerator::GetOrCreateSymbol(const std::string &name,
                                        Symbol::Type type) {
  auto search = scope_.symbols.find(name);
  if (search != scope_.symbols.end()) {
    auto symbol = search->second;
    // Unless we have `Any` type, check that types match.
    if (type != Symbol::Type::Any && symbol.type_ != Symbol::Type::Any &&
        type != symbol.type_) {
      throw TypeMismatchError(name, Symbol::TypeToString(symbol.type_),
                              Symbol::TypeToString(type));
    }
    return search->second;
  }
  return CreateSymbol(name, type);
}

void SymbolGenerator::VisitReturnBody(ReturnBody &body, Where *where) {
  for (auto &expr : body.named_expressions) {
    expr->Accept(*this);
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
  for (auto &named_expr : body.named_expressions) {
    const auto &name = named_expr->name_;
    if (!new_names.insert(name).second) {
      throw SemanticException(
          "Multiple results with the same name '{}' are not allowed.", name);
    }
    // An improvement would be to infer the type of the expression, so that the
    // new symbol would have a more specific type.
    symbol_table_[*named_expr] = CreateSymbol(name);
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

void SymbolGenerator::Visit(Create &create) { scope_.in_create = true; }
void SymbolGenerator::PostVisit(Create &create) { scope_.in_create = false; }

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

void SymbolGenerator::Visit(Where &) { scope_.in_where = true; }
void SymbolGenerator::PostVisit(Where &) { scope_.in_where = false; }

void SymbolGenerator::Visit(Merge &) { scope_.in_merge = true; }
void SymbolGenerator::PostVisit(Merge &) { scope_.in_merge = false; }

void SymbolGenerator::PostVisit(Unwind &unwind) {
  const auto &name = unwind.named_expression_->name_;
  if (HasSymbol(name)) {
    throw RedeclareVariableError(name);
  }
  symbol_table_[*unwind.named_expression_] = CreateSymbol(name);
}

void SymbolGenerator::Visit(Match &) { scope_.in_match = true; }
void SymbolGenerator::PostVisit(Match &) {
  scope_.in_match = false;
  // Check variables in property maps after visiting Match, so that they can
  // reference symbols out of bind order.
  for (auto &ident : scope_.identifiers_in_property_maps) {
    if (!HasSymbol(ident->name_)) throw UnboundVariableError(ident->name_);
    symbol_table_[*ident] = scope_.symbols[ident->name_];
  }
  scope_.identifiers_in_property_maps.clear();
}

// Expressions

void SymbolGenerator::Visit(Identifier &ident) {
  if (scope_.in_skip || scope_.in_limit) {
    throw SemanticException("Variables are not allowed in {}",
                            scope_.in_skip ? "SKIP" : "LIMIT");
  }
  Symbol symbol;
  if (scope_.in_pattern && !scope_.in_property_map) {
    // Patterns can bind new symbols or reference already bound. But there
    // are the following special cases:
    //  1) Patterns used to create nodes and edges cannot redeclare already
    //     established bindings. Declaration only happens in single node
    //     patterns and in edge patterns. OpenCypher example,
    //     `MATCH (n) CREATE (n)` should throw an error that `n` is already
    //     declared. While `MATCH (n) CREATE (n) -[:R]-> (n)` is allowed,
    //     since `n` now references the bound node instead of declaring it.
    //     Additionally, we will support edge referencing in pattern:
    //     `MATCH (n) - [r] -> (n) - [r] -> (n) RETURN r`, which would
    //     usually raise redeclaration of `r`.
    if ((scope_.in_create_node || scope_.in_create_edge) &&
        HasSymbol(ident.name_)) {
      // Case 1)
      throw RedeclareVariableError(ident.name_);
    }
    auto type = Symbol::Type::Vertex;
    if (scope_.in_edge_atom) {
      type = Symbol::Type::Edge;
    }
    symbol = GetOrCreateSymbol(ident.name_, type);
  } else if (scope_.in_pattern && scope_.in_property_map && scope_.in_match) {
    // Variables in property maps during MATCH can reference symbols bound later
    // in the same MATCH. We collect them here, so that they can be checked
    // after visiting Match.
    scope_.identifiers_in_property_maps.emplace_back(&ident);
  } else {
    // Everything else references a bound symbol.
    if (!HasSymbol(ident.name_)) throw UnboundVariableError(ident.name_);
    symbol = scope_.symbols[ident.name_];
  }
  symbol_table_[ident] = symbol;
}

void SymbolGenerator::Visit(Aggregation &aggr) {
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
  // Create a virtual symbol for aggregation result.
  // Currently, we only have aggregation operators which return numbers.
  symbol_table_[aggr] = symbol_table_.CreateSymbol("", Symbol::Type::Number);
  scope_.in_aggregation = true;
  scope_.has_aggregation = true;
}

void SymbolGenerator::PostVisit(Aggregation &aggr) {
  scope_.in_aggregation = false;
}

// Pattern and its subparts.

void SymbolGenerator::Visit(Pattern &pattern) {
  scope_.in_pattern = true;
  if ((scope_.in_create || scope_.in_merge) && pattern.atoms_.size() == 1U) {
    debug_assert(dynamic_cast<NodeAtom *>(pattern.atoms_[0]),
                 "Expected a single NodeAtom in Pattern");
    scope_.in_create_node = true;
  }
}

void SymbolGenerator::PostVisit(Pattern &pattern) {
  scope_.in_pattern = false;
  scope_.in_create_node = false;
}

void SymbolGenerator::Visit(NodeAtom &node_atom) {
  scope_.in_node_atom = true;
  bool props_or_labels =
      !node_atom.properties_.empty() || !node_atom.labels_.empty();
  const auto &node_name = node_atom.identifier_->name_;
  if (scope_.in_create && props_or_labels && HasSymbol(node_name)) {
    throw SemanticException(
        "Cannot create node '" + node_name +
        "' with labels or properties, because it is already declared.");
  }
  scope_.in_property_map = true;
  for (auto kv : node_atom.properties_) {
    kv.second->Accept(*this);
  }
  scope_.in_property_map = false;
}

void SymbolGenerator::PostVisit(NodeAtom &node_atom) {
  scope_.in_node_atom = false;
}

void SymbolGenerator::Visit(EdgeAtom &edge_atom) {
  scope_.in_edge_atom = true;
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
  }
}

void SymbolGenerator::PostVisit(EdgeAtom &edge_atom) {
  scope_.in_edge_atom = false;
  scope_.in_create_edge = false;
}

bool SymbolGenerator::HasSymbol(const std::string &name) {
  return scope_.symbols.find(name) != scope_.symbols.end();
}

}  // namespace query
