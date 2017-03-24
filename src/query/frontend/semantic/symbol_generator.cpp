// Copyright 2017 Memgraph
//
// Created by Teon Banek on 24-03-2017

#include "query/frontend/semantic/symbol_generator.hpp"

namespace query {

auto SymbolGenerator::CreateVariable(const std::string &name,
                                     SymbolGenerator::Variable::Type type) {
  auto symbol = symbol_table_.CreateSymbol(name);
  auto variable = SymbolGenerator::Variable{symbol, type};
  scope_.variables[name] = variable;
  return variable;
}

auto SymbolGenerator::GetOrCreateVariable(
    const std::string &name, SymbolGenerator::Variable::Type type) {
  auto search = scope_.variables.find(name);
  if (search != scope_.variables.end()) {
    auto variable = search->second;
    if (type != SymbolGenerator::Variable::Type::Any && type != variable.type) {
      throw TypeMismatchError(name, TypeToString(variable.type),
                              TypeToString(type));
    }
    return search->second;
  }
  return CreateVariable(name, type);
}

// Clauses

void SymbolGenerator::Visit(Create &create) { scope_.in_create = true; }
void SymbolGenerator::PostVisit(Create &create) { scope_.in_create = false; }

void SymbolGenerator::PostVisit(Return &ret) {
  for (auto &named_expr : ret.named_expressions_) {
    // Named expressions establish bindings for expressions which come after
    // return, but not for the expressions contained inside.
    symbol_table_[*named_expr] = CreateVariable(named_expr->name_).symbol;
  }
}

// Expressions

void SymbolGenerator::Visit(Identifier &ident) {
  Symbol symbol;
  if (scope_.in_pattern) {
    // Patterns can bind new symbols or reference already bound. But there
    // are the following special cases:
    //  1) Expressions in property maps `{prop_name: expr}` can only reference
    //     bound variables.
    //  2) Patterns used to create nodes and edges cannot redeclare already
    //     established bindings. Declaration only happens in single node
    //     patterns and in edge patterns. OpenCypher example,
    //     `MATCH (n) CREATE (n)` should throw an error that `n` is already
    //     declared. While `MATCH (n) CREATE (n) -[:R]-> (n)` is allowed,
    //     since `n` now references the bound node instead of declaring it.
    //     Additionally, we will support edge referencing in pattern:
    //     `MATCH (n) - [r] -> (n) - [r] -> (n) RETURN r`, which would
    //     usually raise redeclaration of `r`.
    if (scope_.in_property_map && !HasVariable(ident.name_)) {
      // Case 1)
      throw UnboundVariableError(ident.name_);
    } else if ((scope_.in_create_node || scope_.in_create_edge) &&
               HasVariable(ident.name_)) {
      // Case 2)
      throw RedeclareVariableError(ident.name_);
    }
    auto type = Variable::Type::Vertex;
    if (scope_.in_edge_atom) {
      type = Variable::Type::Edge;
    }
    symbol = GetOrCreateVariable(ident.name_, type).symbol;
  } else {
    // Everything else references a bound symbol.
    if (!HasVariable(ident.name_)) throw UnboundVariableError(ident.name_);
    symbol = scope_.variables[ident.name_].symbol;
  }
  symbol_table_[ident] = symbol;
}

// Pattern and its subparts.

void SymbolGenerator::Visit(Pattern &pattern) {
  scope_.in_pattern = true;
  if (scope_.in_create && pattern.atoms_.size() == 1) {
    debug_assert(dynamic_cast<NodeAtom *>(pattern.atoms_[0]),
                 "Expected a single NodeAtom in Pattern");
    scope_.in_create_node = true;
  }
}

void SymbolGenerator::PostVisit(Pattern &pattern) { scope_.in_pattern = false; }

void SymbolGenerator::Visit(NodeAtom &node_atom) {
  scope_.in_node_atom = true;
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
  if (scope_.in_create) {
    scope_.in_create_edge = true;
    if (edge_atom.edge_types_.size() != 1) {
      throw SemanticException(
          "A single relationship type must be specified "
          "when creating an edge.");
    }
    if (edge_atom.direction_ == EdgeAtom::Direction::BOTH) {
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

bool SymbolGenerator::HasVariable(const std::string &name) {
  return scope_.variables.find(name) != scope_.variables.end();
}

}  // namespace query
