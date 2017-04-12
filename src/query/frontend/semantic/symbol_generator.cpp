// Copyright 2017 Memgraph
//
// Created by Teon Banek on 24-03-2017

#include "query/frontend/semantic/symbol_generator.hpp"

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
    if (type != Symbol::Type::Any && type != symbol.type_) {
      throw TypeMismatchError(name, Symbol::TypeToString(symbol.type_),
                              Symbol::TypeToString(type));
    }
    return search->second;
  }
  return CreateSymbol(name, type);
}

// Clauses

void SymbolGenerator::Visit(Create &create) { scope_.in_create = true; }
void SymbolGenerator::PostVisit(Create &create) { scope_.in_create = false; }

void SymbolGenerator::PostVisit(Return &ret) {
  for (auto &named_expr : ret.named_expressions_) {
    // Named expressions establish bindings for expressions which come after
    // return, but not for the expressions contained inside.
    symbol_table_[*named_expr] = CreateSymbol(named_expr->name_);
  }
}

void SymbolGenerator::SetWithSymbols(With &with) {
  // WITH clause removes declarations of all the previous variables and declares
  // only those established through named expressions. New declarations must not
  // be visible inside named expressions themselves.
  scope_.symbols.clear();
  for (auto &named_expr : with.named_expressions_) {
    symbol_table_[*named_expr] = CreateSymbol(named_expr->name_);
  }
}

void SymbolGenerator::Visit(With &with) {
  scope_.with = &with;
}

void SymbolGenerator::Visit(Where &where) {
  if (scope_.with) {
    // New symbols must be visible in WHERE clause, so this must be done here
    // and not in PostVisit(With&).
    SetWithSymbols(*scope_.with);
  }
}

void SymbolGenerator::PostVisit(With &with) {
  if (!with.where_) {
    // This wasn't done when visiting Where, so do it here.
    SetWithSymbols(with);
  }
  scope_.with = nullptr;
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
    if (scope_.in_property_map && !HasSymbol(ident.name_)) {
      // Case 1)
      throw UnboundVariableError(ident.name_);
    } else if ((scope_.in_create_node || scope_.in_create_edge) &&
               HasSymbol(ident.name_)) {
      // Case 2)
      throw RedeclareVariableError(ident.name_);
    }
    auto type = Symbol::Type::Vertex;
    if (scope_.in_edge_atom) {
      type = Symbol::Type::Edge;
    }
    symbol = GetOrCreateSymbol(ident.name_, type);
  } else {
    // Everything else references a bound symbol.
    if (!HasSymbol(ident.name_)) throw UnboundVariableError(ident.name_);
    symbol = scope_.symbols[ident.name_];
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

bool SymbolGenerator::HasSymbol(const std::string &name) {
  return scope_.symbols.find(name) != scope_.symbols.end();
}

}  // namespace query
