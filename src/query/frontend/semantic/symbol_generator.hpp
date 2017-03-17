#pragma once

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

namespace query {

class SymbolGenerator : public TreeVisitorBase {
 public:
  SymbolGenerator(SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  using TreeVisitorBase::Visit;
  using TreeVisitorBase::PostVisit;

  // Clauses
  void Visit(Create &create) override {
    scope_.in_create = true;
  }
  void PostVisit(Create &create) override {
    scope_.in_create = false;
  }

  void PostVisit(Return &ret) override {
    for (auto &named_expr : ret.named_expressions_) {
      // Named expressions establish bindings for expressions which come after
      // return, but not for the expressions contained inside.
      symbol_table_[*named_expr] = CreateSymbol(named_expr->name_);
    }
  }

  // Expressions
  void Visit(Identifier &ident) override {
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
      symbol = GetOrCreateSymbol(ident.name_);
    } else {
      // Everything else references a bound symbol.
      if (!HasSymbol(ident.name_))
        throw UnboundVariableError(ident.name_);
      symbol = scope_.variables[ident.name_];
    }
    symbol_table_[ident] = symbol;
  }

  // Pattern and its subparts.
  void Visit(Pattern &pattern) override {
    scope_.in_pattern = true;
    if (scope_.in_create && pattern.atoms_.size() == 1) {
      debug_assert(dynamic_cast<NodeAtom*>(pattern.atoms_[0]),
                   "Expected a single NodeAtom in Pattern");
      scope_.in_create_node = true;
    }
  }
  void PostVisit(Pattern &pattern) override {
    scope_.in_pattern = false;
    scope_.in_create_node = false;
  }

  void Visit(NodeAtom &node_atom) override {
    scope_.in_property_map = true;
    for (auto kv : node_atom.properties_) {
      kv.second->Accept(*this);
    }
    scope_.in_property_map = false;
  }

  void Visit(EdgeAtom &edge_atom) override {
    if (scope_.in_create) {
      scope_.in_create_edge = true;
    }
  }
  void PostVisit(EdgeAtom &edge_atom) override {
    scope_.in_create_edge = false;
  }

 private:
  struct Scope {
    bool in_pattern{false};
    bool in_create{false};
    bool in_create_node{false};
    bool in_create_edge{false};
    bool in_property_map{false};
    std::map<std::string, Symbol> variables;
  };

  bool HasSymbol(const std::string &name) {
    return scope_.variables.find(name) != scope_.variables.end();
  }

  Symbol CreateSymbol(const std::string &name) {
    auto symbol = symbol_table_.CreateSymbol(name);
    scope_.variables[name] = symbol;
    return symbol;
  }

  Symbol GetOrCreateSymbol(const std::string &name) {
    auto search = scope_.variables.find(name);
    if (search != scope_.variables.end()) {
      return search->second;
    }
    return CreateSymbol(name);
  }

  SymbolTable &symbol_table_;
  Scope scope_;
};

}
