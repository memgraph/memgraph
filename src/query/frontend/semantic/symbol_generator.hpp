// Copyright 2017 Memgraph
//
// Created by Teon Banek on 11-03-2017

#pragma once

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

namespace query {

///
/// Visits the AST and generates symbols for variables.
///
/// During the process of symbol generation, simple semantic checks are
/// performed. Such as, redeclaring a variable or conflicting expectations of
/// variable types.
class SymbolGenerator : public TreeVisitorBase {
 public:
  SymbolGenerator(SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  using TreeVisitorBase::Visit;
  using TreeVisitorBase::PostVisit;

  // Clauses
  void Visit(Create &create) override;
  void PostVisit(Create &create) override;
  void PostVisit(Return &ret) override;

  // Expressions
  void Visit(Identifier &ident) override;

  // Pattern and its subparts.
  void Visit(Pattern &pattern) override;
  void PostVisit(Pattern &pattern) override;
  void Visit(NodeAtom &node_atom) override;
  void PostVisit(NodeAtom &node_atom) override;
  void Visit(EdgeAtom &edge_atom) override;
  void PostVisit(EdgeAtom &edge_atom) override;

 private:
  // A variable stores the associated symbol and its type.
  struct Variable {
    // This is similar to TypedValue::Type, but this has `Any` type.
    enum class Type { Any, Vertex, Edge, Path };

    Symbol symbol;
    Type type{Type::Any};
  };

  std::string TypeToString(Variable::Type type) {
    const char *enum_string[] = {"Any", "Vertex", "Edge", "Path"};
    return enum_string[static_cast<int>(type)];
  }

  // Scope stores the state of where we are when visiting the AST and a map of
  // names to variables.
  struct Scope {
    bool in_pattern{false};
    bool in_create{false};
    // in_create_node is true if we are creating *only* a node. Therefore, it
    // is *not* equivalent to in_create && in_node_atom.
    bool in_create_node{false};
    // True if creating an edge; shortcut for in_create && in_edge_atom.
    bool in_create_edge{false};
    bool in_node_atom{false};
    bool in_edge_atom{false};
    bool in_property_map{false};
    std::map<std::string, Variable> variables;
  };

  bool HasVariable(const std::string &name);

  // Returns a new variable with a freshly generated symbol. Previous mapping
  // of the same name to a different variable is replaced with the new one.
  auto CreateVariable(const std::string &name,
                      Variable::Type type = Variable::Type::Any);

  // Returns the variable by name. If the mapping already exists, checks if the
  // types match. Otherwise, returns a new variable.
  auto GetOrCreateVariable(const std::string &name,
                           Variable::Type type = Variable::Type::Any);

  SymbolTable &symbol_table_;
  Scope scope_;
};

}  // namespace query
