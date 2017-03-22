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
      symbol_table_[*named_expr] = CreateVariable(named_expr->name_).symbol;
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
      if (!HasVariable(ident.name_))
        throw UnboundVariableError(ident.name_);
      symbol = scope_.variables[ident.name_].symbol;
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
  }

  void Visit(NodeAtom &node_atom) override {
    scope_.in_node_atom = true;
    scope_.in_property_map = true;
    for (auto kv : node_atom.properties_) {
      kv.second->Accept(*this);
    }
    scope_.in_property_map = false;
  }
  void PostVisit(NodeAtom &node_atom) override {
    scope_.in_node_atom = false;
  }

  void Visit(EdgeAtom &edge_atom) override {
    scope_.in_edge_atom = true;
    if (scope_.in_create) {
      scope_.in_create_edge = true;
      if (edge_atom.edge_types_.size() != 1) {
        throw SemanticException("A single relationship type must be specified "
                                "when creating an edge.");
      }
    }
  }
  void PostVisit(EdgeAtom &edge_atom) override {
    scope_.in_edge_atom = false;
    scope_.in_create_edge = false;
  }

 private:
  // A variable stores the associated symbol and its type.
  struct Variable {
    // This is similar to TypedValue::Type, but this has `Any` type.
    enum class Type : unsigned {
      Any, Vertex, Edge, Path
    };

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

  bool HasVariable(const std::string &name) {
    return scope_.variables.find(name) != scope_.variables.end();
  }

  // Returns a new variable with a freshly generated symbol. Previous mapping
  // of the same name to a different variable is replaced with the new one.
  Variable CreateVariable(
      const std::string &name, Variable::Type type = Variable::Type::Any) {
    auto symbol = symbol_table_.CreateSymbol(name);
    auto variable = Variable{symbol, type};
    scope_.variables[name] = variable;
    return variable;
  }

  // Returns the variable by name. If the mapping already exists, checks if the
  // types match. Otherwise, returns a new variable.
  Variable GetOrCreateVariable(
      const std::string &name, Variable::Type type = Variable::Type::Any) {
    auto search = scope_.variables.find(name);
    if (search != scope_.variables.end()) {
      auto variable = search->second;
      if (type != Variable::Type::Any && type != variable.type) {
        throw TypeMismatchError(name, TypeToString(variable.type),
                                TypeToString(type));
      }
      return search->second;
    }
    return CreateVariable(name, type);
  }

  SymbolTable &symbol_table_;
  Scope scope_;
};

}
