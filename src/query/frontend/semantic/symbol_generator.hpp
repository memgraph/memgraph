#pragma once

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

namespace query {

class SymbolGenerator : public TreeVisitorBase {
 public:
  SymbolGenerator(SymbolTable& symbol_table) : symbol_table_(symbol_table) {}

  using TreeVisitorBase::Visit;
  using TreeVisitorBase::PostVisit;

  // Clauses
  void PostVisit(Return& ret) override {
    for (auto &named_expr : ret.named_expressions_) {
      // Named expressions establish bindings for expressions which come after
      // return, but not for the expressions contained inside.
      symbol_table_[*named_expr] = CreateSymbol(named_expr->name_);
    }
  }

  // Expressions
  void Visit(Identifier& ident) override {
    Symbol symbol;
    if (scope_.in_pattern) {
      symbol = GetOrCreateSymbol(ident.name_);
    } else {
      if (!HasSymbol(ident.name_))
        // TODO: Special exception for type check
        throw SemanticException("Unbound identifier: " + ident.name_);
      symbol = scope_.variables[ident.name_];
    }
    symbol_table_[ident] = symbol;
  }
  // Pattern and its subparts.
  void Visit(Pattern& pattern) override {
    scope_.in_pattern = true;
  }
  void PostVisit(Pattern& pattern) override {
    scope_.in_pattern = false;
  }

 private:
  struct Scope {
    Scope() : in_pattern(false) {}
    bool in_pattern;
    std::map<std::string, Symbol> variables;
  };

  bool HasSymbol(const std::string& name)
  {
    return scope_.variables.find(name) != scope_.variables.end();
  }

  Symbol CreateSymbol(const std::string &name)
  {
    auto symbol = symbol_table_.CreateSymbol(name);
    scope_.variables[name] = symbol;
    return symbol;
  }

  Symbol GetOrCreateSymbol(const std::string& name)
  {
    auto search = scope_.variables.find(name);
    if (search != scope_.variables.end()) {
      return search->second;
    }
    return CreateSymbol(name);
  }

  SymbolTable& symbol_table_;
  Scope scope_;
};

}
