#pragma once

#include "utils/exceptions/basic_exception.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/typecheck/symbol_table.hpp"

namespace query {

class TypeCheckVisitor : public TreeVisitorBase {
 public:
  TypeCheckVisitor(SymbolTable& symbol_table) : symbol_table_(symbol_table) {}

  // Expressions
  void PreVisit(NamedExpression& named_expr) override {
    scope_.in_named_expr = true;
  }
  void Visit(Identifier& ident) override {
    Symbol symbol;
    if (scope_.in_named_expr) {
      // TODO: Handle this better, so that the `with` variables aren't
      // shadowed.
      if (HasSymbol(ident.name_)) {
        scope_.revert_variable = true;
        scope_.old_symbol = scope_.variables[ident.name_];
      }
      symbol = CreateSymbol(ident.name_);
    } else if (scope_.in_pattern) {
      symbol = GetOrCreateSymbol(ident.name_);
    } else {
      if (!HasSymbol(ident.name_))
        // TODO: Special exception for type check
        throw BasicException("Unbound identifier: " + ident.name_);
      symbol = scope_.variables[ident.name_];
    }
    symbol_table_[ident] = symbol;
  }
  void PostVisit(Identifier& ident) override {
    if (scope_.in_named_expr) {
      if (scope_.revert_variable) {
        scope_.variables[ident.name_] = scope_.old_symbol;
      }
      scope_.in_named_expr = false;
      scope_.revert_variable = false;
    }
  }
  // Clauses
  void PreVisit(Return& ret) override {
    scope_.in_return = true;
  }
  void PostVisit(Return& ret) override {
    scope_.in_return = false;
  }
  // Pattern and its subparts.
  void PreVisit(Pattern& pattern) override {
    scope_.in_pattern = true;
  }
  void PostVisit(Pattern& pattern) override {
    scope_.in_pattern = false;
  }

 private:
  struct Scope {
    Scope()
      : in_pattern(false), in_return(false), in_named_expr(false),
        revert_variable(false) {}
    bool in_pattern;
    bool in_return;
    bool in_named_expr;
    bool revert_variable;
    Symbol old_symbol;
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
