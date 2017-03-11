#pragma once

#include "utils/exceptions/basic_exception.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/typecheck/symbol_table.hpp"

namespace query {

class TypeCheckVisitor : public TreeVisitorBase {
 public:
  TypeCheckVisitor(SymbolTable& symbol_table) : symbol_table_(symbol_table) {}

  // Start of the tree is a Query.
  void Visit(Query& query) override {}
  // Expressions
  void Visit(Ident& ident) override {
    Symbol symbol;
    if (scope_.in_pattern) {
      symbol = GetOrCreateSymbol(ident.identifier_);
    } else {
      if (!HasSymbol(ident.identifier_))
        // TODO: Special exception for type check
        throw BasicException("Unbound identifier: " + ident.identifier_);
      symbol = scope_.variables[ident.identifier_];
    }
    symbol_table_[ident] = symbol;
  }
  // Clauses
  void Visit(Match& match) override {}
  void PreVisit(Return& ret) override {
    scope_.in_return = true;
  }
  void PostVisit(Return& ret) override {
    scope_.in_return = false;
  }
  void Visit(Return& ret) override {}
  // Pattern and its subparts.
  void PreVisit(Pattern& pattern) override {
    scope_.in_pattern = true;
  }
  void PostVisit(Pattern& pattern) override {
    scope_.in_pattern = false;
  }
  void Visit(Pattern& pattern) override {}
  void Visit(NodePart& node_part) override {}
  void Visit(EdgePart& edge_part) override {}

 private:
  struct Scope {
    Scope() : in_pattern(false), in_return(false) {}
    bool in_pattern;
    bool in_return;
    std::map<std::string, Symbol> variables;
  };

  bool HasSymbol(const std::string& name)
  {
    return scope_.variables.find(name) != scope_.variables.end();
  }

  Symbol GetOrCreateSymbol(const std::string& name)
  {
    auto search = scope_.variables.find(name);
    if (search != scope_.variables.end()) {
      return search->second;
    }
    auto symbol = symbol_table_.CreateSymbol(name);
    scope_.variables[name] = symbol;
    return symbol;
  }
  SymbolTable& symbol_table_;
  Scope scope_;
};

}
