// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2017 Memgraph
//
// Created by Teon Banek on 11-03-2017

#pragma once

#include <optional>
#include <vector>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

namespace memgraph::query {

/// Visits the AST and generates symbols for variables.
///
/// During the process of symbol generation, simple semantic checks are
/// performed. Such as, redeclaring a variable or conflicting expectations of
/// variable types.
class SymbolGenerator : public HierarchicalTreeVisitor {
 public:
  explicit SymbolGenerator(SymbolTable *symbol_table, const std::vector<Identifier *> &predefined_identifiers);

  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;
  using typename HierarchicalTreeVisitor::ReturnType;

  // Query
  bool PreVisit(SingleQuery &) override;

  // Union
  bool PreVisit(CypherUnion &) override;
  bool PostVisit(CypherUnion &) override;

  // Clauses
  bool PreVisit(Create &) override;
  bool PostVisit(Create &) override;
  bool PreVisit(CallProcedure &) override;
  bool PostVisit(CallProcedure &) override;
  bool PreVisit(CallSubquery & /*unused*/) override;
  bool PostVisit(CallSubquery & /*unused*/) override;
  bool PreVisit(LoadCsv &) override;
  bool PostVisit(LoadCsv &) override;
  bool PreVisit(Return &) override;
  bool PostVisit(Return &) override;
  bool PreVisit(With &) override;
  bool PreVisit(Where &) override;
  bool PostVisit(Where &) override;
  bool PreVisit(Merge &) override;
  bool PostVisit(Merge &) override;
  bool PostVisit(Unwind &) override;
  bool PreVisit(Match &) override;
  bool PostVisit(Match &) override;
  bool PreVisit(Foreach &) override;
  bool PostVisit(Foreach &) override;
  bool PreVisit(SetProperty & /*set_property*/) override;
  bool PostVisit(SetProperty & /*set_property*/) override;

  // Expressions
  ReturnType Visit(Identifier &) override;
  ReturnType Visit(PrimitiveLiteral &) override { return true; }
  bool PreVisit(MapLiteral &) override { return true; }
  bool PostVisit(MapLiteral &) override;
  ReturnType Visit(ParameterLookup &) override { return true; }
  bool PreVisit(Aggregation &) override;
  bool PostVisit(Aggregation &) override;
  bool PreVisit(IfOperator &) override;
  bool PostVisit(IfOperator &) override;
  bool PreVisit(All &) override;
  bool PreVisit(Single &) override;
  bool PreVisit(Any &) override;
  bool PreVisit(None &) override;
  bool PreVisit(Reduce &) override;
  bool PreVisit(Extract &) override;
  bool PreVisit(Exists & /*exists*/) override;
  bool PostVisit(Exists & /*exists*/) override;
  bool PreVisit(NamedExpression & /*unused*/) override;

  // Pattern and its subparts.
  bool PreVisit(Pattern &) override;
  bool PostVisit(Pattern &) override;
  bool PreVisit(NodeAtom &) override;
  bool PostVisit(NodeAtom &) override;
  bool PreVisit(EdgeAtom &) override;
  bool PostVisit(EdgeAtom &) override;

 private:
  // Scope stores the state of where we are when visiting the AST and a map of
  // names to symbols.
  struct Scope {
    bool in_pattern{false};
    bool in_merge{false};
    bool in_create{false};
    // in_create_node is true if we are creating or merging *only* a node.
    // Therefore, it is *not* equivalent to (in_create || in_merge) &&
    // in_node_atom.
    bool in_create_node{false};
    // True if creating an edge;
    // shortcut for (in_create || in_merge) && visiting_edge.
    bool in_create_edge{false};
    bool in_node_atom{false};
    EdgeAtom *visiting_edge{nullptr};
    bool in_aggregation{false};
    bool in_return{false};
    bool in_with{false};
    bool in_skip{false};
    bool in_limit{false};
    bool in_order_by{false};
    bool in_where{false};
    bool in_match{false};
    bool in_foreach{false};
    bool in_exists{false};
    bool in_reduce{false};
    bool in_set_property{false};
    bool in_call_subquery{false};
    bool has_return{false};
    // True when visiting a pattern atom (node or edge) identifier, which can be
    // reused or created in the pattern itself.
    bool in_pattern_atom_identifier{false};
    // True when visiting range bounds of a variable path.
    bool in_edge_range{false};
    // True if the return/with contains an aggregation in any named expression.
    bool has_aggregation{false};
    // Map from variable names to symbols.
    std::map<std::string, Symbol> symbols;
    // Identifiers found in property maps of patterns or as variable length path
    // bounds in a single Match clause. They need to be checked after visiting
    // Match. Identifiers created by naming vertices, edges and paths are *not*
    // stored in here.
    std::vector<Identifier *> identifiers_in_match;
    // Number of nested IfOperators.
    int num_if_operators{0};
    std::unordered_set<std::string> prev_return_names{};
    std::unordered_set<std::string> curr_return_names{};
  };

  static std::optional<Symbol> FindSymbolInScope(const std::string &name, const Scope &scope, Symbol::Type type);

  bool HasSymbol(const std::string &name) const;

  // @return true if it added a predefined identifier with that name
  bool ConsumePredefinedIdentifier(const std::string &name);

  // Returns a freshly generated symbol. Previous mapping of the same name to a
  // different symbol is replaced with the new one.
  auto CreateSymbol(const std::string &name, bool user_declared, Symbol::Type type = Symbol::Type::ANY,
                    int token_position = -1);

  // Returns a freshly generated anonymous symbol.
  auto CreateAnonymousSymbol(Symbol::Type type = Symbol::Type::ANY);

  auto GetOrCreateSymbol(const std::string &name, bool user_declared, Symbol::Type type = Symbol::Type::ANY);
  // Returns the symbol by name. If the mapping already exists, checks if the
  // types match. Otherwise, returns a new symbol.

  void VisitReturnBody(ReturnBody &body, Where *where = nullptr);

  void VisitWithIdentifiers(Expression *, const std::vector<Identifier *> &);

  SymbolTable *symbol_table_;

  // Identifiers which are injected from outside the query. Each identifier
  // is mapped by its name.
  std::unordered_map<std::string, Identifier *> predefined_identifiers_;
  std::vector<Scope> scopes_;
};

inline SymbolTable MakeSymbolTable(CypherQuery *query, const std::vector<Identifier *> &predefined_identifiers = {}) {
  SymbolTable symbol_table;
  SymbolGenerator symbol_generator(&symbol_table, predefined_identifiers);
  query->single_query_->Accept(symbol_generator);
  for (auto *cypher_union : query->cypher_unions_) {
    cypher_union->Accept(symbol_generator);
  }
  return symbol_table;
}

}  // namespace memgraph::query
