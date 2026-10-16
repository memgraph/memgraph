// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <set>
#include <string>
#include <unordered_set>

#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class SymbolTable;
}  // namespace memgraph::query

namespace memgraph::query::plan {

struct Matching;

/// What a clause reads from, or writes to, the graph. Keys are property, label and edge-type names. A read and a write
/// conflict when they can touch the same key; an unknown key (`n[$k]`, `+= $m`, `properties(n)`) is any key.
struct ReadWriteSet {
  std::set<std::string> properties;
  bool any_property{false};
  std::set<std::string> labels;
  bool any_label{false};
  std::set<std::string> edge_types;
  bool any_edge_type{false};
  /// Node existence: an unlabelled node pattern reads it; CREATE and DELETE write it.
  bool nodes{false};
  /// A procedure, a DELETE or a graph-reading function: conflicts with any non-empty set.
  bool everything{false};
  /// Reads: the symbols a pattern expands from or through. Writes: the symbols a CREATE or MERGE first binds, which
  /// View::OLD cannot see until the command advances.
  std::unordered_set<Symbol> symbols;

  bool Empty() const;
  void Merge(const ReadWriteSet &other);

  void Clear() { *this = ReadWriteSet{}; }
};

/// Whether @p reads can observe any of @p writes.
bool Conflicts(const ReadWriteSet &reads, const ReadWriteSet &writes);

struct ReadsAndWrites {
  ReadWriteSet reads;
  ReadWriteSet writes;
};

/// The reads and writes of @p tree (a clause, an expression or a whole query), subqueries included.
/// @param bound symbols bound before @p tree; a CREATE atom naming one creates nothing.
/// @param non_graph symbols that hold no graph entity (a LOAD CSV row), so a lookup on them reads nothing.
ReadsAndWrites CollectReadsAndWrites(utils::Visitable<HierarchicalTreeVisitor> &tree, const SymbolTable &symbol_table,
                                     const std::unordered_set<Symbol> &bound = {},
                                     const std::unordered_set<Symbol> &non_graph = {});

/// The reads of a planned MATCH: its pattern atoms and its filters.
ReadWriteSet CollectReads(const Matching &matching, const SymbolTable &symbol_table,
                          const std::unordered_set<Symbol> &non_graph = {});

}  // namespace memgraph::query::plan
