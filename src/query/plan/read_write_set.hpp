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

#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_set>

#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class CallSubquery;
class Expression;
class SymbolTable;
}  // namespace memgraph::query

namespace memgraph::query::plan {

struct Matching;

/// What a clause reads from, or writes to, the graph. Keys are property, label and edge-type names. A read and a write
/// conflict when they can touch the same key; an unknown key (`n[$k]`, `+= $m`, `properties(n)`) is any key.
struct ReadWriteSet {
  /// The variable a property key is read or written through: one variable, or `mixed` for two or more, or an object
  /// that is no variable.
  struct KeyReach {
    std::optional<Symbol> symbol;
    bool mixed{false};

    void Note(const std::optional<Symbol> &through);
    void Merge(const KeyReach &other);

    /// Whether any read or write was noted.
    bool Any() const { return symbol || mixed; }

    /// The one variable, or nullopt when there is none or more than one.
    std::optional<Symbol> One() const { return mixed ? std::nullopt : symbol; }
  };

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
  /// Reads only: a fine-grained access check reads the node's labels (of every node read or written), through the
  /// variable that holds the node. It conflicts with `relabel` and nothing else, so a DELETE before it changes nothing.
  KeyReach access_check;
  /// Writes only: a SET or REMOVE label changed the labels of a node that already existed, the one write an access
  /// check can see. A created node's labels are not one: whether a read finds that node at all is a real conflict.
  KeyReach relabel;
  /// Per key in `properties`, the variable it is read or written through; `any_property_reach` for `any_property`.
  std::map<std::string, KeyReach> property_reach;
  KeyReach any_property_reach;
  /// Reads: the symbols a pattern expands from or through. Writes: the symbols a CREATE or MERGE first binds, which
  /// View::OLD cannot see until the command advances.
  std::unordered_set<Symbol> symbols;
  /// Reads only: the entity and path symbols an expression reads by value. One that may hold an entity created since
  /// the last barrier makes the read a View::NEW read (`ReadSeeingCreated`), which a later write must not reach.
  std::unordered_set<Symbol> referenced;

  /// Whether it names no graph key. `access_check` and `referenced` do not count: the first conflicts only with
  /// `relabel`, the second only marks a read as a View::NEW read.
  bool Empty() const;
  void Merge(const ReadWriteSet &other);
  /// Adds property @p key, read or written through @p through; nullopt (an object that is no variable) mixes it.
  void NoteProperty(const std::string &key, const std::optional<Symbol> &through);
  /// Sets `any_property`, read or written through @p through.
  void NoteAnyProperty(const std::optional<Symbol> &through);

  void Clear() { *this = ReadWriteSet{}; }

  /// These reads at View::NEW, which sees what this command created: without `symbols`.
  ReadWriteSet AsNewReader() const {
    auto reads = *this;
    reads.symbols.clear();
    return reads;
  }
};

/// Whether @p reads can observe any of @p writes. A pattern through a symbol @p writes binds conflicts too, which
/// matters only to a View::OLD reader: pass `AsNewReader()` for a View::NEW one.
bool Conflicts(const ReadWriteSet &reads, const ReadWriteSet &writes);

/// Whether the reads conflict with the writes only through `access_check`: a barrier for that conflict is
/// needed only while a fine-grained access check runs.
bool ConflictsOnlyInAccessCheck(const ReadWriteSet &reads, const ReadWriteSet &writes);

/// The variable through which the reads and the writes touch every key they conflict on, when they conflict on
/// property keys alone and both reach each such key through that one variable; nullopt otherwise.
std::optional<Symbol> ConflictsOnlyOnKeysOf(const ReadWriteSet &reads, const ReadWriteSet &writes);

/// The variables through which @p a and @p b reach the property keys they share; `Any()` is false when they share none.
ReadWriteSet::KeyReach SharedPropertyReach(const ReadWriteSet &a, const ReadWriteSet &b);

struct ReadsAndWrites {
  ReadWriteSet reads;
  ReadWriteSet writes;
};

/// Whether a value may be, or hold, a node, an edge or a path; a variable in @p non_graph holds none.
bool MayHoldEntity(Expression *value, const SymbolTable &symbol_table, const std::unordered_set<Symbol> &non_graph);

/// Whether the variable @p result that @p call returns may hold an entity in @p fresh: a RETURN expression of the
/// body may hold an entity and reads a variable in @p fresh or one the body binds itself. A result no RETURN names, as
/// in a UNION, may.
/// @param outer_bound the variables bound before @p call.
bool CallResultMayBeFresh(CallSubquery &call, const Symbol &result, const SymbolTable &symbol_table,
                          const std::unordered_set<Symbol> &fresh, const std::unordered_set<Symbol> &outer_bound,
                          const std::unordered_set<Symbol> &non_graph);

/// The reads and writes of @p tree (a clause, an expression or a whole query), subqueries included.
/// @param bound symbols bound before @p tree; a CREATE atom naming one creates nothing.
/// @param non_graph symbols that hold no graph entity (a LOAD CSV row), so a lookup on them reads nothing.
ReadsAndWrites CollectReadsAndWrites(utils::Visitable<HierarchicalTreeVisitor> &tree, const SymbolTable &symbol_table,
                                     const std::unordered_set<Symbol> &bound = {},
                                     const std::unordered_set<Symbol> &non_graph = {});

/// The reads of a planned MATCH: its pattern atoms and its filters.
ReadWriteSet CollectReads(const Matching &matching, const SymbolTable &symbol_table,
                          const std::unordered_set<Symbol> &bound, const std::unordered_set<Symbol> &non_graph);

}  // namespace memgraph::query::plan
