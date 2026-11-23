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

#include <algorithm>
#include <array>
#include <cstdint>
#include <utility>
#include <vector>

#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

/// Where the frontend interned a property's name, which is what the AST carries in place of the
/// name itself. Both the names an `AstStorage` holds and the ids an `EvaluationContext` resolves
/// them to are indexed by it, so nothing holding one needs the name as well. Not a
/// `storage::PropertyId`, which is what names a property to the storage engine.
class AstPropertyIndex {
 public:
  explicit AstPropertyIndex(PropertyIx const &property) : value_{property.ix} {}

  [[nodiscard]] auto Position() const -> size_t { return static_cast<size_t>(value_); }

  friend bool operator==(AstPropertyIndex const &, AstPropertyIndex const &) = default;
  friend auto operator<=>(AstPropertyIndex const &, AstPropertyIndex const &) = default;

 private:
  int64_t value_;
};

/// Which properties a plan may write, which is what decides whether two reads of one property can
/// answer differently. A read of a property nothing writes answers the same at either view, so a
/// slot holding it stands in for either read.
///
/// Which record a write lands on is deliberately not part of this. A projection binds a new symbol
/// to the record it was given, so a write through one symbol reaches a record a slot holds under
/// another, and naming the symbol would call that write somebody else's.
class PropertyWrites {
 public:
  void Note(AstPropertyIndex property) { properties_.push_back(property); }

  void NoteEverything() { everything_ = true; }

  [[nodiscard]] auto MayWrite(AstPropertyIndex property) const -> bool {
    return everything_ || std::ranges::find(properties_, property) != properties_.end();
  }

 private:
  bool everything_{false};
  std::vector<AstPropertyIndex> properties_;
};

/// What one operator may write to the properties a row has already read.
///
/// Every operator says so below, and an operator saying nothing is a compile error rather than a
/// silent answer of nothing: the table is folded over the list of operators the visitor is built
/// from, which an operator has to be in to be visited at all.
///
/// An operator that writes properties it cannot name, and one holding a branch this does not walk,
/// both answer for everything. That costs a saving where a plan writes, and never an answer.
template <class TOperator>
struct OperatorWrites;

template <>
struct OperatorWrites<Once> {
  static void Note(Once const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<CreateNode> {
  static void Note(CreateNode const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<CreateExpand> {
  static void Note(CreateExpand const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAll> {
  static void Note(ScanAll const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByLabel> {
  static void Note(ScanAllByLabel const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByLabelProperties> {
  static void Note(ScanAllByLabelProperties const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllById> {
  static void Note(ScanAllById const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByEdge> {
  static void Note(ScanAllByEdge const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByEdgeType> {
  static void Note(ScanAllByEdgeType const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByEdgeTypeProperty> {
  static void Note(ScanAllByEdgeTypeProperty const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByEdgeProperty> {
  static void Note(ScanAllByEdgeProperty const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByEdgeId> {
  static void Note(ScanAllByEdgeId const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByVertexProperty> {
  static void Note(ScanAllByVertexProperty const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByPointDistance> {
  static void Note(ScanAllByPointDistance const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanAllByPointWithinbbox> {
  static void Note(ScanAllByPointWithinbbox const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Expand> {
  static void Note(Expand const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ExpandVariable> {
  static void Note(ExpandVariable const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ConstructNamedPath> {
  static void Note(ConstructNamedPath const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Filter> {
  static void Note(Filter const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Produce> {
  static void Note(Produce const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Delete> {
  static void Note(Delete const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<SetProperty> {
  static void Note(SetProperty const &op, PropertyWrites &writes) { writes.Note(AstPropertyIndex{op.lhs_->property_}); }
};

template <>
struct OperatorWrites<SetProperties> {
  static void Note(SetProperties const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<SetLabels> {
  static void Note(SetLabels const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<RemoveProperty> {
  static void Note(RemoveProperty const &op, PropertyWrites &writes) {
    writes.Note(AstPropertyIndex{op.lhs_->property_});
  }
};

template <>
struct OperatorWrites<RemoveLabels> {
  static void Note(RemoveLabels const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<EdgeUniquenessFilter> {
  static void Note(EdgeUniquenessFilter const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Accumulate> {
  static void Note(Accumulate const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Aggregate> {
  static void Note(Aggregate const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Skip> {
  static void Note(Skip const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Limit> {
  static void Note(Limit const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<OrderBy> {
  static void Note(OrderBy const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Merge> {
  static void Note(Merge const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<Optional> {
  static void Note(Optional const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<Unwind> {
  static void Note(Unwind const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<CardinalityScale> {
  static void Note(CardinalityScale const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Distinct> {
  static void Note(Distinct const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Union> {
  static void Note(Union const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<Cartesian> {
  static void Note(Cartesian const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<CallProcedure> {
  static void Note(CallProcedure const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<LoadCsv> {
  static void Note(LoadCsv const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<Foreach> {
  static void Note(Foreach const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<EmptyResult> {
  static void Note(EmptyResult const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<EvaluatePatternFilter> {
  static void Note(EvaluatePatternFilter const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<Apply> {
  static void Note(Apply const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<IndexedJoin> {
  static void Note(IndexedJoin const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<HashJoin> {
  static void Note(HashJoin const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<RollUpApply> {
  static void Note(RollUpApply const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<Conditional> {
  static void Note(Conditional const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<PeriodicCommit> {
  static void Note(PeriodicCommit const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<PeriodicSubquery> {
  static void Note(PeriodicSubquery const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<SetNestedProperty> {
  static void Note(SetNestedProperty const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<RemoveNestedProperty> {
  static void Note(RemoveNestedProperty const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<LoadParquet> {
  static void Note(LoadParquet const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<LoadJsonl> {
  static void Note(LoadJsonl const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<AggregateParallel> {
  static void Note(AggregateParallel const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<OrderByParallel> {
  static void Note(OrderByParallel const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallel> {
  static void Note(ScanParallel const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallelByLabel> {
  static void Note(ScanParallelByLabel const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallelByLabelProperties> {
  static void Note(ScanParallelByLabelProperties const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallelByEdgeType> {
  static void Note(ScanParallelByEdgeType const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallelByEdgeTypeProperty> {
  static void Note(ScanParallelByEdgeTypeProperty const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallelByEdge> {
  static void Note(ScanParallelByEdge const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallelByEdgeProperty> {
  static void Note(ScanParallelByEdgeProperty const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanParallelByVertexProperty> {
  static void Note(ScanParallelByVertexProperty const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanChunk> {
  static void Note(ScanChunk const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ScanChunkByEdge> {
  static void Note(ScanChunkByEdge const &, PropertyWrites &) {}
};

template <>
struct OperatorWrites<ParallelMerge> {
  static void Note(ParallelMerge const &, PropertyWrites &writes) { writes.NoteEverything(); }
};

template <>
struct OperatorWrites<PrefetchProperties> {
  static void Note(PrefetchProperties const &, PropertyWrites &) {}
};

namespace impl {

/// The operators the visitor is built from, taken from the visitor itself so that this cannot name
/// a different set.
template <class TVisitor>
struct VisitedOperators;

template <class... TOperators>
struct VisitedOperators<utils::CompositeVisitor<TOperators...>> {
  static_assert((requires(TOperators const &op, PropertyWrites &writes) {
                  OperatorWrites<TOperators>::Note(op, writes);
                } && ...),
                "every logical operator has to say what it may write: give it an OperatorWrites specialisation");

  using Noter = void (*)(LogicalOperator const &, PropertyWrites &);
  using Entry = std::pair<utils::TypeId, Noter>;

  /// Built once rather than at compile time, because an operator's type info is defined in its own
  /// translation unit.
  static auto const &Table() {
    static auto const table = std::array<Entry, sizeof...(TOperators)>{
        {{TOperators::kType.id, [](LogicalOperator const &op, PropertyWrites &writes) {
            OperatorWrites<TOperators>::Note(static_cast<TOperators const &>(op), writes);
          }}...}};
    return table;
  }
};

using OperatorTable = VisitedOperators<LogicalOperatorCompositeVisitor>;

}  // namespace impl

/// What `op` and everything it reads from may write.
///
/// Walks the operators one input at a time. An operator with no single input is one this does not
/// follow, so what lies beyond it answers for everything, which is why a branch nobody has taught
/// this to walk costs a saving rather than an answer.
inline void NotePropertyWrites(LogicalOperator const &op, PropertyWrites &writes) {
  auto const &table = impl::OperatorTable::Table();
  for (auto const *at = &op; at != nullptr;) {
    auto const found = std::ranges::find(table, at->GetTypeInfo().id, &impl::OperatorTable::Entry::first);
    if (found == table.end()) {
      // Not an operator the visitor knows, so nothing here can say what it writes.
      writes.NoteEverything();
      return;
    }
    found->second(*at, writes);

    if (!at->HasSingleInput()) {
      // Once is where a plan ends; anything else without a single input holds more than this walk
      // follows.
      if (at->GetTypeInfo().id != Once::kType.id) writes.NoteEverything();
      return;
    }
    at = at->input().get();
  }
}

/// What a whole plan may write.
[[nodiscard]] inline auto PropertyWritesOf(LogicalOperator const &root) -> PropertyWrites {
  auto writes = PropertyWrites{};
  NotePropertyWrites(root, writes);
  return writes;
}

}  // namespace memgraph::query::plan
