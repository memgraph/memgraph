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
#include <optional>
#include <utility>

#include "query/plan/operator.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::query::plan {

/// What one operator does with a row, as far as reading several of a record's properties at once is
/// concerned. Three facts, each needed in more than one place and each quiet when wrong:
///
///   - `kEvaluatesAt`, the view its expressions read a record at. A read standing in for one of its
///     lookups has to be made at the same view, or it answers what the operator would never have
///     seen. Empty where an operator evaluates nothing against a record.
///   - `kKeepsSlotsFilledBelow`, whether a slot filled below it still holds that read above it. An
///     operator qualifies only if it writes no record, restores no row from the symbols it was told
///     to carry, and does not end the transaction the read was made in. The last is the one an
///     operator fails while looking harmless: committing part way through a query leaves a slot
///     holding what another transaction read.
///   - `ForEachExpression`, where its expressions are, for an operator a pass may serve.
///
/// Every operator answers the first two, and one saying nothing is a compile error rather than a
/// silent answer: the specialisations are folded over the list of operators the visitor is built
/// from, which an operator has to be in to be visited at all.
template <class TOperator>
struct OperatorEvaluation;

/// An operator evaluating nothing against a record, which a filled slot does not outlive.
struct EvaluatesNoRecord {
  static constexpr auto kEvaluatesAt = std::optional<storage::View>{};
  static constexpr bool kKeepsSlotsFilledBelow = false;
};

/// The same, except that it hands a row on with the frame as its input left it.
struct EvaluatesNoRecordAndKeepsSlots {
  static constexpr auto kEvaluatesAt = std::optional<storage::View>{};
  static constexpr bool kKeepsSlotsFilledBelow = true;
};

template <>
struct OperatorEvaluation<Once> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<CreateNode> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<CreateExpand> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAll> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByLabel> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByLabelProperties> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllById> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByEdge> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByEdgeType> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByEdgeTypeProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByEdgeProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByEdgeId> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByVertexProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByPointDistance> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanAllByPointWithinbbox> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Expand> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ExpandVariable> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ConstructNamedPath> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Delete> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<SetProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<SetProperties> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<SetLabels> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<RemoveProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<RemoveLabels> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<EdgeUniquenessFilter> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Accumulate> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Skip> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Limit> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<OrderBy> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Merge> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Optional> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Unwind> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<CardinalityScale> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Distinct> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Union> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Cartesian> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<CallProcedure> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<LoadCsv> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Foreach> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<EmptyResult> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<EvaluatePatternFilter> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Apply> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<IndexedJoin> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<HashJoin> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<RollUpApply> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<Conditional> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<PeriodicCommit> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<PeriodicSubquery> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<SetNestedProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<RemoveNestedProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<LoadParquet> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<LoadJsonl> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<AggregateParallel> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<OrderByParallel> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallel> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallelByLabel> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallelByLabelProperties> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallelByEdgeType> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallelByEdgeTypeProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallelByEdge> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallelByEdgeProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanParallelByVertexProperty> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanChunk> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ScanChunkByEdge> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<ParallelMerge> : EvaluatesNoRecord {};

template <>
struct OperatorEvaluation<PrefetchProperties> : EvaluatesNoRecord {};

/// A projection returns the values its expressions read, at the view a row's own writes left.
template <>
struct OperatorEvaluation<Produce> {
  static constexpr auto kEvaluatesAt = std::optional<storage::View>{storage::View::NEW};
  static constexpr bool kKeepsSlotsFilledBelow = false;

  template <class TEach>
  static void ForEachExpression(Produce &op, TEach &&each) {
    for (auto *&named : op.named_expressions_) each(named);
  }
};

/// A predicate tests a row against the record as the row was produced, which is the old view, and
/// hands on the rows it keeps with the frame as it found it.
template <>
struct OperatorEvaluation<Filter> {
  static constexpr auto kEvaluatesAt = std::optional<storage::View>{storage::View::OLD};
  static constexpr bool kKeepsSlotsFilledBelow = true;

  template <class TEach>
  static void ForEachExpression(Filter &op, TEach &&each) {
    if (op.expression_) each(op.expression_);
  }
};

/// An aggregation consumes its values at the view a row's own writes left. A secondary argument,
/// which a map collection and the graph projections take, is left out however many lookups it
/// holds: it is reached only once the row's first value is accepted, so a Null value, or a repeat a
/// distinct aggregation ignores, never evaluates it.
template <>
struct OperatorEvaluation<Aggregate> {
  static constexpr auto kEvaluatesAt = std::optional<storage::View>{storage::View::NEW};
  static constexpr bool kKeepsSlotsFilledBelow = false;

  template <class TEach>
  static void ForEachExpression(Aggregate &op, TEach &&each) {
    for (auto &aggregation : op.aggregations_) {
      if (aggregation.arg1) each(aggregation.arg1);
    }
    for (auto *&group_by : op.group_by_) each(group_by);
  }
};

/// The view `TOperator`'s expressions read a record at. Its cursor and anything standing in for one
/// of its lookups both take it from here, so the two cannot come to disagree.
template <class TOperator>
inline constexpr auto kEvaluatedAt = OperatorEvaluation<TOperator>::kEvaluatesAt.value();

namespace impl {

template <class TVisitor>
struct EvaluationOfOperators;

template <class... TOperators>
struct EvaluationOfOperators<utils::CompositeVisitor<TOperators...>> {
  static_assert(((requires {
                   OperatorEvaluation<TOperators>::kEvaluatesAt;
                   OperatorEvaluation<TOperators>::kKeepsSlotsFilledBelow;
                 }) && ...),
                "every logical operator has to say which view it evaluates a record at and whether a slot filled "
                "below it survives: give it an OperatorEvaluation specialisation");

  using Entry = std::pair<utils::TypeId, bool>;

  /// Built once rather than at compile time, because an operator's type info is defined in its own
  /// translation unit.
  static auto const &KeepsSlotsTable() {
    static auto const table = std::array<Entry, sizeof...(TOperators)>{
        {{TOperators::kType.id, OperatorEvaluation<TOperators>::kKeepsSlotsFilledBelow}...}};
    return table;
  }
};

using OperatorEvaluationTable = EvaluationOfOperators<LogicalOperatorCompositeVisitor>;

}  // namespace impl

/// Whether a slot a pass below `op` filled still holds that read above it. An operator this does
/// not know answers no, which costs a saving and never an answer.
[[nodiscard]] inline auto KeepsSlotsFilledBelow(LogicalOperator const &op) -> bool {
  auto const &table = impl::OperatorEvaluationTable::KeepsSlotsTable();
  auto const found = std::ranges::find(table, op.GetTypeInfo().id, &impl::OperatorEvaluationTable::Entry::first);
  return found != table.end() && found->second;
}

}  // namespace memgraph::query::plan
