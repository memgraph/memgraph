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

#include <cstdint>
#include <ostream>
#include <string>

#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

/// Whether an action reaches the graph, and in which direction.
///
/// NONE describes an action that neither reads the graph nor writes it, R one that reads, W one
/// that writes, and RW one that does both.
enum class RWType : uint8_t { NONE, R, W, RW };

namespace detail {

/// What a plan's operators have said so far, and the one rule for combining a further answer with
/// it. Held apart from the operators' own answers so that a specialisation below can reach it.
struct RWTypeState : public virtual HierarchicalLogicalOperatorVisitor {
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  using RWType = plan::RWType;

  RWType type{RWType::NONE};

  void UpdateType(RWType op_type);
};

}  // namespace detail

/// Whether one operator's own action reads the graph, writes it, both, or neither, and whether the
/// walk carries on into what it reads from. An operator holding branches walks each itself.
///
/// Every operator says so below, and one saying nothing is a compile error rather than an action
/// nobody counted: a plan built from operators that name no type is reported as touching no graph
/// at all.
template <class TOperator>
struct OperatorRWType;

namespace detail {

/// An operator whose own action is one type, and whose single input the walk carries on into.
template <RWType kType, bool kContinueVisiting = true>
struct Contributes {
  static bool Visit(LogicalOperator &, RWTypeState &state) {
    state.UpdateType(kType);
    return kContinueVisiting;
  }
};

/// Neither reads the graph nor writes it. Covers an operator working on rows an operator below
/// produced, and one reading a source that is not the graph.
using TouchesNoGraph = Contributes<RWType::NONE>;
using Reads = Contributes<RWType::R>;
using Writes = Contributes<RWType::W>;
/// Both reads and writes, which is the answer for a whole plan whatever lies below, so the walk
/// stops here.
using ReadsAndWrites = Contributes<RWType::RW, false>;

/// Walks each of an operator's branches and stops, which is how an operator with more than one
/// input answers for all of them.
template <auto... kBranches>
struct VisitsBranches {
  static bool Visit(auto &op, RWTypeState &state) {
    (((op.*kBranches)->Accept(state)), ...);
    return false;
  }
};

}  // namespace detail

template <>
struct OperatorRWType<Once> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<CreateNode> : detail::Writes {};

template <>
struct OperatorRWType<CreateExpand> : detail::ReadsAndWrites {};

template <>
struct OperatorRWType<ScanAll> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByLabel> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByLabelProperties> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllById> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByEdge> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByEdgeType> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByEdgeTypeProperty> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByEdgeProperty> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByEdgeId> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByVertexProperty> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByPointDistance> : detail::Reads {};

template <>
struct OperatorRWType<ScanAllByPointWithinbbox> : detail::Reads {};

template <>
struct OperatorRWType<Expand> : detail::Reads {};

template <>
struct OperatorRWType<ExpandVariable> : detail::Reads {};

template <>
struct OperatorRWType<ConstructNamedPath> : detail::Reads {};

template <>
struct OperatorRWType<Filter> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Produce> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Delete> : detail::Writes {};

template <>
struct OperatorRWType<SetProperty> : detail::Writes {};

template <>
struct OperatorRWType<SetProperties> : detail::Writes {};

template <>
struct OperatorRWType<SetLabels> : detail::Writes {};

template <>
struct OperatorRWType<RemoveProperty> : detail::Writes {};

template <>
struct OperatorRWType<RemoveLabels> : detail::Writes {};

template <>
struct OperatorRWType<EdgeUniquenessFilter> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Accumulate> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Aggregate> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Skip> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Limit> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<OrderBy> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Merge> : detail::ReadsAndWrites {};

template <>
struct OperatorRWType<Optional> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Unwind> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<CardinalityScale> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Distinct> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Union> : detail::VisitsBranches<&Union::left_op_, &Union::right_op_> {};

template <>
struct OperatorRWType<Cartesian> : detail::VisitsBranches<&Cartesian::left_op_, &Cartesian::right_op_> {};

template <>
struct OperatorRWType<CallProcedure> {
  static bool Visit(CallProcedure &op, detail::RWTypeState &state) {
    if (op.graph_access_ == GraphAccess::Write) {
      state.UpdateType(RWType::RW);
      return false;
    }
    state.UpdateType(RWType::R);
    return true;
  }
};

template <>
struct OperatorRWType<LoadCsv> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Foreach> : detail::ReadsAndWrites {};

template <>
struct OperatorRWType<EmptyResult> : detail::TouchesNoGraph {};

/// The pattern it folds to a value is a plan of its own, which the operator holding this one walks.
template <>
struct OperatorRWType<EvaluatePatternFilter> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<Apply> : detail::VisitsBranches<&Apply::input_, &Apply::subquery_> {};

template <>
struct OperatorRWType<IndexedJoin> : detail::VisitsBranches<&IndexedJoin::main_branch_, &IndexedJoin::sub_branch_> {};

template <>
struct OperatorRWType<HashJoin> : detail::VisitsBranches<&HashJoin::left_op_, &HashJoin::right_op_> {};

template <>
struct OperatorRWType<RollUpApply>
    : detail::VisitsBranches<&RollUpApply::input_, &RollUpApply::list_collection_branch_> {};

template <>
struct OperatorRWType<Conditional> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<PeriodicCommit> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<PeriodicSubquery>
    : detail::VisitsBranches<&PeriodicSubquery::input_, &PeriodicSubquery::subquery_> {};

template <>
struct OperatorRWType<SetNestedProperty> : detail::Writes {};

template <>
struct OperatorRWType<RemoveNestedProperty> : detail::Writes {};

template <>
struct OperatorRWType<LoadParquet> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<LoadJsonl> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<AggregateParallel> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<OrderByParallel> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<ScanParallel> : detail::Reads {};

template <>
struct OperatorRWType<ScanParallelByLabel> : detail::Reads {};

template <>
struct OperatorRWType<ScanParallelByLabelProperties> : detail::Reads {};

template <>
struct OperatorRWType<ScanParallelByEdge> : detail::Reads {};

template <>
struct OperatorRWType<ScanParallelByEdgeType> : detail::Reads {};

template <>
struct OperatorRWType<ScanParallelByEdgeTypeProperty> : detail::Reads {};

template <>
struct OperatorRWType<ScanParallelByEdgeProperty> : detail::Reads {};

template <>
struct OperatorRWType<ScanParallelByVertexProperty> : detail::Reads {};

/// A chunk of a scan an operator above hands out, which the scan itself has already answered for.
template <>
struct OperatorRWType<ScanChunk> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<ScanChunkByEdge> : detail::TouchesNoGraph {};

template <>
struct OperatorRWType<ParallelMerge> : detail::TouchesNoGraph {};

/// Reading properties of records an operator below produced is not a read of the graph in its own
/// right: whatever produced those records has said so itself.
template <>
struct OperatorRWType<CacheProperties> : detail::TouchesNoGraph {};

namespace detail {

/// Declares one `PreVisit` per operator, each asking that operator what it does. Recursive so that
/// the set it declares is the set it is given, leaving no operator to a default that would count
/// its action as nothing.
template <class TBase, class... TOperators>
struct DeclaresRWTypes;

template <class TBase>
struct DeclaresRWTypes<TBase> : TBase {};

template <class TBase, class THead, class... TTail>
struct DeclaresRWTypes<TBase, THead, TTail...> : DeclaresRWTypes<TBase, TTail...> {
  using DeclaresRWTypes<TBase, TTail...>::PreVisit;

  bool PreVisit(THead &op) override { return OperatorRWType<THead>::Visit(op, *this); }
};

/// The operators the visitor is built from, taken from the visitor itself so that this cannot name
/// a different set.
template <class TBase, class TVisitor>
struct DeclaresEveryVisitedOperator;

template <class TBase, class... TOperators>
struct DeclaresEveryVisitedOperator<TBase, utils::CompositeVisitor<TOperators...>> {
  static_assert((requires(TOperators &op, RWTypeState &state) { OperatorRWType<TOperators>::Visit(op, state); } && ...),
                "every logical operator has to say whether it reads or writes the graph: give it an OperatorRWType "
                "specialisation");

  using Type = DeclaresRWTypes<TBase, TOperators...>;
};

}  // namespace detail

struct ReadWriteTypeChecker
    : public detail::DeclaresEveryVisitedOperator<detail::RWTypeState, LogicalOperatorCompositeVisitor>::Type {
  ReadWriteTypeChecker() = default;

  ReadWriteTypeChecker(const ReadWriteTypeChecker &) = delete;
  ReadWriteTypeChecker(ReadWriteTypeChecker &&) = delete;

  ReadWriteTypeChecker &operator=(const ReadWriteTypeChecker &) = delete;
  ReadWriteTypeChecker &operator=(ReadWriteTypeChecker &&) = delete;

  void InferRWType(LogicalOperator &root);
  static std::string TypeToString(const RWType type);

  bool Visit(Once &) override;
};

/// Reaching storage is a different question from reading the graph: a call to a procedure that declared
/// it touches no graph stays a read, which is what clients and the read counters are told, but it reaches
/// no storage. Derives from ReadWriteTypeChecker so every other operator keeps one classification, and an
/// operator added later is covered by the type it declares there.
struct StorageAccessChecker : ReadWriteTypeChecker {
  using ReadWriteTypeChecker::PreVisit;

  bool PreVisit(CallProcedure &op) override;
};

/// True if executing `plan` needs a storage transaction.
bool PlanRequiresStorageAccess(const LogicalOperator &plan);

inline std::ostream &operator<<(std::ostream &os, RWType type) {
  switch (type) {
    using enum RWType;
    case NONE:
      return os << "NONE";
    case R:
      return os << "READ";
    case W:
      return os << "WRITE";
    case RW:
      return os << "READ-WRITE";
  }
  return os;
}

}  // namespace memgraph::query::plan
