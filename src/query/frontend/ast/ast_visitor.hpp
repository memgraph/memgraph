// Copyright 2025 Memgraph Ltd.
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
#include "utils/visitor.hpp"

namespace memgraph::query {

// Forward declares for Tree visitors.
class CypherQuery;
class SingleQuery;
class CypherUnion;
class NamedExpression;
class Identifier;
class PropertyLookup;
class AllPropertiesLookup;
class LabelsTest;
class Aggregation;
class Function;
class Reduce;
class Coalesce;
class Extract;
class All;
class Single;
class Any;
class None;
class ListComprehension;
class ParameterLookup;
class CallProcedure;
class Create;
class Match;
class Return;
class With;
class Pattern;
class NodeAtom;
class EdgeAtom;
class PrimitiveLiteral;
class ListLiteral;
class MapLiteral;
class MapProjectionLiteral;
class OrOperator;
class XorOperator;
class AndOperator;
class NotOperator;
class AdditionOperator;
class SubtractionOperator;
class MultiplicationOperator;
class DivisionOperator;
class ModOperator;
class ExponentiationOperator;
class UnaryPlusOperator;
class UnaryMinusOperator;
class IsNullOperator;
class NotEqualOperator;
class EqualOperator;
class LessOperator;
class GreaterOperator;
class LessEqualOperator;
class GreaterEqualOperator;
class RangeOperator;
class InListOperator;
class SubscriptOperator;
class ListSlicingOperator;
class IfOperator;
class Delete;
class Where;
class SetProperty;
class SetProperties;
class SetLabels;
class RemoveProperty;
class RemoveLabels;
class Merge;
class Unwind;
class AuthQuery;
class ExplainQuery;
class ProfileQuery;
class IndexQuery;
class EdgeIndexQuery;
class PointIndexQuery;
class TextIndexQuery;
class CreateTextEdgeIndexQuery;
class VectorIndexQuery;
class CreateVectorEdgeIndexQuery;
class DatabaseInfoQuery;
class SystemInfoQuery;
class ConstraintQuery;
class RegexMatch;
class DumpQuery;
class ReplicationQuery;
class ReplicationInfoQuery;
class LockPathQuery;
class LoadCsv;
class FreeMemoryQuery;
class TriggerQuery;
class IsolationLevelQuery;
class StorageModeQuery;
class CreateSnapshotQuery;
class RecoverSnapshotQuery;
class ShowSnapshotsQuery;
class ShowNextSnapshotQuery;
class StreamQuery;
class SettingQuery;
class VersionQuery;
class Foreach;
class ShowConfigQuery;
class CallSubquery;
class AnalyzeGraphQuery;
class TransactionQueueQuery;
class Exists;
class MultiDatabaseQuery;
class UseDatabaseQuery;
class ShowDatabaseQuery;
class ShowDatabasesQuery;
class EdgeImportModeQuery;
class PatternComprehension;
class CoordinatorQuery;
class DropGraphQuery;
class CreateEnumQuery;
class ShowEnumsQuery;
class EnumValueAccess;
class AlterEnumAddValueQuery;
class AlterEnumUpdateValueQuery;
class AlterEnumRemoveValueQuery;
class DropEnumQuery;
class ShowSchemaInfoQuery;
class TtlQuery;
class SessionTraceQuery;

using TreeCompositeVisitor = utils::CompositeVisitor<
    SingleQuery, CypherUnion, NamedExpression, OrOperator, XorOperator, AndOperator, NotOperator, AdditionOperator,
    SubtractionOperator, MultiplicationOperator, DivisionOperator, ModOperator, ExponentiationOperator,
    NotEqualOperator, EqualOperator, LessOperator, GreaterOperator, LessEqualOperator, GreaterEqualOperator,
    RangeOperator, InListOperator, SubscriptOperator, ListSlicingOperator, IfOperator, UnaryPlusOperator,
    UnaryMinusOperator, IsNullOperator, ListLiteral, MapLiteral, MapProjectionLiteral, PropertyLookup,
    AllPropertiesLookup, LabelsTest, Aggregation, Function, Reduce, Coalesce, Extract, All, Single, Any, None,
    ListComprehension, CallProcedure, Create, Match, Return, With, Pattern, NodeAtom, EdgeAtom, Delete, Where,
    SetProperty, SetProperties, SetLabels, RemoveProperty, RemoveLabels, Merge, Unwind, RegexMatch, LoadCsv, Foreach,
    Exists, CallSubquery, CypherQuery, PatternComprehension>;

using TreeLeafVisitor = utils::LeafVisitor<Identifier, PrimitiveLiteral, ParameterLookup, EnumValueAccess>;

class HierarchicalTreeVisitor : public TreeCompositeVisitor, public TreeLeafVisitor {
 public:
  using TreeCompositeVisitor::PostVisit;
  using TreeCompositeVisitor::PreVisit;
  using TreeLeafVisitor::Visit;
  using typename TreeLeafVisitor::ReturnType;
};

template <class TResult>
class ExpressionVisitor
    : public utils::Visitor<TResult, NamedExpression, OrOperator, XorOperator, AndOperator, NotOperator,
                            AdditionOperator, SubtractionOperator, MultiplicationOperator, DivisionOperator,
                            ModOperator, ExponentiationOperator, NotEqualOperator, EqualOperator, LessOperator,
                            GreaterOperator, LessEqualOperator, GreaterEqualOperator, RangeOperator, InListOperator,
                            SubscriptOperator, ListSlicingOperator, IfOperator, UnaryPlusOperator, UnaryMinusOperator,
                            IsNullOperator, ListLiteral, MapLiteral, MapProjectionLiteral, PropertyLookup,
                            AllPropertiesLookup, LabelsTest, Aggregation, Function, Reduce, Coalesce, Extract, All,
                            Single, Any, None, ListComprehension, ParameterLookup, Identifier, PrimitiveLiteral,
                            RegexMatch, Exists, PatternComprehension, EnumValueAccess> {};

template <class TResult>
class QueryVisitor
    : public utils::Visitor<
          TResult, CypherQuery, ExplainQuery, ProfileQuery, IndexQuery, EdgeIndexQuery, PointIndexQuery, TextIndexQuery,
          CreateTextEdgeIndexQuery, VectorIndexQuery, CreateVectorEdgeIndexQuery, AuthQuery, DatabaseInfoQuery,
          SystemInfoQuery, ConstraintQuery, DumpQuery, ReplicationQuery, ReplicationInfoQuery, LockPathQuery,
          FreeMemoryQuery, TriggerQuery, IsolationLevelQuery, CreateSnapshotQuery, RecoverSnapshotQuery,
          ShowSnapshotsQuery, ShowNextSnapshotQuery, StreamQuery, SettingQuery, VersionQuery, ShowConfigQuery,
          TransactionQueueQuery, StorageModeQuery, AnalyzeGraphQuery, MultiDatabaseQuery, UseDatabaseQuery,
          ShowDatabaseQuery, ShowDatabasesQuery, EdgeImportModeQuery, CoordinatorQuery, DropGraphQuery, CreateEnumQuery,
          ShowEnumsQuery, AlterEnumAddValueQuery, AlterEnumUpdateValueQuery, AlterEnumRemoveValueQuery, DropEnumQuery,
          ShowSchemaInfoQuery, TtlQuery, SessionTraceQuery> {};

}  // namespace memgraph::query
