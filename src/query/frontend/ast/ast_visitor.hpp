#pragma once

#include "utils/visitor.hpp"

namespace query {

// Forward declares for Tree visitors.
class CypherQuery;
class SingleQuery;
class CypherUnion;
class NamedExpression;
class Identifier;
class PropertyLookup;
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
class OrOperator;
class XorOperator;
class AndOperator;
class NotOperator;
class AdditionOperator;
class SubtractionOperator;
class MultiplicationOperator;
class DivisionOperator;
class ModOperator;
class UnaryPlusOperator;
class UnaryMinusOperator;
class IsNullOperator;
class NotEqualOperator;
class EqualOperator;
class LessOperator;
class GreaterOperator;
class LessEqualOperator;
class GreaterEqualOperator;
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
class InfoQuery;
class ConstraintQuery;
class RegexMatch;
class DumpQuery;
class ReplicationQuery;
class LockPathQuery;
class LoadCsv;
class FreeMemoryQuery;
class TriggerQuery;
class IsolationLevelQuery;
class CreateSnapshotQuery;
class StreamQuery;
class SettingQuery;

using TreeCompositeVisitor = ::utils::CompositeVisitor<
    SingleQuery, CypherUnion, NamedExpression, OrOperator, XorOperator, AndOperator, NotOperator, AdditionOperator,
    SubtractionOperator, MultiplicationOperator, DivisionOperator, ModOperator, NotEqualOperator, EqualOperator,
    LessOperator, GreaterOperator, LessEqualOperator, GreaterEqualOperator, InListOperator, SubscriptOperator,
    ListSlicingOperator, IfOperator, UnaryPlusOperator, UnaryMinusOperator, IsNullOperator, ListLiteral, MapLiteral,
    PropertyLookup, LabelsTest, Aggregation, Function, Reduce, Coalesce, Extract, All, Single, Any, None, CallProcedure,
    Create, Match, Return, With, Pattern, NodeAtom, EdgeAtom, Delete, Where, SetProperty, SetProperties, SetLabels,
    RemoveProperty, RemoveLabels, Merge, Unwind, RegexMatch, LoadCsv>;

using TreeLeafVisitor = ::utils::LeafVisitor<Identifier, PrimitiveLiteral, ParameterLookup>;

class HierarchicalTreeVisitor : public TreeCompositeVisitor, public TreeLeafVisitor {
 public:
  using TreeCompositeVisitor::PostVisit;
  using TreeCompositeVisitor::PreVisit;
  using TreeLeafVisitor::Visit;
  using typename TreeLeafVisitor::ReturnType;
};

template <class TResult>
class ExpressionVisitor
    : public ::utils::Visitor<
          TResult, NamedExpression, OrOperator, XorOperator, AndOperator, NotOperator, AdditionOperator,
          SubtractionOperator, MultiplicationOperator, DivisionOperator, ModOperator, NotEqualOperator, EqualOperator,
          LessOperator, GreaterOperator, LessEqualOperator, GreaterEqualOperator, InListOperator, SubscriptOperator,
          ListSlicingOperator, IfOperator, UnaryPlusOperator, UnaryMinusOperator, IsNullOperator, ListLiteral,
          MapLiteral, PropertyLookup, LabelsTest, Aggregation, Function, Reduce, Coalesce, Extract, All, Single, Any,
          None, ParameterLookup, Identifier, PrimitiveLiteral, RegexMatch> {};

template <class TResult>
class QueryVisitor
    : public ::utils::Visitor<TResult, CypherQuery, ExplainQuery, ProfileQuery, IndexQuery, AuthQuery, InfoQuery,
                              ConstraintQuery, DumpQuery, ReplicationQuery, LockPathQuery, FreeMemoryQuery,
                              TriggerQuery, IsolationLevelQuery, CreateSnapshotQuery, StreamQuery, SettingQuery> {};

}  // namespace query
