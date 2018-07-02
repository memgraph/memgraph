#pragma once

#include "utils/visitor.hpp"

namespace query {

// Forward declares for Tree visitors.
class Query;
class SingleQuery;
class CypherUnion;
class NamedExpression;
class Identifier;
class PropertyLookup;
class LabelsTest;
class Aggregation;
class Function;
class Reduce;
class Extract;
class All;
class Single;
class ParameterLookup;
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
class CreateIndex;
class ModifyUser;
class DropUser;

using TreeCompositeVisitor = ::utils::CompositeVisitor<
    Query, SingleQuery, CypherUnion, NamedExpression, OrOperator, XorOperator,
    AndOperator, NotOperator, AdditionOperator, SubtractionOperator,
    MultiplicationOperator, DivisionOperator, ModOperator, NotEqualOperator,
    EqualOperator, LessOperator, GreaterOperator, LessEqualOperator,
    GreaterEqualOperator, InListOperator, SubscriptOperator,
    ListSlicingOperator, IfOperator, UnaryPlusOperator, UnaryMinusOperator,
    IsNullOperator, ListLiteral, MapLiteral, PropertyLookup, LabelsTest,
    Aggregation, Function, Reduce, Extract, All, Single, Create, Match, Return,
    With, Pattern, NodeAtom, EdgeAtom, Delete, Where, SetProperty,
    SetProperties, SetLabels, RemoveProperty, RemoveLabels, Merge, Unwind>;

using TreeLeafVisitor =
    ::utils::LeafVisitor<Identifier, PrimitiveLiteral, ParameterLookup,
                         CreateIndex, ModifyUser, DropUser>;

class HierarchicalTreeVisitor : public TreeCompositeVisitor,
                                public TreeLeafVisitor {
 public:
  using TreeCompositeVisitor::PostVisit;
  using TreeCompositeVisitor::PreVisit;
  using TreeLeafVisitor::Visit;
  using typename TreeLeafVisitor::ReturnType;
};

template <typename TResult>
using TreeVisitor = ::utils::Visitor<
    TResult, Query, SingleQuery, CypherUnion, NamedExpression, OrOperator,
    XorOperator, AndOperator, NotOperator, AdditionOperator,
    SubtractionOperator, MultiplicationOperator, DivisionOperator, ModOperator,
    NotEqualOperator, EqualOperator, LessOperator, GreaterOperator,
    LessEqualOperator, GreaterEqualOperator, InListOperator, SubscriptOperator,
    ListSlicingOperator, IfOperator, UnaryPlusOperator, UnaryMinusOperator,
    IsNullOperator, ListLiteral, MapLiteral, PropertyLookup, LabelsTest,
    Aggregation, Function, Reduce, Extract, All, Single, ParameterLookup,
    Create, Match, Return, With, Pattern, NodeAtom, EdgeAtom, Delete, Where,
    SetProperty, SetProperties, SetLabels, RemoveProperty, RemoveLabels, Merge,
    Unwind, Identifier, PrimitiveLiteral, CreateIndex, ModifyUser, DropUser>;

}  // namespace query
