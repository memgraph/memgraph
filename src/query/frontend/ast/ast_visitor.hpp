#pragma once

#include "utils/visitor.hpp"

namespace query {

// Forward declares for Tree visitors.
class Query;
class NamedExpression;
class Identifier;
class PropertyLookup;
class LabelsTest;
class EdgeTypeTest;
class Aggregation;
class Function;
class All;
class Create;
class Match;
class Return;
class With;
class Pattern;
class NodeAtom;
class EdgeAtom;
class BreadthFirstAtom;
class PrimitiveLiteral;
class ListLiteral;
class MapLiteral;
class OrOperator;
class XorOperator;
class AndOperator;
class FilterAndOperator;
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
class ListIndexingOperator;
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

using TreeCompositeVisitor = ::utils::CompositeVisitor<
    Query, NamedExpression, OrOperator, XorOperator, AndOperator,
    FilterAndOperator, NotOperator, AdditionOperator, SubtractionOperator,
    MultiplicationOperator, DivisionOperator, ModOperator, NotEqualOperator,
    EqualOperator, LessOperator, GreaterOperator, LessEqualOperator,
    GreaterEqualOperator, InListOperator, ListIndexingOperator,
    ListSlicingOperator, IfOperator, UnaryPlusOperator, UnaryMinusOperator,
    IsNullOperator, ListLiteral, MapLiteral, PropertyLookup, LabelsTest,
    EdgeTypeTest, Aggregation, Function, All, Create, Match, Return, With,
    Pattern, NodeAtom, EdgeAtom, BreadthFirstAtom, Delete, Where, SetProperty,
    SetProperties, SetLabels, RemoveProperty, RemoveLabels, Merge, Unwind>;

using TreeLeafVisitor =
    ::utils::LeafVisitor<Identifier, PrimitiveLiteral, CreateIndex>;

class HierarchicalTreeVisitor : public TreeCompositeVisitor,
                                public TreeLeafVisitor {
 public:
  using TreeCompositeVisitor::PreVisit;
  using TreeCompositeVisitor::PostVisit;
  using typename TreeLeafVisitor::ReturnType;
  using TreeLeafVisitor::Visit;
};

template <typename TResult>
using TreeVisitor = ::utils::Visitor<
    TResult, Query, NamedExpression, OrOperator, XorOperator, AndOperator,
    FilterAndOperator, NotOperator, AdditionOperator, SubtractionOperator,
    MultiplicationOperator, DivisionOperator, ModOperator, NotEqualOperator,
    EqualOperator, LessOperator, GreaterOperator, LessEqualOperator,
    GreaterEqualOperator, InListOperator, ListIndexingOperator,
    ListSlicingOperator, IfOperator, UnaryPlusOperator, UnaryMinusOperator,
    IsNullOperator, ListLiteral, MapLiteral, PropertyLookup, LabelsTest,
    EdgeTypeTest, Aggregation, Function, All, Create, Match, Return, With,
    Pattern, NodeAtom, EdgeAtom, BreadthFirstAtom, Delete, Where, SetProperty,
    SetProperties, SetLabels, RemoveProperty, RemoveLabels, Merge, Unwind,
    Identifier, PrimitiveLiteral, CreateIndex>;

}  // namespace query
