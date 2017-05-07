#pragma once

#include "utils/visitor/visitor.hpp"

namespace query {

// Forward declares for TreeVisitorBase
class Query;
class NamedExpression;
class Identifier;
class PropertyLookup;
class LabelsTest;
class EdgeTypeTest;
class Aggregation;
class Function;
class Create;
class Match;
class Return;
class With;
class Pattern;
class NodeAtom;
class EdgeAtom;
class PrimitiveLiteral;
class ListLiteral;
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
class ListIndexingOperator;
class ListSlicingOperator;
class Delete;
class Where;
class SetProperty;
class SetProperties;
class SetLabels;
class RemoveProperty;
class RemoveLabels;
class Merge;
class Unwind;

using TreeVisitorBase = ::utils::Visitor<
    Query, NamedExpression, OrOperator, XorOperator, AndOperator, NotOperator,
    AdditionOperator, SubtractionOperator, MultiplicationOperator,
    DivisionOperator, ModOperator, NotEqualOperator, EqualOperator,
    LessOperator, GreaterOperator, LessEqualOperator, GreaterEqualOperator,
    InListOperator, ListIndexingOperator, ListSlicingOperator,
    UnaryPlusOperator, UnaryMinusOperator, IsNullOperator, Identifier,
    PrimitiveLiteral, ListLiteral, PropertyLookup, LabelsTest, EdgeTypeTest,
    Aggregation, Function, Create, Match, Return, With, Pattern, NodeAtom,
    EdgeAtom, Delete, Where, SetProperty, SetProperties, SetLabels,
    RemoveProperty, RemoveLabels, Merge, Unwind>;

}  // namespace query
