#pragma once

#include "utils/visitor/visitor.hpp"

namespace query {

// Forward declares for TreeVisitorBase
class Query;
class NamedExpression;
class Identifier;
class PropertyLookup;
class Create;
class Match;
class Return;
class Pattern;
class NodeAtom;
class EdgeAtom;
class Literal;
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
class NotEqualOperator;
class EqualOperator;
class LessOperator;
class GreaterOperator;
class LessEqualOperator;
class GreaterEqualOperator;
class Delete;
class Where;
class SetProperty;
class SetProperties;
class SetLabels;

using TreeVisitorBase = ::utils::Visitor<
    Query, NamedExpression, OrOperator, XorOperator, AndOperator, NotOperator,
    AdditionOperator, SubtractionOperator, MultiplicationOperator,
    DivisionOperator, ModOperator, NotEqualOperator, EqualOperator,
    LessOperator, GreaterOperator, LessEqualOperator, GreaterEqualOperator,
    UnaryPlusOperator, UnaryMinusOperator, Identifier, Literal, PropertyLookup,
    Create, Match, Return, Pattern, NodeAtom, EdgeAtom, Delete, Where,
    SetProperty, SetProperties, SetLabels>;
}
