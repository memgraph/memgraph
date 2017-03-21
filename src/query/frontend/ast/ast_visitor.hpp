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

using TreeVisitorBase = ::utils::Visitor<Query, NamedExpression, Identifier,
                                         Literal, PropertyLookup, Create, Match,
                                         Return, Pattern, NodeAtom, EdgeAtom>;
}
