#pragma once

#include "utils/visitor/visitor.hpp"

namespace query {

// Forward declares for TreeVisitorBase
class Query;
class NamedExpression;
class Identifier;
class PropertyLookup;
class Match;
class Return;
class Pattern;
class NodeAtom;
class EdgeAtom;

using TreeVisitorBase =
  ::utils::Visitor<Query, NamedExpression, Identifier, PropertyLookup, Match,
                   Return, Pattern, NodeAtom, EdgeAtom>;

}
