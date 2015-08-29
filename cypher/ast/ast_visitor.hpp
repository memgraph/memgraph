#ifndef MEMGRAPH_CYPHER_AST_AST_VISITOR_HPP
#define MEMGRAPH_CYPHER_AST_AST_VISITOR_HPP

#include "utils/visitor/visitor.hpp"

namespace ast
{

struct Identifier;

// properties
struct Property;
struct PropertyList;
struct Accessor;

// values
struct Boolean;
struct Float;
struct Integer;
struct String;

// operators
struct And;
struct Or;
struct Lt;
struct Gt;
struct Ge;
struct Le;
struct Eq;
struct Ne;
struct Plus;
struct Minus;
struct Star;
struct Slash;
struct Rem;

struct RelationshipList;
struct Relationship;

struct Node;
struct LabelList;

struct ReturnList;

struct AstVisitor : Visitor<Accessor, Boolean, Float, Identifier, Integer,
    String, Property, And, Or, Lt, Gt, Ge, Le, Eq, Ne, Plus, Minus, Star,
    Slash, Rem, PropertyList, RelationshipList, Relationship, Node,
    LabelList, ReturnList> {};

}

#endif
