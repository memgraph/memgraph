#pragma once

#include "utils/visitor/visitor.hpp"

namespace ast
{

struct Identifier;
struct Alias;

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

struct RelationshipSpecs;
struct RelationshipList;
struct Relationship;

struct Node;
struct LabelList;
struct Pattern;
struct PatternExpr;

struct Return;
struct ReturnList;
struct Distinct;

struct Create;
struct Match;
struct Where;
struct Set;
struct Delete;

struct Start;
struct WriteQuery;
struct ReadQuery;
struct UpdateQuery;
struct DeleteQuery;

struct SetKey;
struct SetValue;
struct SetElement;
struct SetList;

struct AstVisitor : public Visitor<Accessor, Boolean, Float, Identifier, Alias,
    Integer, String, Property, And, Or, Lt, Gt, Ge, Le, Eq, Ne, Plus, Minus,
    Star, Slash, Rem, PropertyList, RelationshipList, Relationship, Node,
    RelationshipSpecs, LabelList, ReturnList, Pattern, PatternExpr, Match, ReadQuery,
    Start, Where, WriteQuery, Create, Return, Distinct, Delete,
    DeleteQuery, UpdateQuery, Set, SetKey, SetValue, SetElement, SetList> {};

}
