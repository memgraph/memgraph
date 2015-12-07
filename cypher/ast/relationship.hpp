#pragma once

#include "list.hpp"
#include "identifier.hpp"

namespace ast
{

struct RelationshipList : public List<Identifier, RelationshipList>
{
    using List::List;
};

struct RelationshipSpecs : public AstNode<RelationshipSpecs>
{
    RelationshipSpecs(Identifier* idn, RelationshipList* types, PropertyList* props)
        : idn(idn), types(types), props(props) {}

    Identifier* idn;
    RelationshipList* types;
    PropertyList* props;
};

struct Relationship : public AstNode<Relationship>
{
    enum Direction { Left, Right, Both };

    Relationship(RelationshipSpecs* specs, Direction direction)
        : specs(specs), direction(direction) {}

    RelationshipSpecs* specs;
    Direction direction;
};

}
