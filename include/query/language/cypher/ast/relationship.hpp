#pragma once

#include "identifier.hpp"
#include "list.hpp"

namespace ast
{

struct RelationshipTypeList : public List<Identifier, RelationshipTypeList>
{
    using List::List;
};

struct RelationshipSpecs : public AstNode<RelationshipSpecs>
{
    RelationshipSpecs(Identifier *idn, RelationshipTypeList *types,
                      PropertyList *props)
        : idn(idn), types(types), props(props)
    {
    }

    Identifier *idn;
    RelationshipTypeList *types;
    PropertyList *props;

    bool has_identifier() const { return idn != nullptr; }

    std::string name() const { return idn->name; }
};

struct Relationship : public AstNode<Relationship>
{
    enum Direction
    {
        Left,
        Right,
        Both
    };

    Relationship(RelationshipSpecs *specs, Direction direction)
        : specs(specs), direction(direction)
    {
    }

    RelationshipSpecs *specs;
    Direction direction;

    bool has_relationship_specs() const { return specs != nullptr; }
    bool has_name() const
    {
        return has_relationship_specs() && specs->has_identifier();
    }

    std::string name() const { return specs->name(); }
};
}
