#ifndef MEMGRAPH_CYPHER_AST_RELATIONSHIP_HPP
#define MEMGRAPH_CYPHER_AST_RELATIONSHIP_HPP

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
    Relationship()
};

}

#endif
