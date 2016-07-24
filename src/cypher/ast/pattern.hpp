#pragma once

#include "ast_node.hpp"
#include "node.hpp"
#include "relationship.hpp"

namespace ast
{
struct Pattern : public AstNode<Pattern>
{
    Pattern(Node *node, Relationship *relationship, Pattern *next)
        : node(node), relationship(relationship), next(next)
    {
    }

    Node *node;
    Relationship *relationship;
    Pattern *next;

    bool has_node() const { return node != nullptr; }
    bool has_relationship() const { return relationship != nullptr; }
    bool has_next() const { return next != nullptr; }
};

struct PatternList : public List<Pattern, PatternList>
{
    using List::List;
};
}
