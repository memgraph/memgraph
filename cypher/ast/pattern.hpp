#pragma once

#include "ast_node.hpp"
#include "relationship.hpp"
#include "node.hpp"

namespace ast
{

struct Pattern : public AstNode<Pattern>
{
    Pattern(Node* node, Relationship* relationship, Pattern* next)
        : node(node), relationship(relationship), next(next) {}

    Node* node;
    Relationship* relationship;
    Pattern* next;
};

}
