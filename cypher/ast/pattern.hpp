#ifndef MEMGRAPH_CYPHER_AST_PATTERN_HPP
#define MEMGRAPH_CYPHER_AST_PATTERN_HPP

#include "ast_node.hpp"

namespace ast
{

template <class Derived>
struct Direction : public AstNode<Derived> {};

struct DirectionLeft : public Direction<DirectionLeft> {};
struct DirectionRight : public Direction<DirectionLeft> {};
struct Unidirectional : public Direction<DirectionLeft> {};

struct Pattern : public AstNode<Pattern>
{
    Node* node;
    Pattern* next;
};

}

#endif
