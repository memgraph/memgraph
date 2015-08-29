#ifndef MEMGRAPH_CYPHER_AST_NODE_HPP
#define MEMGRAPH_CYPHER_AST_NODE_HPP

#include "list.hpp"
#include "identifier.hpp"

namespace ast
{

struct LabelList : public List<Identifier, LabelList>
{
    using List::List;
};

struct Node : public AstNode<Node>
{
    Node(Identifier* idn, LabelList* labels, PropertyList* props)
        : idn(idn), labels(labels), props(props) {}

    Identifier* idn;
    LabelList* labels;
    PropertyList* props;
};

}

#endif
