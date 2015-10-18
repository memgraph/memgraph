#ifndef MEMGRAPH_CYPHER_AST_AST_ACCESSOR_HPP
#define MEMGRAPH_CYPHER_AST_AST_ACCESSOR_HPP

#include <memory>
#include <string>

#include "expr.hpp"
#include "identifier.hpp"

namespace ast
{

struct Accessor : public VisitableExpr<Accessor>
{
    Accessor(Identifier* entity, Identifier* prop)
        : entity(entity), prop(prop) {}

    Identifier* entity;
    Identifier* prop;
};

}

#endif
