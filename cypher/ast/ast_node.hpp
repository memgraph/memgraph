#ifndef MEMGRAPH_CYPHER_AST_AST_NODE_HPP
#define MEMGRAPH_CYPHER_AST_AST_NODE_HPP

#include <memory>

#include "utils/visitor/visitable.hpp"
#include "utils/crtp.hpp"
#include "ast_visitor.hpp"

namespace ast
{

struct AstVisitor;

struct AstVisitable : public Visitable<AstVisitor>
{
    using uptr = std::unique_ptr<AstVisitable>;
};

template <class Derived>
struct AstNode : public Crtp<Derived>, public AstVisitable
{
    using uptr = std::unique_ptr<Derived>;

    virtual void accept(AstVisitor& visitor)
    {
        visitor.visit(this->derived());
    }
};

}

#endif
