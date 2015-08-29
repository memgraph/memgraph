#ifndef MEMGRAPH_CYPHER_AST_EXPR_HPP
#define MEMGRAPH_CYPHER_AST_EXPR_HPP

#include "ast_node.hpp"

namespace ast
{

struct Expr : public AstVisitable {};

template <class Derived>
struct VisitableExpr : public Crtp<Derived>, public Expr
{
    using uptr = std::unique_ptr<Derived>;

    virtual void accept(AstVisitor& visitor)
    {
        visitor.visit(this->derived());
    }
};

template <class T, class Derived>
struct LeafExpr : public VisitableExpr<Derived>
{
    LeafExpr(T value) : value(value) {}
    T value;
};

template <class Derived>
struct BinaryExpr : public VisitableExpr<Derived>
{
    BinaryExpr(Expr* left, Expr* right) : left(left), right(right) {}

    Expr* left;
    Expr* right;
};

}

#endif
