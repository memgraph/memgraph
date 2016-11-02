#pragma once

#include "ast_node.hpp"

namespace ast
{

struct Expr : public AstVisitable
{
};

template <class Derived>
struct ValueExpr : public Crtp<Derived>, public Expr
{
    using uptr = std::unique_ptr<Derived>;

    virtual void accept(AstVisitor &visitor) { visitor.visit(this->derived()); }
};

template <class T, class Derived>
struct LeafExpr : public ValueExpr<Derived>
{
    LeafExpr(T value) : value(value) {}
    T value;
};

// T is argument type
template <class T, class Derived>
struct FunctionExpr : public ValueExpr<Derived>
{
    FunctionExpr(const std::string& name, T argument) : name(name), argument(argument) {}
    
    std::string name;
    T argument;
};

template <class Derived>
struct BinaryExpr : public ValueExpr<Derived>
{
    BinaryExpr(Expr *left, Expr *right) : left(left), right(right) {}

    Expr *left;
    Expr *right;
};

struct PatternExpr : public Expr
{
    using uptr = std::unique_ptr<PatternExpr>;

    PatternExpr(Pattern *pattern) : pattern(pattern) {}

    virtual void accept(AstVisitor &visitor) { visitor.visit(*this); }

    Pattern *pattern;
};

}
