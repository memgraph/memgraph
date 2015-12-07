#pragma once

#include "ast_node.hpp"

namespace ast
{

template <class T, class Derived>
struct List : public AstNode<Derived>
{
    List(T* value, Derived* next)
        : value(value), next(next) {}

    T* value;
    Derived* next;
};

}
