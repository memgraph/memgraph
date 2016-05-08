#pragma once

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

    void accept(AstVisitor& visitor) override
    {
        visitor.visit(this->derived());
    }
};

}
