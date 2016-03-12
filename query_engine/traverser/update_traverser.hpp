#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"

class UpdateTraverser : public Traverser, public Code
{
    void visit(ast::Match& match) override
    {
    }

    void visit(ast::Set& set) override
    {
    }

    void visit(ast::Return& ret) override
    {
    }
};
