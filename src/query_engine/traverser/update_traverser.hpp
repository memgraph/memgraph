#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"

class UpdateTraverser : public Traverser, public Code
{
    void visit(ast::Match& match) override
    {
        code += LINE(fmt::format(code::todo, "MATCH"));
    }

    void visit(ast::Set& set) override
    {
        code += LINE(fmt::format(code::todo, "SET"));
    }

    void visit(ast::Return& ret) override
    {
        code += LINE(fmt::format(code::todo, "RETURN"));
        code += LINE(code::return_empty_result);
    }
};
