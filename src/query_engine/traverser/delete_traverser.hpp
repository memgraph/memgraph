#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"

class DeleteTraverser : public Traverser, public Code
{
    void visit(ast::Match& match) override
    {
        code += LINE(fmt::format(code::todo, "MATCH"));
    }

    void visit(ast::Delete& delete_clause) override
    {
        code += LINE(fmt::format(code::todo, "DELETE"));
        code += LINE(code::return_empty_result);
    }
};
