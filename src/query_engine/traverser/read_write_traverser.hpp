#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"

class ReadWriteTraverser : public Traverser, public Code
{
    void visit(ast::Match& match) override
    {
        code += LINE(fmt::format(code::todo, "MATCH"));
        Traverser::visit(match);
    }

    void visit(ast::Where& where) override
    {
        code += LINE(fmt::format(code::todo, "WHERE"));
        Traverser::visit(where);
    }

    void visit(ast::Create& create) override
    {
        code += LINE(fmt::format(code::todo, "CREATE"));
        Traverser::visit(create);
    }

    void visit(ast::Return& ast_return) override
    {
        code += LINE(fmt::format(code::todo, "RETURN"));
        code += LINE(code::return_empty_result);
    }
};
