#pragma once

#include <string>

#include "cypher/visitor/traverser.hpp"
#include "cypher/ast/queries.hpp"
#include "write_traverser.hpp"
#include "read_traverser.hpp"

class CodeTraverser : public Traverser
{
public:

    std::string code;

    void reset()
    {
        code = "";
    }

    void visit(ast::WriteQuery& write_query) override
    {
        auto write_traverser = WriteTraverser();
        write_query.accept(write_traverser);
        code = write_traverser.code;
    }

    void visit(ast::ReadQuery& read_query) override
    {
        auto read_traverser = ReadTraverser();
        read_query.accept(read_traverser);
        code = read_traverser.code;
    }
};
