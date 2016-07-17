#pragma once

#include <string>

#include "cypher/visitor/traverser.hpp"
#include "cypher/ast/queries.hpp"
#include "code.hpp"
#include "write_traverser.hpp"
#include "read_traverser.hpp"
#include "update_traverser.hpp"
#include "delete_traverser.hpp"
#include "read_write_traverser.hpp"

class CodeTraverser : public Traverser, public Code
{
public:

    // TODO: remove code duplication

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

    void visit(ast::UpdateQuery& update_query) override
    {
        auto update_traverser = UpdateTraverser();
        update_query.accept(update_traverser);
        code = update_traverser.code;
    }

    void visit(ast::DeleteQuery& delete_query) override
    {
        auto delete_traverser = DeleteTraverser();
        delete_query.accept(delete_traverser);
        code = delete_traverser.code;
    }

    void visit(ast::ReadWriteQuery& read_write_query) override
    {
        auto read_write_traverser = ReadWriteTraverser();
        read_write_query.accept(read_write_traverser);
        code = read_write_traverser.code;
    }
};
