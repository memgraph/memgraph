#ifndef MEMGRAPH_CYPHER_CODEGEN_CPPGEN_HPP
#define MEMGRAPH_CYPHER_CODEGEN_CPPGEN_HPP

#include "cypher/visitor/traverser.hpp"

class CppGen : public Traverser
{
    struct CreateGen : public Traverser
    {
        void visit(ast::Pattern& pattern) override
        {
            
        }

        void visit(ast
    };

public:

    void visit(ast::Start& start) override
    {

    }

    void visit(ast::Create create) override
    {
    };
};

#endif
