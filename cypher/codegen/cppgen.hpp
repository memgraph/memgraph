#ifndef MEMGRAPH_CYPHER_CODEGEN_CPPGEN_HPP
#define MEMGRAPH_CYPHER_CODEGEN_CPPGEN_HPP

#include "cypher/visitor/traverser.hpp"

#include <iostream>
using std::cout;
using std::endl;

class CppGen : public Traverser
{
    struct CreateGen : public Traverser
    {
        void visit(ast::Pattern& pattern) override
        {
            Traverser::visit(pattern);     
        }

        void visit(ast::Node& node) override
        {
            auto next_node = node.props->next;
            cout << next_node->value->idn->name;

            Traverser::visit(node);
        }
    };

public:

    void visit(ast::Start& start) override
    {
        Traverser::visit(start);
    }

    void visit(ast::Create& create) override
    {
        auto create_gen = CreateGen();
        create.accept(create_gen);
    };
};

#endif
