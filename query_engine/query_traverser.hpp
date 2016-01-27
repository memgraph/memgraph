#pragma once

#include "cypher/codegen/cppgen.hpp"
#include "cypher/compiler.hpp"
#include "cypher/ast/ast.hpp"

class QueryTraverser
{
public:
    QueryTraverser() = default;

    void build_tree(const std::string& query)
    {
        tree = compiler.syntax_tree(query);
    }

    void traverse()
    {
        tree.root->accept(traverser);
    }

private:
    ast::Ast tree;
    cypher::Compiler compiler;
    CppGen traverser;
};
