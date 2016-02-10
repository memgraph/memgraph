#pragma once

#include "cypher/ast/ast.hpp"
#include "cypher/compiler.hpp"
#include "create_traverser.hpp"

class QueryTraverser
{
public:
    QueryTraverser() = default;

    void build_tree(const std::string& query)
    {
        tree = compiler.syntax_tree(query);
    }

    std::string traverse()
    {
        tree.root->accept(traverser);
        return traverser.code;
    }

private:
    ast::Ast tree;
    cypher::Compiler compiler;
    CreateTraverser traverser;
};
