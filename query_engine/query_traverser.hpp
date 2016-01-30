#pragma once

#include "cypher/ast/ast.hpp"
#include "cypher/compiler.hpp"
#include "traverser/node_traverser.hpp"

// The purpose of this class is to find out has
// the query already been compiled into the machine code / c++ code
// in the same pass query arguments should be extracted (e.g. poperties
// or node names)

// if the query has already been comiled into the machine code
// than it shouldn't be compiled again

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
        for (auto& kv : traverser.json) {
            cout << "Key: " << kv.first << ", Value: " << kv.second << endl;
        }
    }

private:
    ast::Ast tree;
    cypher::Compiler compiler;
    NodeTraverser traverser;
};
