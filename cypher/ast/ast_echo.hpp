#ifndef MEMGRAPH_CYPHER_AST_PRINT_TREE_HPP
#define MEMGRAPH_CYPHER_AST_PRINT_TREE_HPP

#include <iostream>

#include "ast_visitor.hpp"
#include "values.hpp"

namespace ast
{

class AstEcho : public AstVisitor
{
public:
    
    virtual void visit(Boolean& node)
    {
        std::cout << "Boolean: " << node.value << std::endl;
    }
};

}

#endif
