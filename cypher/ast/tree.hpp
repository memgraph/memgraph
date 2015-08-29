#ifndef MEMGRAPH_CYPHER_AST_TREE_HPP
#define MEMGRAPH_CYPHER_AST_TREE_HPP

#include <vector>

#include "ast_node.hpp"

namespace ast
{

class Ast
{
public:
    Ast() {}

    AstVisitable::uptr root;

    void traverse(AstVisitor& visitor)
    {
        root->accept(visitor);
    }

    template <class T, typename... Args>
    T* create(Args&&... args)
    {
        auto node = new T(std::forward<Args>(args)...);
        items.push_back(std::unique_ptr<AstVisitable>(node));
        return node;
    }

private:
    // basically a gc vector that destroys all ast nodes once this object is
    // destroyed. parser generator is written in c and works only with raw
    // pointers so this is what makes it leak free after the parser finishes
    // parsing
    std::vector<AstVisitable::uptr> items;
};

}

#endif
