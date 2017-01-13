#pragma once

#include <vector>

#include "ast_node.hpp"

namespace ast
{

class Ast
{
public:
    Ast() = default;
    Ast(const Ast&) = delete;

    Ast(Ast&& other) : root(other.root), items(std::move(other.items))
    {
        other.root = nullptr;
    }

    Ast& operator=(Ast&& other)
    {
        // TODO: write this more appropriate
        // here is CP of above code
        if(this != &other) {
            this->root = other.root;
            other.root = nullptr;
            this->items = std::move(other.items);
        }
        return *this;
    }

    AstVisitable* root;

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
    // parsing without using shared pointers
    std::vector<AstVisitable::uptr> items;
};

}
