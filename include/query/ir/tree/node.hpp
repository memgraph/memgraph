#pragma once

#include <memory>
#include <vector>

#include "query/backend/backend.hpp"

namespace ir
{

class Node
{
public:
    virtual ~Node() {}

    virtual void accept(Backend* visitor)
    {
        for (auto &child : childs)
        {
            visitor->process(child.get());
        }
    }

    std::vector<std::unique_ptr<Node>> childs;
    Node *parent;
};
}
