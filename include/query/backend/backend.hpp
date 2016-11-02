#pragma once

#include "query/ir/tree/node.hpp"

class Backend
{
public:
    virtual void process(ir::Node *node) = 0;
    virtual ~Backend() {}
};
