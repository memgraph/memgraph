#ifndef MEMGRAPH_DATA_MODEL_EDGE_HPP
#define MEMGRAPH_DATA_MODEL_EDGE_HPP

#include "json/all.hpp"
#include "record.hpp"

struct Node;

struct Edge : Record
{
    Node* to;

    std::string data;
};

#endif
