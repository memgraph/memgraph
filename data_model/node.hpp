#ifndef MEMGRAPH_DATA_MODEL_NODE_HPP
#define MEMGRAPH_DATA_MODEL_NODE_HPP

#include <vector>

#include "json/all.hpp"
#include "record.hpp"
#include "edge.hpp"

struct Node : Record
{
    std::vector<Edge*> in;
    std::vector<Edge*> out;

    json::Object* data;
};

#endif
