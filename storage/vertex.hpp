#ifndef MEMGRAPH_STORAGE_VERTEX_HPP
#define MEMGRAPH_STORAGE_VERTEX_HPP

#include "record.hpp"
#include "edge.hpp"

struct Vertex : Record
{
    std::list<Edge*> out;

    std::string name;
};

#endif
