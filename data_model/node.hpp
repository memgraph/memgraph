#ifndef MEMGRAPH_DATA_MODEL_NODE_HPP
#define MEMGRAPH_DATA_MODEL_NODE_HPP

#include <vector>

#include "edge.hpp"

template <class T>
struct Node
{
    std::vector<Edge<T>*> in;
    std::vector<Edge<T>*> out;
    T* data;
};

#endif
