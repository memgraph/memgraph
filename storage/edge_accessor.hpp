#pragma once

#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"

class Edges;

class Edge::Accessor : public RecordAccessor<Edge, Edges, Edge::Accessor>
{
public:
    using RecordAccessor::RecordAccessor;
};
