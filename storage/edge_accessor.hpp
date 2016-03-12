#pragma once

#include "storage/edge.hpp"
#include "storage/record_accessor.hpp"

class Edges;

class Edge::Accessor : public RecordAccessor<Edge, Edges, Edge::Accessor>
{
public:
    using RecordAccessor::RecordAccessor;

    void edge_type(EdgeType type)
    {
        this->record->data.edge_type = type;
    }

    void from(VertexRecord* vertex_record)
    {
        this->record->data.from = vertex_record;
    }

    void to(VertexRecord* vertex_record)
    {
        this->record->data.to = vertex_record;
    }
};
