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

    const std::string& edge_type() const
    {
        return this->record->data.edge_type;
    }

    void from(VertexRecord* vertex_record)
    {
        this->record->data.from = vertex_record;
    }

    void to(VertexRecord* vertex_record)
    {
        this->record->data.to = vertex_record;
    }

    auto from()
    {
        return this->record->data.from;
    }

    auto to()
    {
        return this->record->data.to;
    }
};
