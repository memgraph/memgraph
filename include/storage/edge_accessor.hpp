#pragma once

#include "storage/edge.hpp"
#include "storage/edge_record.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/assert.hpp"
#include "utils/reference_wrapper.hpp"

class Edges;

// TODO: Edge, Db, Edge::Accessor
class Edge::Accessor : public RecordAccessor<Edge, Edge::Accessor, EdgeRecord>
{
public:
    using RecordAccessor::RecordAccessor;

    void edge_type(edge_type_ref_t edge_type);

    edge_type_ref_t edge_type() const;

    Vertex::Accessor from() const;

    Vertex::Accessor to() const;

    VertexRecord *from_record() const;

    VertexRecord *to_record() const;
};
