#pragma once

#include "storage/edge.hpp"
#include "storage/edge_record.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/assert.hpp"
#include "utils/reference_wrapper.hpp"

class EdgeType;
using edge_type_ref_t = ReferenceWrapper<const EdgeType>;

class Edges;

class EdgeAccessor : public RecordAccessor<TypeGroupEdge, EdgeAccessor>
{
public:
    using RecordAccessor::RecordAccessor;
    typedef Edge record_t;
    typedef EdgeRecord record_list_t;

    void edge_type(edge_type_ref_t edge_type);

    edge_type_ref_t edge_type() const;

    VertexAccessor from() const;

    VertexAccessor to() const;
};
