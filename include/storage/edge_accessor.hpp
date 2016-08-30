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

// There exists circular dependecy with VertexAccessor.
class EdgeAccessor : public RecordAccessor<TypeGroupEdge, EdgeAccessor>
{
    friend VertexAccessor;

public:
    using RecordAccessor::RecordAccessor;
    typedef Edge record_t;
    typedef EdgeRecord record_list_t;

    // Removes self and disconects vertices from it.
    void remove() const;

    void edge_type(EdgeType const &edge_type);

    const EdgeType &edge_type() const;

    // EdgeAccessor doesnt need to be filled
    VertexAccessor from() const;

    // EdgeAccessor doesnt need to be filled
    VertexAccessor to() const;
};
