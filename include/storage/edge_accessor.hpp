#pragma once

#include "storage/edge.hpp"
#include "storage/edge_record.hpp"
#include "storage/record_accessor.hpp"
#include "utils/assert.hpp"
#include "utils/reference_wrapper.hpp"

class Edges;

// TODO: Edge, Db, Edge::Accessor
class Edge::Accessor
    : public RecordAccessor<Edge, Edges, Edge::Accessor, EdgeRecord>
{
public:
    using RecordAccessor::RecordAccessor;

    void edge_type(edge_type_ref_t edge_type)
    {
        this->record->data.edge_type = &edge_type.get();
    }

    edge_type_ref_t edge_type() const
    {
        runtime_assert(this->record->data.edge_type != nullptr,
                       "EdgeType is null");
        return edge_type_ref_t(*this->record->data.edge_type);
    }

    auto from() const { return this->vlist->from(); }

    auto to() const { return this->vlist->to(); }
};
