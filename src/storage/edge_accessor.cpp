#include "storage/edge_accessor.hpp"

void Edge::Accessor::edge_type(edge_type_ref_t edge_type)
{
    this->record->data.edge_type = &edge_type.get();
}

edge_type_ref_t Edge::Accessor::edge_type() const
{
    runtime_assert(this->record->data.edge_type != nullptr, "EdgeType is null");
    return edge_type_ref_t(*this->record->data.edge_type);
}

Vertex::Accessor Edge::Accessor::from() const
{
    return Vertex::Accessor(this->vlist->from(), this->db);
}

Vertex::Accessor Edge::Accessor::to() const
{
    return Vertex::Accessor(this->vlist->to(), this->db);
}
