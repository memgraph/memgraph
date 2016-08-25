#include "storage/edge_accessor.hpp"

void EdgeAccessor::edge_type(edge_type_ref_t edge_type)
{
    this->record->data.edge_type = &edge_type.get();
}

edge_type_ref_t EdgeAccessor::edge_type() const
{
    runtime_assert(this->record->data.edge_type != nullptr, "EdgeType is null");
    return edge_type_ref_t(*this->record->data.edge_type);
}

VertexAccessor EdgeAccessor::from() const
{
    return VertexAccessor(this->vlist->from(), this->db);
}

VertexAccessor EdgeAccessor::to() const
{
    return VertexAccessor(this->vlist->to(), this->db);
}
