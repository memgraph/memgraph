#include "storage/edge_accessor.hpp"

#include "storage/vertex_record.hpp"

void EdgeAccessor::remove() const
{
    RecordAccessor::remove();
    auto from_v = from();
    bool f_from = from_v.fill();
    assert(f_from);

    auto to_v = to();
    bool f_to = to_v.fill();
    assert(f_to);

    from_v.update().record->data.out.remove(vlist);
    to_v.update().record->data.in.remove(vlist);
}

void EdgeAccessor::edge_type(const EdgeType &edge_type)
{
    this->record->data.edge_type = &edge_type;
}

const EdgeType &EdgeAccessor::edge_type() const
{
    runtime_assert(this->record->data.edge_type != nullptr, "EdgeType is null");
    return *this->record->data.edge_type;
}

VertexAccessor EdgeAccessor::from() const
{
    return VertexAccessor(this->vlist->from(), this->db);
}

VertexAccessor EdgeAccessor::to() const
{
    return VertexAccessor(this->vlist->to(), this->db);
}
