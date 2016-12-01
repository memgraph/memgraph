#include "storage/edge_accessor.hpp"

#include <cassert>

#include "storage/vertex_record.hpp"
#include "storage/edge_type/edge_type.hpp"

void EdgeAccessor::remove() const
{
    RecordAccessor::remove();

    auto from_va = from();
    assert(from_va.fill());

    auto to_va = to();
    assert(to_va.fill());

    from_va.update().record->data.out.remove(vlist);
    to_va.update().record->data.in.remove(vlist);
}

void EdgeAccessor::edge_type(const EdgeType &edge_type)
{
    this->record->data.edge_type = &edge_type;
}

const EdgeType &EdgeAccessor::edge_type() const
{
    assert(this->record->data.edge_type != nullptr);
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
