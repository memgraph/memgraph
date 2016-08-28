#include "storage/edge_accessor.hpp"

void EdgeAccessor::edge_type(const EdgeType &edge_type)
{
    this->record->data.edge_type = &edge_type;
}

const EdgeType &EdgeAccessor::edge_type() const
{
    runtime_assert(this->record->data.edge_type != nullptr, "EdgeType is null");
    return *this->record->data.edge_type;
}
