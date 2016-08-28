#pragma once

// There exists circular dependecy with EdgeAccessor. This file serves to break
// that circularity.
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

const auto EdgeAccessor::from() const
{
    return VertexAccessor(this->vlist->from(), this->db);
}

const auto EdgeAccessor::to() const
{
    return VertexAccessor(this->vlist->to(), this->db);
}

auto VertexAccessor::out() const
{
    DbTransaction &t = this->db;
    return iter::make_map(iter::make_iter_ref(record->data.out),
                          [&](auto e) -> auto { return EdgeAccessor(*e, t); });
}

auto VertexAccessor::in() const
{
    DbTransaction &t = this->db;
    return iter::make_map(iter::make_iter_ref(record->data.in),
                          [&](auto e) -> auto { return EdgeAccessor(e, t); });
}
