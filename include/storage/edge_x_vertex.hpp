#pragma once

// There exists circular dependecy with EdgeAccessor. This file serves to break
// that circularity.
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

auto VertexAccessor::out() const
{
    DbTransaction &t = this->db;
    std::cout << "VA OUT" << std::endl;
    std::cout << record->data.out.size() << std::endl;
    return iter::make_map(iter::make_iter_ref(record->data.out),
                          [&](auto e) -> auto { return EdgeAccessor(*e, t); });
}

auto VertexAccessor::in() const
{
    DbTransaction &t = this->db;
    return iter::make_map(iter::make_iter_ref(record->data.in),
                          [&](auto e) -> auto { return EdgeAccessor(e, t); });
}
