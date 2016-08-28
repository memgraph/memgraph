#pragma once

#include "storage/vertex_accessor.hpp"

#include "database/db.hpp"
#include "storage/vertex_record.hpp"
#include "storage/vertices.hpp"
#include "utils/iterator/iterator.hpp"

size_t VertexAccessor::out_degree() const
{
    return this->record->data.out.degree();
}

size_t VertexAccessor::in_degree() const
{
    return this->record->data.in.degree();
}

size_t VertexAccessor::degree() const { return in_degree() + out_degree(); }

bool VertexAccessor::add_label(const Label &label)
{
    // update vertex
    return this->record->data.labels.add(label);
}

bool VertexAccessor::remove_label(const Label &label)
{
    // update vertex
    return this->record->data.labels.remove(label);
}

bool VertexAccessor::has_label(const Label &label) const
{
    return this->record->data.labels.has(label);
}

const std::vector<label_ref_t> &VertexAccessor::labels() const
{
    return this->record->data.labels();
}

// Returns unfilled accessors
auto VertexAccessor::out() const
{
    DbTransaction &t = this->db;
    return iter::make_map(iter::make_iter_ref(record->data.out),
                          [&](auto e) -> auto { return EdgeAccessor(*e, t); });
}

// Returns unfilled accessors
auto VertexAccessor::in() const
{
    DbTransaction &t = this->db;
    return iter::make_map(iter::make_iter_ref(record->data.in),
                          [&](auto e) -> auto { return EdgeAccessor(e, t); });
}

bool VertexAccessor::in_contains(VertexAccessor const &other) const
{
    return record->data.in.contains(other.vlist);
}
