#pragma once

// #include "storage/edge_accessor.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
// #include "utils/iterator/iterator.hpp"

class Vertices;

class VertexAccessor : public RecordAccessor<TypeGroupVertex, VertexAccessor>
{
public:
    using RecordAccessor::RecordAccessor;
    typedef Vertex record_t;
    typedef VertexRecord record_list_t;

    size_t out_degree() const;

    size_t in_degree() const;

    size_t degree() const;

    // False if it's label with it already.
    bool add_label(const Label &label);

    // False if it doesn't have label.
    bool remove_label(const Label &label);

    bool has_label(const Label &label) const;

    const std::vector<label_ref_t> &labels() const;

    auto out() const;

    auto in() const;

    // True if there exists edge other->this
    bool in_contains(VertexAccessor const &other) const;
};
