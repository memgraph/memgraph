#pragma once

#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
#include "utils/iterator/iterator.hpp"

class Vertices;
class EdgeAccessor;

// There exists circular dependecy with EdgeAccessor.
class VertexAccessor : public RecordAccessor<TypeGroupVertex, VertexAccessor>
{
    friend EdgeAccessor;

public:
    using RecordAccessor::RecordAccessor;
    typedef Vertex record_t;
    typedef VertexRecord record_list_t;

    // Removes self and all edges connected to it.
    void remove() const;

    size_t out_degree() const;

    size_t in_degree() const;

    size_t degree() const;

    // True if vertex isn't connected to any other vertex.
    bool isolated() const;

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
