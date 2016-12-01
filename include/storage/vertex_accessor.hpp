#pragma once

#include "storage/label/label_collection.hpp"
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

    using record_t      = Vertex;
    using record_list_t = VertexRecord;

    // Removes self and all edges connected to it.
    void remove() const;

    // Returns number of out edges.
    size_t out_degree() const;

    // Returns number of in edges.
    size_t in_degree() const;

    // Returns number of all edges.
    size_t degree() const;

    // True if vertex isn't connected to any other vertex.
    bool isolated() const;

    // False if it already has the label.
    bool add_label(const Label &label);

    // False if it doesn't have the label.
    bool remove_label(const Label &label);

    // Checks if it has the label.
    bool has_label(const Label &label) const;

    // Returns container with all labels.
    const std::vector<label_ref_t> &labels() const;

    auto out() const;

    auto in() const;

    // True if there exists edge between other vertex and this vertex.
    bool in_contains(VertexAccessor const &other) const;

    template <typename Stream>
    void stream_repr(Stream& stream) const
    {
        if (!this->record)
            return;

        this->record->stream_repr(stream);
    }
};
