#pragma once

#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"

class Vertices;

class Vertex::Accessor : public RecordAccessor<Vertex, Vertex::Accessor>
{
public:
    using RecordAccessor::RecordAccessor;

    static Vertex::Accessor create(Vertex *t, mvcc::VersionList<Vertex> *vlist,
                                   DbTransaction &db)
    {
        return Vertex::Accessor(t, vlist, db);
    }

    size_t out_degree() const;

    size_t in_degree() const;

    size_t degree() const;

    // False if it's label with it already.
    bool add_label(const Label &label);

    // False if it doesn't have label.
    bool remove_label(const Label &label);

    bool has_label(const Label &label) const;

    const std::set<label_ref_t> &labels() const;

    auto out() const;

    auto in() const;

    // True if there exists edge other->this
    bool in_contains(Vertex::Accessor const &other) const;
};
