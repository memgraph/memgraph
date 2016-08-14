#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/db_transaction.hpp"
#include "storage/common.hpp"
#include "storage/indexes/index.hpp"
#include "storage/indexes/index_record_collection.hpp"
#include "storage/vertex_accessor.hpp"

class Vertices
{
public:
    using vertices_t = ConcurrentMap<uint64_t, VertexRecord>;

    vertices_t::Accessor access();

    const Vertex::Accessor find(DbTransaction &t, const Id &id);

    const Vertex::Accessor first(DbTransaction &t);

    Vertex::Accessor insert(DbTransaction &t);

    void update_label_index(const Label &label,
                            VertexIndexRecord &&index_record);

    VertexIndexRecordCollection &find_label_index(const Label &label);

private:
    vertices_t vertices;
    Index<label_ref_t, VertexIndexRecordCollection> label_index;
    AtomicCounter<uint64_t> counter;
};
