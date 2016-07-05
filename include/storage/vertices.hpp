#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/common.hpp"
#include "storage/indexes/index.hpp"
#include "storage/indexes/index_record_collection.hpp"
#include "storage/vertex_accessor.hpp"

class Vertices
{
public:
    const Vertex::Accessor find(tx::Transaction &t, const Id &id);

    Vertex::Accessor insert(tx::Transaction &t);

    void update_label_index(const Label &label,
                            VertexIndexRecord &&index_record);

    VertexIndexRecordCollection& find_label_index(const Label& label);

private:
    Index<label_ref_t, VertexIndexRecordCollection> label_index;

    ConcurrentMap<uint64_t, VertexRecord> vertices;
    AtomicCounter<uint64_t> counter;
};
