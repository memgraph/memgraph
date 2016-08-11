#pragma once

#include <memory>

#include "data_structures/concurrent/concurrent_set.hpp"
#include "storage/indexes/index_record.hpp"

template <class T>
class IndexRecordCollection
{
public:
    using index_record_t = IndexRecord<T>;
    using index_record_collection_t = ConcurrentSet<index_record_t>;

    IndexRecordCollection()
        : records(std::make_unique<index_record_collection_t>())
    {
    }

    void add(index_record_t &&record)
    {
        auto accessor = records->access();
        accessor.insert(std::forward<index_record_t>(record));
    }

    auto access()
    {
        return records->access();
    } 

    // TODO: iterator and proxy

private:
    std::unique_ptr<index_record_collection_t> records;
};

using VertexIndexRecordCollection = IndexRecordCollection<Vertex>;
using EdgeIndexRecordCollection = IndexRecordCollection<Edge>;
