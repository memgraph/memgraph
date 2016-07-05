#pragma once

#include "mvcc/version_list.hpp"
#include "utils/total_ordering.hpp"

template <class T>
class IndexRecord : TotalOrdering<IndexRecord<T>>
{
public:
    using vlist_t = mvcc::VersionList<T>;

    IndexRecord() = default;

    IndexRecord(T *record, vlist_t *vlist) : record(record), vlist(vlist)
    {
        assert(record != nullptr);
        assert(vlist != nullptr);
    }

    friend bool operator<(const IndexRecord& lhs, const IndexRecord& rhs)
    {
        return lhs.record < rhs.record;
    }

    friend bool operator==(const IndexRecord& lhs, const IndexRecord& rhs)
    {
        return lhs.record == rhs.record;
    }

    bool empty() const { return record == nullptr; }

    // const typename T::Accessor get()
    // {
    //     // TODO: if somebody wants to read T content
    //     // const T::Accessor has to be returned from here
    //     // the problem is that here we don't have pointer to store
    //     // TODO: figure it out
    // }

// private:
    T *const record{nullptr};
    vlist_t *const vlist{nullptr};
};

using VertexIndexRecord = IndexRecord<Vertex>;
using EdgeIndexRecord = IndexRecord<Edge>;
