#pragma once

#include "mvcc/version_list.hpp"
#include "storage/edge.hpp"

class EdgeRecord : public mvcc::VersionList<Edge>
{
public:
    EdgeRecord(Id id, VertexRecord *from, VertexRecord *to)
        : from_v(from), to_v(to), VersionList(id)
    {
    }
    EdgeRecord(const VersionList &) = delete;

    /* @brief Move constructs the version list
     * Note: use only at the beginning of the "other's" lifecycle since this
     * constructor doesn't move the RecordLock, but only the head pointer
     */
    EdgeRecord(EdgeRecord &&other)
        : from_v(other.from_v), to_v(other.to_v), VersionList(std::move(other))
    {
    }

    VertexRecord *&get_key() { return this->from_v; }

    auto from() const { return this->from_v; }

    auto to() const { return this->to_v; }

private:
    VertexRecord *from_v;
    VertexRecord *to_v;
};
