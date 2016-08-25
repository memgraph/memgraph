#pragma once

#include "mvcc/version_list.hpp"
#include "storage/vertex.hpp"

class VertexRecord : public mvcc::VersionList<Vertex>
{
public:
    VertexRecord(Id id) : VersionList(id) {}
    VertexRecord(const VersionList &) = delete;
    VertexRecord(VersionList &&other) : VersionList(std::move(other)) {}
};
