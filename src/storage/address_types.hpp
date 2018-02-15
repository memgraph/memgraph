#pragma once

#include "mvcc/version_list.hpp"
#include "storage/address.hpp"

class Edge;
class Vertex;
namespace storage {
using VertexAddress = Address<mvcc::VersionList<Vertex>>;
using EdgeAddress = Address<mvcc::VersionList<Edge>>;

}  // namespace storage
