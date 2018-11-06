#pragma once

#include "storage/distributed/mvcc/version_list.hpp"
#include "storage/distributed/address.hpp"

class Edge;
class Vertex;
namespace storage {
using VertexAddress = Address<mvcc::VersionList<Vertex>>;
using EdgeAddress = Address<mvcc::VersionList<Edge>>;

}  // namespace storage
