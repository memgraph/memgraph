#pragma once

#include "mvcc/single_node/version_list.hpp"
#include "storage/single_node/address.hpp"

class Edge;
class Vertex;
namespace storage {
using VertexAddress = Address<mvcc::VersionList<Vertex>>;
using EdgeAddress = Address<mvcc::VersionList<Edge>>;

}  // namespace storage
