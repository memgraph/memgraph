#pragma once

///
///
/// IMPORTANT: Please update this file for every snapshot format change!!!
/// TODO (buda): This is not rock solid.
///

#include <array>
#include <cstdint>

namespace durability {

constexpr std::array<uint8_t, 4> kMagicNumber{{'M', 'G', 's', 'n'}};

// The current default version of snapshot and WAL encoding / decoding.
constexpr int64_t kVersion{6};

// Snapshot format (version 6):
// 1) Magic number + snapshot version
// 2) Distributed worker ID
//
// The following two entries indicate the starting points for generating new
// vertex/edge IDs in the DB. They are important when there are vertices/edges
// that were moved to another worker (in distributed Memgraph).
// 3) Vertex generator ID
// 4) Edge generator ID
//
// The following two entries are required when recovering from snapshot combined
// with WAL to determine record visibility.
// 5) Transactional ID of the snapshooter
// 6) Transactional snapshot of the snapshooter
//
// 7) A list of label+property indices.
//
// We must inline edges with nodes because some edges might be stored on other
// worker (edges are always stored only on the worker of the edge source).
// 8) Bolt encoded nodes. Each node is written in the following format:
//      * gid, labels, properties
//      * cypher_id
//      * inlined edges (edge address, other endpoint address and edge type)
// 9) Bolt encoded edges. Each edge is written in the following format:
//      * gid
//      * from, to
//      * edge_type
//      * properties
//      * cypher_id
//
// 10) Snapshot summary (number of nodes, number of edges, hash)

}  // namespace durability
