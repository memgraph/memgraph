#pragma once

///
///
/// IMPORTANT: Please update this file for every snapshot format change!!!
/// TODO (buda): This is not rock solid.
///

#include <array>
#include <cstdint>

namespace durability {

constexpr std::array<uint8_t, 6> kSnapshotMagic{{'M', 'G', 'H', 'A', 's', 'n'}};

// The current default version of snapshot and WAL encoding / decoding.
constexpr int64_t kVersion{9};

// Snapshot format (version 9):
// 1) Magic number + snapshot version
//
// 2) A list of label+property indices.
//
// 3) Bolt encoded nodes. Each node is written in the following format:
//      * gid, labels, properties
// 4) Bolt encoded edges. Each edge is written in the following format:
//      * gid
//      * from, to
//      * edge_type
//      * properties
//
// 5) Snapshot summary (number of nodes, number of edges, hash)

}  // namespace durability
