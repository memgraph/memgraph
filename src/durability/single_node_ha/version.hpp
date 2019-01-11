#pragma once

///
///
/// IMPORTANT: Please update this file for every snapshot format change!!!
/// TODO (buda): This is not rock solid.
///

#include <array>
#include <cstdint>

namespace durability {

constexpr std::array<uint8_t, 4> kSnapshotMagic{{'M', 'G', 's', 'n'}};
constexpr std::array<uint8_t, 4> kWalMagic{{'M', 'G', 'w', 'l'}};

// The current default version of snapshot and WAL encoding / decoding.
constexpr int64_t kVersion{7};

// Snapshot format (version 7):
// 1) Magic number + snapshot version
//
// The following two entries are required when recovering from snapshot combined
// with WAL to determine record visibility.
// 2) Transactional ID of the snapshooter
// 3) Transactional snapshot of the snapshooter
//
// 4) A list of label+property indices.
//
// 5) Bolt encoded nodes. Each node is written in the following format:
//      * gid, labels, properties
// 6) Bolt encoded edges. Each edge is written in the following format:
//      * gid
//      * from, to
//      * edge_type
//      * properties
//
// 7) Snapshot summary (number of nodes, number of edges, hash)

}  // namespace durability
