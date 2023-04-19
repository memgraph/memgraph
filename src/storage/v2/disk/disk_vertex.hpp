// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <cstdint>
#include "storage/v2/vertex.hpp"

namespace memgraph::storage {

struct DiskVertex : Vertex {
  /// Create a new vertex with the given GID and delta.
  /// Use this constructor when creating a new vertex in the transaction.
  DiskVertex(Gid gid, Delta *delta) : Vertex(gid, delta) {}

  /// Create a new vertex with the given GID, delta and modification timestamp.
  /// Use this constructor when loading a vertex from disk.
  DiskVertex(Gid gid, Delta *delta, uint64_t modification_ts) : Vertex(gid, delta), modification_ts(modification_ts) {}

  /// Modification timestamp of the last transaction that modified this vertex.
  uint64_t modification_ts;
};

inline bool operator==(const DiskVertex &first, const DiskVertex &second) {
  return first.modification_ts == second.modification_ts;
}
inline bool operator<(const DiskVertex &first, const DiskVertex &second) {
  return first.modification_ts < second.modification_ts;
}
inline bool operator==(const DiskVertex &first, const uint64_t &second) { return first.modification_ts == second; }
inline bool operator<(const DiskVertex &first, const uint64_t &second) { return first.modification_ts < second; }

}  // namespace memgraph::storage
