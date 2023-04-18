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

#include "storage/v2/edge.hpp"

namespace memgraph::storage {

struct DiskEdge : public Edge {
  /// Create a new edge with the given GID and delta.
  /// Use this constructor when creating a new edge in the transaction.
  DiskEdge(Gid gid, Delta *delta) : Edge(gid, delta) {}

  /// Create a new edge with the given GID, delta and modification timestamp.
  /// Use this constructor when loading an edge from disk.
  DiskEdge(Gid gid, Delta *delta, uint64_t modification_ts) : Edge(gid, delta), modification_ts(modification_ts) {}

  /// modification timestamp of the last transaction that modified this vertex.
  uint64_t modification_ts;
};

}  // namespace memgraph::storage
