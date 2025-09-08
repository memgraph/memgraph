// Copyright 2025 Memgraph Ltd.
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

#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"

#include <algorithm>
#include <map>
#include <span>
#include <vector>

namespace memgraph::storage {

struct Transaction;
struct Vertex;

class EdgeTypeIndex {
 public:
  using AbortableInfo = std::map<EdgeTypeId, std::vector<std::tuple<Vertex *, Vertex *, Edge *>>>;

  struct AbortProcessor {
    explicit AbortProcessor(std::span<EdgeTypeId const> edge_types);

    void CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex, Edge *edge);

    AbortableInfo cleanup_collection_;
  };

  struct ActiveIndices {
    virtual ~ActiveIndices() = default;

    virtual void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                      const Transaction &tx) = 0;

    virtual auto ApproximateEdgeCount(EdgeTypeId edge_type) const -> uint64_t = 0;

    virtual bool IndexReady(EdgeTypeId edge_type) const = 0;
    virtual bool IndexRegistered(EdgeTypeId edge_type) const = 0;

    virtual auto ListIndices(uint64_t start_timestamp) const -> std::vector<EdgeTypeId> = 0;

    virtual auto GetAbortProcessor() const -> AbortProcessor = 0;

    virtual void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) = 0;
  };

  EdgeTypeIndex() = default;

  EdgeTypeIndex(const EdgeTypeIndex &) = delete;
  EdgeTypeIndex(EdgeTypeIndex &&) = delete;
  EdgeTypeIndex &operator=(const EdgeTypeIndex &) = delete;
  EdgeTypeIndex &operator=(EdgeTypeIndex &&) = delete;

  virtual ~EdgeTypeIndex() = default;

  virtual bool DropIndex(EdgeTypeId edge_type) = 0;

  virtual void DropGraphClearIndices() = 0;

  virtual auto GetActiveIndices() const -> std::unique_ptr<ActiveIndices> = 0;
};

}  // namespace memgraph::storage
