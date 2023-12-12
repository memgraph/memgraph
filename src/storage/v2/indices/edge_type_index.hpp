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

#include <vector>

#include "storage/v2/transaction.hpp"

namespace memgraph::storage {

class EdgeTypeIndex {
 public:
  EdgeTypeIndex() = default;

  EdgeTypeIndex(const EdgeTypeIndex &) = delete;
  EdgeTypeIndex(EdgeTypeIndex &&) = delete;
  EdgeTypeIndex &operator=(const EdgeTypeIndex &) = delete;
  EdgeTypeIndex &operator=(EdgeTypeIndex &&) = delete;

  virtual ~EdgeTypeIndex() = default;

  virtual bool DropIndex(EdgeTypeId edge_type) = 0;

  virtual bool IndexExists(EdgeTypeId edge_type) const = 0;

  virtual std::vector<EdgeTypeId> ListIndices() const = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const = 0;

  virtual void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeTypeId edge_type, const Transaction &tx) = 0;
};

}  // namespace memgraph::storage
