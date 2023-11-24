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

  virtual void UpdateOnAddLabel(EdgeTypeId added_label, Edge *edge_after_update, const Transaction &tx) {}

  // Not used for in-memory
  virtual void UpdateOnRemoveLabel(EdgeTypeId removed_label, const Transaction &tx) {}

  virtual bool DropIndex(EdgeTypeId label) { return false; }

  virtual bool IndexExists(EdgeTypeId label) const { return false; }

  virtual std::vector<EdgeTypeId> ListIndices() const { return std::vector<EdgeTypeId>{}; }

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const { return 0; }

  virtual uint64_t ApproximateVertexCount(EdgeTypeId label) const { return 0; }
};

}  // namespace memgraph::storage
