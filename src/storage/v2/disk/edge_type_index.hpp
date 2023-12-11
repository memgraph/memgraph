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

#include "storage/v2/indices/edge_type_index.hpp"

namespace memgraph::storage {

class DiskEdgeTypeIndex : public storage::EdgeTypeIndex {
 public:
  //   // Are these needed?
  //   virtual void UpdateOnAddLabel(EdgeTypeId added_label, Edge *edge_after_update, const Transaction &tx) {}
  //   // Not used for in-memory
  //   virtual void UpdateOnRemoveLabel(EdgeTypeId removed_label, const Transaction &tx) {}

  bool DropIndex(EdgeTypeId edge_type) override;

  bool IndexExists(EdgeTypeId edge_type) const override;

  std::vector<EdgeTypeId> ListIndices() const override;

  uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const override;

  void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeTypeId edge_type, const Transaction &tx) override;
};

}  // namespace memgraph::storage
