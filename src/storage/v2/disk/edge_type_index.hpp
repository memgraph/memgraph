// Copyright 2024 Memgraph Ltd.
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
  bool DropIndex(EdgeTypeId edge_type) override;

  bool IndexExists(EdgeTypeId edge_type) const override;

  std::vector<EdgeTypeId> ListIndices() const override;

  uint64_t ApproximateEdgeCount(EdgeTypeId edge_type) const override;

  void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                            const Transaction &tx) override;

  void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to, EdgeRef edge_ref,
                                EdgeTypeId edge_type, const Transaction &tx) override;

  void DropGraphClearIndices() override;
};

}  // namespace memgraph::storage
