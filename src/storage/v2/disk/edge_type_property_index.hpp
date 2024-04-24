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

#include "storage/v2/indices/edge_type_property_index.hpp"

namespace memgraph::storage {

class DiskEdgeTypePropertyIndex : public storage::EdgeTypePropertyIndex {
 public:
  bool DropIndex(EdgeTypeId edge_type, PropertyId property) override;

  bool IndexExists(EdgeTypeId edge_type, PropertyId property) const override;

  std::vector<std::pair<EdgeTypeId, PropertyId>> ListIndices() const override;

  void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                           PropertyId property, PropertyValue value, uint64_t timestamp) override;

  uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const override;

  void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to, EdgeRef edge_ref,
                                EdgeTypeId edge_type, PropertyId property, PropertyValue value,
                                const Transaction &tx) override;

  void DropGraphClearIndices() override;
};

}  // namespace memgraph::storage
