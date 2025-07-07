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

class DiskEdgeTypePropertyIndex : public EdgeTypePropertyIndex {
 public:
  struct ActiveIndices : storage::EdgeTypePropertyIndex::ActiveIndices {
    void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                             PropertyId property, PropertyValue value, uint64_t timestamp) override;

    void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to, EdgeRef edge_ref,
                                  EdgeTypeId edge_type, PropertyId property, const PropertyValue &value,
                                  const Transaction &tx) override;

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const override;

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value) const override;

    uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                  const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;

    bool IndexReady(EdgeTypeId edge_type, PropertyId property) const override;

    auto ListIndices(uint64_t start_timestamp) const -> std::vector<std::pair<EdgeTypeId, PropertyId>> override;
  };

  bool DropIndex(EdgeTypeId edge_type, PropertyId property) override;

  void DropGraphClearIndices() override;

  auto GetActiveIndices() const -> std::unique_ptr<EdgeTypePropertyIndex::ActiveIndices> override {
    return std::make_unique<ActiveIndices>();
  }
};

}  // namespace memgraph::storage
