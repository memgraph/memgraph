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

#include "storage/v2/indices/edge_property_index.hpp"

namespace memgraph::storage {

class DiskEdgePropertyIndex : public EdgePropertyIndex {
 public:
  bool DropIndex(PropertyId property) override;

  struct ActiveIndices : EdgePropertyIndex::ActiveIndices {
    void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                             PropertyId property, PropertyValue value, uint64_t timestamp) override;

    uint64_t ApproximateEdgeCount(PropertyId property) const override;

    uint64_t ApproximateEdgeCount(PropertyId property, const PropertyValue &value) const override;

    uint64_t ApproximateEdgeCount(PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
                                  const std::optional<utils::Bound<PropertyValue>> &upper) const override;

    bool IndexExists(PropertyId property) const override;

    bool IndexReady(PropertyId property) const override;

    std::vector<PropertyId> ListIndices(uint64_t start_timestamp) const override;

    auto GetAbortProcessor() const -> AbortProcessor override;
    void AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) override;
  };

  void DropGraphClearIndices() override;

  auto GetActiveIndices() const -> std::unique_ptr<EdgePropertyIndex::ActiveIndices> override;
};

}  // namespace memgraph::storage
