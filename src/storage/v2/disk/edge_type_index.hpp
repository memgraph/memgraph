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

#include "storage/v2/indices/edge_type_index.hpp"

namespace memgraph::storage {

class DiskEdgeTypeIndex : public EdgeTypeIndex {
 public:
  struct ActiveIndices : EdgeTypeIndex::ActiveIndices {
    bool IndexReady(EdgeTypeId edge_type) const override;

    bool IndexRegistered(EdgeTypeId edge_type) const override;

    auto ListIndices(uint64_t start_timestamp) const -> std::vector<EdgeTypeId> override;

    auto ApproximateEdgeCount(EdgeTypeId edge_type) const -> uint64_t override;

    void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                              const Transaction &tx) override;

    void AbortEntries(AbortableInfo const &info, uint64_t exact_start_timestamp) override {}

    auto GetAbortProcessor() const -> AbortProcessor override;
  };

  bool DropIndex(EdgeTypeId edge_type) override;

  void DropGraphClearIndices() override;

  auto GetActiveIndices() const -> std::unique_ptr<EdgeTypeIndex::ActiveIndices> override;
};

}  // namespace memgraph::storage
