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

#include "edge_type_index.hpp"

#include "utils/exceptions.hpp"

#include "spdlog/spdlog.h"

namespace memgraph::storage {

bool DiskEdgeTypeIndex::DropIndex(EdgeTypeId /*edge_type*/) {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return true;
}

bool DiskEdgeTypeIndex::ActiveIndices::IndexReady(EdgeTypeId /*edge_type*/) const {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return false;
}

bool DiskEdgeTypeIndex::ActiveIndices::IndexRegistered(EdgeTypeId /*edge_type*/) const {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return false;
}

std::vector<EdgeTypeId> DiskEdgeTypeIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return {};
}

uint64_t DiskEdgeTypeIndex::ActiveIndices::ApproximateEdgeCount(EdgeTypeId /*edge_type*/) const {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
  return 0U;
}

void DiskEdgeTypeIndex::ActiveIndices::UpdateOnEdgeCreation(Vertex * /*from*/, Vertex * /*to*/, EdgeRef /*edge_ref*/,
                                                            EdgeTypeId /*edge_type*/, const Transaction & /*tx*/) {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
}

void DiskEdgeTypeIndex::ActiveIndices::UpdateOnEdgeModification(Vertex * /*old_from*/, Vertex * /*old_to*/,
                                                                Vertex * /*new_from*/, Vertex * /*new_to*/,
                                                                EdgeRef /*edge_ref*/, EdgeTypeId /*edge_type*/,
                                                                const Transaction & /*tx*/) {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
}

EdgeTypeIndex::AbortProcessor DiskEdgeTypeIndex::ActiveIndices::GetAbortProcessor() const { return AbortProcessor({}); }

void DiskEdgeTypeIndex::DropGraphClearIndices() {
  spdlog::warn("Edge-type index related operations are not yet supported using on-disk storage mode.");
}

auto DiskEdgeTypeIndex::GetActiveIndices() const -> std::unique_ptr<EdgeTypeIndex::ActiveIndices> {
  return std::make_unique<DiskEdgeTypeIndex::ActiveIndices>();
}

void EdgeTypeIndex::AbortProcessor::CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex,
                                                         Edge *edge) {
  auto it = cleanup_collection_.find(edge_type);
  if (it == cleanup_collection_.end()) return;
  it->second.emplace_back(from_vertex, to_vertex, edge);
}

EdgeTypeIndex::AbortProcessor::AbortProcessor(std::span<EdgeTypeId const> edge_types) {
  for (auto edge_type : edge_types) {
    cleanup_collection_.insert({edge_type, {}});
  }
}
}  // namespace memgraph::storage
