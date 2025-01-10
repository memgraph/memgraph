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

/// TODO: clear dependencies

#include "storage/v2/disk/label_property_composite_index.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/disk_utils.hpp"
#include "utils/rocksdb_serialization.hpp"

namespace memgraph::storage {

DiskLabelPropertyCompositeIndex::DiskLabelPropertyCompositeIndex(const Config &config) {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
}

bool DiskLabelPropertyCompositeIndex::CreateIndex(LabelId label, const std::vector<PropertyId> &properties,
                                                  const std::vector<std::pair<std::string, std::string>> &vertices) {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
  return true;
}

void DiskLabelPropertyCompositeIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                       const Transaction &tx) {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
}

void DiskLabelPropertyCompositeIndex::UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update,
                                                          const Transaction &tx) {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
}

bool DiskLabelPropertyCompositeIndex::DropIndex(LabelId label, const std::vector<PropertyId> &properties) {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
  return true;
}

bool DiskLabelPropertyCompositeIndex::IndexExists(LabelId label, const std::vector<PropertyId> &properties) const {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
  return true;
}

std::vector<std::pair<LabelId, std::vector<PropertyId>>> DiskLabelPropertyCompositeIndex::ListIndices() const {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
  return {};
}

uint64_t DiskLabelPropertyCompositeIndex::ApproximateVertexCount(LabelId /*label*/,
                                                                 const std::vector<PropertyId> & /*properties*/) const {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");

  return 10;
}

uint64_t DiskLabelPropertyCompositeIndex::ApproximateVertexCount(LabelId /*label*/,
                                                                 const std::vector<PropertyId> & /*properties*/,
                                                                 const PropertyValue & /*value*/) const {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
  return 10;
}

uint64_t DiskLabelPropertyCompositeIndex::ApproximateVertexCount(
    LabelId /*label*/, const std::vector<PropertyId> & /*properties*/,
    const std::optional<utils::Bound<PropertyValue>> & /*lower*/,
    const std::optional<utils::Bound<PropertyValue>> & /*upper*/) const {
  spdlog::warn("Label property composite index related operations are not yet supported using on-disk storage mode.");
  return 10;
}

}  // namespace memgraph::storage
