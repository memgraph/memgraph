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

#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/indices/label_property_composite_index.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class DiskLabelPropertyCompositeIndex : public storage::LabelPropertyCompositeIndex {
 public:
  explicit DiskLabelPropertyCompositeIndex(const Config &config);

  static bool CreateIndex(LabelId label, const std::vector<PropertyId> &properties,
                          const std::vector<std::pair<std::string, std::string>> &vertices);

  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) override;

  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) override{};

  bool DropIndex(LabelId label, const std::vector<PropertyId> &properties) override;

  bool IndexExists(LabelId label, const std::vector<PropertyId> &properties) const override;

  std::vector<std::pair<LabelId, std::vector<PropertyId>>> ListIndices() const override;

  uint64_t ApproximateVertexCount(LabelId label, const std::vector<PropertyId> &properties) const override;

  uint64_t ApproximateVertexCount(LabelId label, const std::vector<PropertyId> &properties,
                                  const std::vector<std::optional<utils::Bound<PropertyValue>>> &lower,
                                  const std::vector<std::optional<utils::Bound<PropertyValue>>> &upper) const override;

  void DropGraphClearIndices() override{};
};

}  // namespace memgraph::storage
