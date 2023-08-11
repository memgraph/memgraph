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

#include "storage/v2/disk//edge_import_mode_cache.hpp"

namespace memgraph::storage {

EdgeImportModeCache::EdgeImportModeCache(const DiskLabelIndex *disk_label_index,
                                         const DiskLabelPropertyIndex *disk_label_property_index) {
  const std::unordered_set<LabelId> label_index_info = disk_label_index->GetInfo();
  for (const LabelId label : label_index_info) {
    /// TODO: Parallel index creation
    label_index_->CreateIndex(label, cache_.access(), {});
  }
  using LabelPropertyInfo = std::pair<LabelId, PropertyId>;
  const std::set<LabelPropertyInfo> label_property_index_info = disk_label_property_index->GetInfo();
  for (const LabelPropertyInfo &label_property : label_property_index_info) {
    /// TODO: Parallel index creation
    label_property_index_->CreateIndex(label_property.first, label_property.second, cache_.access(), {});
  }
}

}  // namespace memgraph::storage
