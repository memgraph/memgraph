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

#include "storage/v2/indices/indices.hpp"
#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"

namespace memgraph::storage {

void Indices::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) const {
  static_cast<InMemoryLabelIndex *>(label_index_.get())->RemoveObsoleteEntries(oldest_active_start_timestamp);
  static_cast<InMemoryLabelPropertyIndex *>(label_property_index_.get())
      ->RemoveObsoleteEntries(oldest_active_start_timestamp);
}

void Indices::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) const {
  label_index_->UpdateOnAddLabel(label, vertex, tx);
  label_property_index_->UpdateOnAddLabel(label, vertex, tx);
}

void Indices::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, const Transaction &tx) const {
  label_index_->UpdateOnRemoveLabel(label, vertex, tx);
  label_property_index_->UpdateOnRemoveLabel(label, vertex, tx);
}

void Indices::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                  const Transaction &tx) const {
  label_property_index_->UpdateOnSetProperty(property, value, vertex, tx);
}

Indices::Indices(const Config &config, StorageMode storage_mode) {
  std::invoke([this, config, storage_mode]() {
    if (storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL || storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      label_index_ = std::make_unique<InMemoryLabelIndex>();
      label_property_index_ = std::make_unique<InMemoryLabelPropertyIndex>();
    } else {
      label_index_ = std::make_unique<DiskLabelIndex>(config);
      label_property_index_ = std::make_unique<DiskLabelPropertyIndex>(config);
    }
  });
}

}  // namespace memgraph::storage
