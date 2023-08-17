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
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/storage_mode.hpp"

namespace memgraph::storage {

EdgeImportModeCache::EdgeImportModeCache(const Config &config)
    : in_memory_indices_(Indices(config, StorageMode::IN_MEMORY_TRANSACTIONAL)) {}

bool EdgeImportModeCache::CreateIndex(LabelId label, PropertyId property,
                                      const std::optional<ParallelizedIndexCreationInfo> &parallel_exec_info) {
  auto *mem_label_property_index =
      static_cast<InMemoryLabelPropertyIndex *>(in_memory_indices_.label_property_index_.get());
  bool res = mem_label_property_index->CreateIndex(label, property, cache_.access(), parallel_exec_info);
  if (res) {
    scanned_label_property_indices_.insert({label, property});
  }
  return res;
}

}  // namespace memgraph::storage
