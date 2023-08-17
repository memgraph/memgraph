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

#pragma once

#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/indices.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class EdgeImportModeCache {
 public:
  explicit EdgeImportModeCache(const Config &config);

  EdgeImportModeCache(const EdgeImportModeCache &) = delete;
  EdgeImportModeCache &operator=(const EdgeImportModeCache &) = delete;
  EdgeImportModeCache(EdgeImportModeCache &&) = delete;
  EdgeImportModeCache &operator=(EdgeImportModeCache &&) = delete;
  ~EdgeImportModeCache() = default;

  InMemoryLabelIndex::Iterable Vertices(LabelId label, View view, Transaction *transaction,
                                        Constraints *constraints) const {
    auto *mem_label_index = static_cast<InMemoryLabelIndex *>(in_memory_indices_.label_index_.get());
    return mem_label_index->Vertices(label, view, transaction, constraints);
  }

  InMemoryLabelPropertyIndex::Iterable Vertices(LabelId label, PropertyId property,
                                                const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                View view, Transaction *transaction, Constraints *constraints) const {
    auto *mem_label_property_index =
        static_cast<InMemoryLabelPropertyIndex *>(in_memory_indices_.label_property_index_.get());
    return mem_label_property_index->Vertices(label, property, lower_bound, upper_bound, view, transaction,
                                              constraints);
  }

  bool CreateIndex(LabelId label, PropertyId property,
                   const std::optional<ParallelizedIndexCreationInfo> &parallel_exec_info = {});

  utils::SkipList<Vertex> cache_;
  Indices in_memory_indices_;
  bool scanned_all_vertices_{false};
  std::set<LabelId> scanned_labels_;
  std::set<std::pair<LabelId, PropertyId>> scanned_label_property_indices_;
};

}  // namespace memgraph::storage
