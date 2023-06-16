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

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::storage {

struct IndexStats {
  double statistic, avg_group_size;
};

class LabelPropertyIndex {
 public:
  LabelPropertyIndex(Indices *indices, Constraints *constraints, const Config &config)
      : indices_(indices), constraints_(constraints), config_(config) {}

  LabelPropertyIndex(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex(LabelPropertyIndex &&) = delete;
  LabelPropertyIndex &operator=(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex &operator=(LabelPropertyIndex &&) = delete;

  virtual ~LabelPropertyIndex() = default;

  virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_before_update, const Transaction &tx) = 0;

  virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_before_update, const Transaction &tx) = 0;

  virtual void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                   const Transaction &tx) = 0;

  virtual bool DropIndex(LabelId label, PropertyId property) = 0;

  virtual bool IndexExists(LabelId label, PropertyId property) const = 0;

  virtual std::vector<std::pair<LabelId, PropertyId>> ListIndices() const = 0;

  virtual void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property, const PropertyValue &value) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                          const std::optional<utils::Bound<PropertyValue>> &lower,
                                          const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

  virtual std::vector<std::pair<LabelId, PropertyId>> ClearIndexStats() = 0;

  virtual std::vector<std::pair<LabelId, PropertyId>> DeleteIndexStatsForLabel(const storage::LabelId &label) = 0;

  virtual void SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                             const storage::IndexStats &stats) = 0;

  virtual std::optional<storage::IndexStats> GetIndexStats(const storage::LabelId &label,
                                                           const storage::PropertyId &property) const = 0;

  virtual void Clear() = 0;

 protected:
  std::map<std::pair<LabelId, PropertyId>, storage::IndexStats> stats_;
  Indices *indices_;
  Constraints *constraints_;
  Config config_;
};

}  // namespace memgraph::storage
