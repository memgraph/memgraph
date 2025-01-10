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

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::storage {

class LabelPropertyCompositeIndex {
 public:
  struct IndexStats {
    std::map<LabelId, std::vector<PropertyId>> l2p;
    std::map<PropertyId, std::vector<LabelId>> p2l;
  };

  LabelPropertyCompositeIndex() = default;
  LabelPropertyCompositeIndex(const LabelPropertyCompositeIndex &) = delete;
  LabelPropertyCompositeIndex(LabelPropertyCompositeIndex &&) = delete;
  LabelPropertyCompositeIndex &operator=(const LabelPropertyCompositeIndex &) = delete;
  LabelPropertyCompositeIndex &operator=(LabelPropertyCompositeIndex &&) = delete;

  virtual ~LabelPropertyCompositeIndex() = default;

  virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  // Not used for in-memory
  virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  virtual void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                   const Transaction &tx) = 0;

  virtual bool DropIndex(LabelId label, const std::vector<PropertyId> &properties) = 0;

  virtual bool IndexExists(LabelId label, const std::vector<PropertyId> &properties) const = 0;

  virtual std::vector<std::pair<LabelId, std::vector<PropertyId>>> ListIndices() const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, const std::vector<PropertyId> &properties) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, const std::vector<PropertyId> &properties,
                                          const PropertyValue &value) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, const std::vector<PropertyId> &properties,
                                          const std::optional<utils::Bound<PropertyValue>> &lower,
                                          const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

  virtual void DropGraphClearIndices() = 0;
};

}  // namespace memgraph::storage
