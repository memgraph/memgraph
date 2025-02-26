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

class LabelPropertyIndex {
 public:
  struct IndexStats {
    // @TODO what are the impliciations of these being sets now we
    // have composite indices?
    // std::map<LabelId, std::vector<PropertyId>> l2p;
    // std::map<PropertyId, std::vector<LabelId>> p2l;
    std::map<LabelId, std::set<PropertyId>> l2p;
    std::map<PropertyId, std::set<LabelId>> p2l;
  };

  LabelPropertyIndex() = default;
  LabelPropertyIndex(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex(LabelPropertyIndex &&) = delete;
  LabelPropertyIndex &operator=(const LabelPropertyIndex &) = delete;
  LabelPropertyIndex &operator=(LabelPropertyIndex &&) = delete;

  virtual ~LabelPropertyIndex() = default;

  virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  // Not used for in-memory
  virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  virtual void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                   const Transaction &tx) = 0;

  virtual bool DropIndex(LabelId label, std::vector<PropertyId> const &properties) = 0;

  virtual bool IndexExists(LabelId label, std::vector<PropertyId> const &properties) const = 0;

  virtual std::vector<std::pair<LabelId, std::vector<PropertyId>>> ListIndices() const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, std::vector<PropertyId> const &properties,
                                          std::vector<PropertyValue> const &values) const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label, const std::vector<PropertyId> &properties) const = 0;

  virtual uint64_t ApproximateVertexCount(
      LabelId label, std::vector<PropertyId> const &properties,
      std::vector<std::optional<utils::Bound<PropertyValue>>> const &lower,
      std::vector<std::optional<utils::Bound<PropertyValue>>> const &upper) const = 0;

  virtual void DropGraphClearIndices() = 0;
};

}  // namespace memgraph::storage
