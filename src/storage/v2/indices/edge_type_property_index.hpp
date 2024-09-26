// Copyright 2024 Memgraph Ltd.
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

#include <map>
#include <vector>

#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"

namespace memgraph::storage {

struct Transaction;
struct Vertex;
struct Edge;

class EdgeTypePropertyIndex {
 public:
  struct IndexStats {
    std::map<EdgeTypeId, std::vector<PropertyId>> et2p;
    std::map<PropertyId, std::vector<EdgeTypeId>> p2et;
  };

  EdgeTypePropertyIndex() = default;

  EdgeTypePropertyIndex(const EdgeTypePropertyIndex &) = delete;
  EdgeTypePropertyIndex(EdgeTypePropertyIndex &&) = delete;
  EdgeTypePropertyIndex &operator=(const EdgeTypePropertyIndex &) = delete;
  EdgeTypePropertyIndex &operator=(EdgeTypePropertyIndex &&) = delete;

  virtual ~EdgeTypePropertyIndex() = default;

  virtual bool DropIndex(EdgeTypeId edge_type, PropertyId property) = 0;

  virtual bool IndexExists(EdgeTypeId edge_type, PropertyId property) const = 0;

  virtual std::vector<std::pair<EdgeTypeId, PropertyId>> ListIndices() const = 0;

  virtual void UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge, EdgeTypeId edge_type,
                                   PropertyId property, PropertyValue value, uint64_t timestamp) = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                        const PropertyValue &value) const = 0;

  virtual uint64_t ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                        const std::optional<utils::Bound<PropertyValue>> &lower,
                                        const std::optional<utils::Bound<PropertyValue>> &upper) const = 0;

  virtual void UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to,
                                        EdgeRef edge_ref, EdgeTypeId edge_type, PropertyId property,
                                        const PropertyValue &value, const Transaction &tx) = 0;

  virtual void DropGraphClearIndices() = 0;
};

}  // namespace memgraph::storage
