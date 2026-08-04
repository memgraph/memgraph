// Copyright 2026 Memgraph Ltd.
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
#include <memory>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"

namespace memgraph::storage {

struct ActiveIndicesUpdater;
struct Transaction;
struct Vertex;

struct VertexPropertyIndexActiveIndices;
struct VertexPropertyIndexAbortProcessor;
using VertexPropertyIndexAbortableInfo = std::map<PropertyId, std::vector<std::pair<PropertyValue, Vertex *>>>;

class VertexPropertyIndex {
 public:
  using AbortableInfo = VertexPropertyIndexAbortableInfo;
  using AbortProcessor = VertexPropertyIndexAbortProcessor;
  using ActiveIndices = VertexPropertyIndexActiveIndices;

  VertexPropertyIndex() = default;

  VertexPropertyIndex(VertexPropertyIndex const &) = delete;
  VertexPropertyIndex(VertexPropertyIndex &&) = delete;
  VertexPropertyIndex &operator=(VertexPropertyIndex const &) = delete;
  VertexPropertyIndex &operator=(VertexPropertyIndex &&) = delete;

  virtual ~VertexPropertyIndex() = default;

  virtual void DropGraphClearIndices() = 0;

  virtual auto GetActiveIndices() const -> std::shared_ptr<ActiveIndices> = 0;
};

struct VertexPropertyIndexAbortProcessor {
  explicit VertexPropertyIndexAbortProcessor(std::span<PropertyId const> properties);

  void CollectOnPropertyChange(PropertyId property, Vertex *vertex, PropertyValue value);

  VertexPropertyIndexAbortableInfo cleanup_collection_;
  bool IsInteresting(PropertyId property) const;
};

struct VertexPropertyIndexActiveIndices {
  virtual ~VertexPropertyIndexActiveIndices() = default;

  virtual void UpdateOnSetProperty(PropertyId property, PropertyValue value, Vertex *vertex, uint64_t timestamp) = 0;

  virtual uint64_t ApproximateVertexCount(PropertyId property) const = 0;

  virtual uint64_t ApproximateVertexCount(PropertyId property, PropertyValue const &value) const = 0;

  virtual uint64_t ApproximateVertexCount(PropertyId property, std::optional<utils::Bound<PropertyValue>> const &lower,
                                          std::optional<utils::Bound<PropertyValue>> const &upper) const = 0;

  virtual bool IndexExists(PropertyId property) const = 0;

  virtual bool IndexReady(PropertyId property) const = 0;

  virtual std::vector<PropertyId> ListIndices(uint64_t start_timestamp) const = 0;

  virtual auto GetAbortProcessor() const -> VertexPropertyIndexAbortProcessor = 0;

  virtual void AbortEntries(VertexPropertyIndexAbortableInfo const &info, uint64_t start_timestamp) = 0;
};

}  // namespace memgraph::storage
