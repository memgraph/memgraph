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

#include <optional>
#include <tuple>
#include <utility>
#include "storage/v2/constraints.hpp"
#include "storage/v2/disk/disk_vertex.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/exceptions.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

struct DiskIndices;

class LabelDiskIndex {
 public:
  LabelDiskIndex(DiskIndices *indices, Constraints *constraints, Config config)
      : indices_(indices), constraints_(constraints), config_(config) {}

  /// TODO(andi): If there are no other usages of constaints_ and config_ maybe we can remove
  /// them from here
  VerticesIterable Vertices(LabelId label, View view, Transaction *transaction) {
    utils::SkipList<Vertex> vertices;
    /// TODO(andi): Perform loading
    // return VerticesIterable(AllVerticesIterable(vertices.access(), transaction, view, indices_, constraints_,
    // config_));
    throw utils::NotYetImplemented("LabelIndex::Vertices");
  }

  bool CreateIndex(LabelId label) { throw utils::NotYetImplemented("LabelIndex::CreateIndex"); }

  bool DropIndex(LabelId label) { throw utils::NotYetImplemented("LabelIndex::DropIndex"); }

  bool IndexExists(LabelId label) const { throw utils::NotYetImplemented("LabelIndex::IndexExists"); }

  std::vector<LabelId> ListIndices() const { throw utils::NotYetImplemented("LabelIndex::ListIndices"); }

  int64_t ApproximateVertexCount(LabelId label) const {
    throw utils::NotYetImplemented("LabelIndex::ApproximateVertexCount");
  }

  /// Clear all indexed vertices from the disk
  void Clear() { throw utils::NotYetImplemented("LabelIndex::Clear"); }

  /// TODO: Maybe we can remove completely interaction with garbage collector
  void RunGC() { throw utils::NotYetImplemented("LabelIndex::RunGC"); }

 private:
  std::vector<LabelId> index_;
  DiskIndices *indices_;
  Constraints *constraints_;
  Config config_;
};

class LabelPropertyDiskIndex {
 public:
  LabelPropertyDiskIndex(DiskIndices *indices, Constraints *constraints, Config config)
      : indices_(indices), constraints_(constraints), config_(config) {}

  /// TODO(andi): If there are no other usages of constaints_ and config_ maybe we can remove
  /// them from here
  VerticesIterable Vertices(LabelId label, PropertyId property,
                            const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                            const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                            Transaction *transaction) {
    utils::SkipList<Vertex> vertices;
    // return VerticesIterable(AllVerticesIterable(vertices.access(), transaction, view, indices_, constraints_,
    // config_));
    throw utils::NotYetImplemented("LabelPropertyIndex::Vertices");
  }

  bool CreateIndex(LabelId label, PropertyId property) {
    throw utils::NotYetImplemented("LabelPropertyIndex::CreateIndex");
  }

  bool DropIndex(LabelId label, PropertyId property) {
    throw utils::NotYetImplemented("LabelPropertyIndex::DropIndex");
  }

  bool IndexExists(LabelId label, PropertyId property) const {
    throw utils::NotYetImplemented("LabelPropertyIndex::IndexExists");
  }

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const {
    throw utils::NotYetImplemented("LabelPropertyIndex::ListIndices");
  }

  int64_t ApproximateVertexCount(LabelId label, PropertyId property) const {
    throw utils::NotYetImplemented("LabelPropertyIndex::ApproximateVertexCount");
  }

  /// Clear all indexed vertices from the disk
  void Clear() { throw utils::NotYetImplemented("LabelPropertyIndex::Clear"); }

  /// TODO: Maybe we can remove completely interaction with garbage collector
  void RunGC() { throw utils::NotYetImplemented("LabelPropertyIndex::RunGC"); }

 private:
  std::vector<std::pair<LabelId, PropertyId>> index_;
  DiskIndices *indices_;
  Constraints *constraints_;
  Config config_;
};

struct DiskIndices {
  DiskIndices(Constraints *constraints, Config config)
      : label_index(this, constraints, config), label_property_index(this, constraints, config) {}

  // Disable copy and move because members hold pointer to `this`.
  DiskIndices(const DiskIndices &) = delete;
  DiskIndices(DiskIndices &&) = delete;
  DiskIndices &operator=(const DiskIndices &) = delete;
  DiskIndices &operator=(DiskIndices &&) = delete;
  ~DiskIndices() = default;

  LabelDiskIndex label_index;
  LabelPropertyDiskIndex label_property_index;
};

void RemoveObsoleteEntries(DiskIndices *indices, uint64_t oldest_active_start_timestamp);

}  // namespace memgraph::storage
