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

#include "storage/v2/constraints.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::storage {

using ParalellizedIndexCreationInfo =
    std::pair<std::vector<std::pair<Gid, uint64_t>> /*vertex_recovery_info*/, uint64_t /*thread_count*/>;

class LabelIndex {
 private:
  struct Entry {
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) {
      return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
    }
    bool operator==(const Entry &rhs) { return vertex == rhs.vertex && timestamp == rhs.timestamp; }
  };

 public:
  LabelIndex(Indices *indices, Constraints *constraints, Config::Items config)
      : indices_(indices), constraints_(constraints), config_(config) {}

  virtual ~LabelIndex() = default;

  virtual void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) = 0;

  virtual bool DropIndex(LabelId label) = 0;

  virtual bool IndexExists(LabelId label) const = 0;

  virtual std::vector<LabelId> ListIndices() const = 0;

  /// TODO: (andi) Maybe not needed for disk version so remove from abstract class
  virtual void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) = 0;

  virtual int64_t ApproximateVertexCount(LabelId label) const = 0;

  virtual void Clear() = 0;

  /// TODO: (andi) Maybe not needed for disk version so remove from abstract class
  virtual void RunGC() = 0;

 private:
  std::map<LabelId, utils::SkipList<Entry>> index_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
};

}  // namespace memgraph::storage
