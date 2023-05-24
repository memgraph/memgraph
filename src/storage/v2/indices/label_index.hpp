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

class LabelIndex {
 public:
  LabelIndex(Indices *indices, Constraints *constraints, const Config &config)
      : indices_(indices), constraints_(constraints), config_(config) {}

  LabelIndex(const LabelIndex &) = delete;
  LabelIndex(LabelIndex &&) = delete;
  LabelIndex &operator=(const LabelIndex &) = delete;
  LabelIndex &operator=(LabelIndex &&) = delete;

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

 protected:
  /// TODO: andi maybe no need for have those in abstract class if disk storage isn't using it
  Indices *indices_;
  Constraints *constraints_;
  Config config_;
};

}  // namespace memgraph::storage
