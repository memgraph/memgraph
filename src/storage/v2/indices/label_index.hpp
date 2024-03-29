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

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::storage {

class LabelIndex {
 public:
  LabelIndex() = default;

  LabelIndex(const LabelIndex &) = delete;
  LabelIndex(LabelIndex &&) = delete;
  LabelIndex &operator=(const LabelIndex &) = delete;
  LabelIndex &operator=(LabelIndex &&) = delete;

  virtual ~LabelIndex() = default;

  virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  // Not used for in-memory
  virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  virtual bool DropIndex(LabelId label) = 0;

  virtual bool IndexExists(LabelId label) const = 0;

  virtual std::vector<LabelId> ListIndices() const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label) const = 0;
};

}  // namespace memgraph::storage
