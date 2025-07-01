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

  virtual bool DropIndex(LabelId label) = 0;
  virtual void DropGraphClearIndices() = 0;

  using AbortableInfo = std::map<LabelId, std::vector<Vertex *>>;

  struct ActiveIndices;

  struct AbortProcessor {
    explicit AbortProcessor() = default;
    explicit AbortProcessor(std::vector<LabelId> label) : label_(std::move(label)) {}

    void CollectOnLabelRemoval(LabelId label, Vertex *vertex) {
      if (std::binary_search(label_.begin(), label_.end(), label)) {
        cleanup_collection_[label].emplace_back(vertex);  // TODO (ivan): check that this is sorted
      }
    }
    std::vector<LabelId> label_;
    AbortableInfo cleanup_collection_;
  };

  struct ActiveIndices {
    virtual ~ActiveIndices() = default;

    virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

    // Not used for in-memory
    virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

    virtual bool IndexReady(LabelId label) const = 0;

    virtual std::vector<LabelId> ListIndices(uint64_t start_timestamp) const = 0;

    virtual uint64_t ApproximateVertexCount(LabelId label) const = 0;

    virtual void AbortEntries(AbortableInfo const &, uint64_t start_timestamp) = 0;

    virtual auto GetAbortProcessor() const -> AbortProcessor = 0;
  };

  virtual auto GetActiveIndices() const -> std::unique_ptr<ActiveIndices> = 0;
};

}  // namespace memgraph::storage
