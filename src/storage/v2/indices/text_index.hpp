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

namespace memgraph::storage {

class TextIndex {
 public:
  TextIndex() = default;

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  virtual ~TextIndex() = default;

  virtual void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  // Not used for in-memory
  virtual void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) = 0;

  virtual bool DropIndex(LabelId label) = 0;

  virtual bool IndexExists(LabelId label) const = 0;

  virtual std::vector<LabelId> ListIndices() const = 0;

  virtual uint64_t ApproximateVertexCount(LabelId label) const = 0;
};

}  // namespace memgraph::storage
