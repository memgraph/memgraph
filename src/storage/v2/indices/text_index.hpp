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

#include "storage/v2/mgcxx_mock.hpp"

namespace memgraph::storage {

class TextIndex {
 public:
  TextIndex() = default;

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  ~TextIndex() = default;

  void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) {}

  void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) {}

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
    auto index_config = mgcxx_mock::text_search::IndexConfig{.mappings = "TODO antepusic"};
    auto new_index = mgcxx_mock::text_search::Mock::create_index(label.ToString(), index_config);
    index_[label] = new_index;
    return true;

    // TODO add documents to index
  }

  bool DropIndex(LabelId label) {
    mgcxx_mock::text_search::Mock::drop_index(label.ToString());
    index_.erase(label);
    return true;
  }

  bool IndexExists(LabelId label) { return index_.contains(label); }

  mgcxx_mock::text_search::SearchOutput Search(LabelId label, mgcxx_mock::text_search::SearchInput input) {
    return mgcxx_mock::text_search::Mock::search(index_.at(label), input);
  }

  std::vector<LabelId> ListIndices() {
    std::vector<LabelId> ret;
    ret.reserve(index_.size());
    for (const auto &item : index_) {
      ret.push_back(item.first);
    }
    return ret;
  }

  uint64_t ApproximateVertexCount(LabelId label) { return 10; }

  std::map<LabelId, mgcxx_mock::text_search::IndexContext> index_;
};

}  // namespace memgraph::storage
