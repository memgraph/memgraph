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

  std::vector<mgcxx_mock::text_search::IndexContext *> GetApplicableTextIndices(Vertex *vertex, Storage *storage) {
    std::vector<mgcxx_mock::text_search::IndexContext *> applicable_text_indices;
    for (const auto &label : vertex->labels) {
      if (label_to_index_.contains(label)) {
        applicable_text_indices.push_back(&index_.at(label_to_index_.at(label)));
      }
    }
    return applicable_text_indices;
  }

  /// @throw std::bad_alloc
  bool CreateIndex(std::string index_name, LabelId label,
                   const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
    auto index_config = mgcxx_mock::text_search::IndexConfig{.mappings = "TODO antepusic"};
    auto new_index = mgcxx_mock::text_search::Mock::create_index(index_name, index_config);
    index_[index_name] = new_index;
    return true;

    // TODO add documents to index
  }

  bool DropIndex(std::string index_name) {
    mgcxx_mock::text_search::Mock::drop_index(index_name);
    index_.erase(index_name);
    return true;
  }

  bool IndexExists(std::string index_name) { return index_.contains(index_name); }

  std::vector<Gid> Search(std::string index_name, std::string search_query) {
    auto input = mgcxx_mock::text_search::SearchInput{.search_query = search_query, .return_fields = {"metadata.gid"}};
    // Basic check for search fields in the query (Tantivy syntax delimits them with `:` to the right)
    if (search_query.find(":") == std::string::npos) {
      input.search_fields = {"data"};
    }

    std::vector<Gid> found_nodes;
    for (const auto &doc : mgcxx_mock::text_search::Mock::search(index_.at(index_name), input).docs) {
      found_nodes.push_back(storage::Gid::FromString(doc.data));
    }
    return found_nodes;
  }

  std::vector<std::string> ListIndices() {
    std::vector<std::string> ret;
    ret.reserve(index_.size());
    for (const auto &item : index_) {
      ret.push_back(item.first);
    }
    return ret;
  }

  uint64_t ApproximateVertexCount(std::string index_name) { return 10; }

  std::map<std::string, mgcxx_mock::text_search::IndexContext> index_;
  std::map<LabelId, std::string> label_to_index_;
};

}  // namespace memgraph::storage
