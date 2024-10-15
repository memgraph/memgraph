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

#include "storage/v2/indices/vector_index.hpp"
#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "usearch/index.hpp"
#include "usearch/index_dense.hpp"

namespace unum {
namespace usearch {

template <>
struct unum::usearch::hash_gt<memgraph::storage::VectorIndexKey> {
  std::size_t operator()(memgraph::storage::VectorIndexKey const &element) const noexcept {
    return std::hash<uint64_t>{}(element.timestamp) ^ std::hash<memgraph::storage::Gid>{}(element.vertex->gid);
  }
};

template <>
struct unum::usearch::hash_gt<std::pair<uint64_t, uint64_t>> {
  std::size_t operator()(std::pair<uint64_t, uint64_t> const &element) const noexcept {
    return std::hash<uint64_t>{}(element.first) ^ std::hash<uint64_t>{}(element.second);
  }
};

}  // namespace usearch
}  // namespace unum

namespace memgraph::storage {

using mg_vector_index_t = unum::usearch::index_dense_gt<VectorIndexKey, unum::usearch::uint40_t>;

struct VectorIndex::Impl {
  Impl() = default;
  ~Impl() = default;

  std::map<LabelPropKey, mg_vector_index_t> indexes_;
  absl::flat_hash_map<std::string, LabelPropKey> index_to_label_prop_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() {}

void VectorIndex::CreateIndex(const std::string &index_name, std::vector<VectorIndexSpec> const &specs) {
  // TODO(davivek): Take a look under https://github.com/memgraph/cmake/blob/main/vs_usearch.cpp to see how to inject
  // custom key.
  // TODO(davivek): Parametrize everything (e.g. vector_size should be dynamic).
  int i = 0;  // just for testing
  for (const auto &spec : specs) {
    uint64_t vector_size = spec.config["size"];
    unum::usearch::metric_punned_t metric(vector_size, unum::usearch::metric_kind_t::l2sq_k,
                                          unum::usearch::scalar_kind_t::f32_k);
    const auto label = spec.label;
    const auto prop = spec.property;

    const auto label_prop = LabelPropKey{label, prop};
    pimpl->index_to_label_prop_.emplace(index_name + std::to_string(i), label_prop);
    pimpl->indexes_.emplace(label_prop, mg_vector_index_t::make(metric));
    pimpl->indexes_[label_prop].reserve(100);
    i++;
  }

  std::cout << "Created vector index" << std::endl;
}

void VectorIndex::AddNode(Vertex *vertex, uint64_t timestamp) {
  for (const auto label_id : vertex->labels) {
    for (const auto &[property_id, property_value] : vertex->properties.Properties()) {
      const auto label_prop = LabelPropKey{label_id, property_id};
      if (pimpl->indexes_.find(label_prop) != pimpl->indexes_.end()) {
        const auto key = VectorIndexKey{vertex, timestamp};
        const auto &list = property_value.ValueList();
        spdlog::trace("Adding node to vector index)");
        std::vector<float> vec;
        vec.reserve(list.size());
        for (const auto &value : list) {
          vec.push_back(static_cast<float>(value.ValueDouble()));
        }
        pimpl->indexes_[label_prop].add(key, vec.data());
      }
    }
  }
}

}  // namespace memgraph::storage
