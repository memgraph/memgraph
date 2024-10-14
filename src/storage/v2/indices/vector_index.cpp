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

  std::map<std::pair<LabelId, PropertyId>, mg_vector_index_t> indexes_;
  absl::flat_hash_map<std::string, std::pair<LabelId, PropertyId>> index_to_label_prop_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() {}

void VectorIndex::CreateIndex(const std::string &index_name, std::vector<VectorIndexSpec> const &specs) {
  // TODO(davivek): Take a look under https://github.com/memgraph/cmake/blob/main/vs_usearch.cpp to see how to inject
  // custom key.
  // TODO(davivek): Parametrize everything (e.g. vector_size should be dynamic).

  for (const auto &spec : specs) {
    uint64_t vector_size = spec.config["size"];
    unum::usearch::metric_punned_t metric(vector_size, unum::usearch::metric_kind_t::l2sq_k,
                                          unum::usearch::scalar_kind_t::f32_k);
    const auto label = spec.label;
    const auto prop = spec.property;

    pimpl->index_to_label_prop_[index_name] = std::make_pair(label, prop);
    pimpl->indexes_[std::make_pair(label, prop)] = std::move(mg_vector_index_t::make(metric));
  }
}

}  // namespace memgraph::storage
