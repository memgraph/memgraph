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
#include "utils/algorithm.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_string(experimental_vector_indexes, "",
                     "Enables vector search indexes on nodes with Label and property specified in the "
                     "IndexName__Label1__property1__{JSON_config};IndexName__Label2__property2 format.");

namespace unum {
namespace usearch {

template <>
struct unum::usearch::hash_gt<memgraph::storage::VectorIndexKey> {
  std::size_t operator()(memgraph::storage::VectorIndexKey const &element) const noexcept {
    return std::hash<uint64_t>{}(element.start_timestamp) ^ std::hash<memgraph::storage::Gid>{}(element.vertex->gid);
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

// The `Impl` structure implements the underlying functionality of the `VectorIndex` class.
// It uses the PIMPL (Pointer to Implementation) idiom to separate the interface of `VectorIndex`
// from its implementation, making it easier to maintain, extend, and hide implementation details.
struct VectorIndex::Impl {
  Impl() = default;
  ~Impl() = default;

  // The `index_` member is a map that associates a `LabelPropKey` (a combination of label and property)
  // with the actual vector index (`mg_vector_index_t`). This allows us to store multiple vector indexes
  // based on different labels and properties.
  std::map<LabelPropKey, mg_vector_index_t> index_;

  // The `index_name_to_label_prop_` is a hash map that maps an index name (as a string) to the corresponding
  // `LabelPropKey`. This allows the system to quickly resolve an index name to the specific label and property
  // associated with that index, enabling easy lookup and management of indexes by name.
  absl::flat_hash_map<std::string, LabelPropKey> index_name_to_label_prop_;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<Impl>()) {}
VectorIndex::~VectorIndex() {}

void VectorIndex::CreateIndex(const VectorIndexSpec &spec) {
  // TODO(DavIvek): Take a look under https://github.com/memgraph/cmake/blob/main/vs_usearch.cpp to see how to inject
  // custom key.
  // TODO(DavIvek): Parametrize everything (e.g. vector_size should be dynamic).
  uint64_t vector_size = spec.config["size"];
  unum::usearch::metric_punned_t metric(vector_size, unum::usearch::metric_kind_t::l2sq_k,
                                        unum::usearch::scalar_kind_t::f32_k);

  const auto label_prop = LabelPropKey{spec.label, spec.property};
  pimpl->index_name_to_label_prop_.emplace(spec.index_name, label_prop);
  pimpl->index_.emplace(label_prop, mg_vector_index_t::make(metric));
  pimpl->index_[label_prop].reserve(100);

  spdlog::trace("Created vector index " + spec.index_name);
}

void VectorIndex::AddNode(Vertex *vertex, uint64_t timestamp) {
  for (auto &[key, index] : pimpl->index_) {
    const auto label_id = key.label();
    const auto property_id = key.property();
    if (utils::Contains(vertex->labels, label_id) && vertex->properties.HasProperty(property_id)) {
      const auto key = VectorIndexKey{vertex, timestamp};
      const auto &property_value = vertex->properties.GetProperty(property_id);
      const auto &list = property_value.ValueList();
      spdlog::trace("Adding node to vector index");

      // TODO(DavIvek): This is a temporary solution. Maybe we will need to modify PropertyValues so we don't need to
      // call ValueDouble for each element.
      // TODO (DavIvek): Does usearch have some specific type based on the quantization,
      // from the config we could maybe deduce the right type (having that at runtime could be a problem)?
      std::vector<float> vec(list.size());
      std::ranges::transform(list, vec.begin(),
                             [](const auto &value) { return static_cast<float>(value.ValueDouble()); });
      index.add(key, vec.data());
    }
  }
}

std::size_t VectorIndex::Size(const std::string &index_name) {
  const auto label_prop = pimpl->index_name_to_label_prop_.at(index_name);
  return pimpl->index_.at(label_prop).size();
}

std::vector<std::string> VectorIndex::ListAllIndices() {
  std::vector<std::string> indices;
  for (const auto &[index_name, _] : pimpl->index_name_to_label_prop_) {
    indices.push_back(index_name);
  }
  return indices;
}

}  // namespace memgraph::storage
