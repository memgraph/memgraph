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

#include "usearch/index.hpp"
#include "usearch/index_dense.hpp"

#include "utils/logging.hpp"

namespace memgraph::storage {

using mg_vector_key_t = uint64_t;
using mg_vector_index_t = unum::usearch::index_dense_gt<mg_vector_key_t, unum::usearch::uint40_t>;

struct VectorIndex::Impl {
  Impl() = default;
  ~Impl() = default;

  mg_vector_index_t index;
};

VectorIndex::VectorIndex() : pimpl(std::make_unique<VectorIndex::Impl>()) {}
VectorIndex::~VectorIndex() {}

void VectorIndex::CreateIndex(const std::string &index_name) {
  // TODO(davivek): Take a look under https://github.com/memgraph/cmake/blob/main/vs_usearch.cpp to see how to inject
  // custom key.
  // TODO(davivek): Parametrize everything (e.g. vector_size should be dynamic).
  uint64_t vector_size = 1000;
  unum::usearch::metric_punned_t metric(vector_size, unum::usearch::metric_kind_t::l2sq_k,
                                        unum::usearch::scalar_kind_t::f32_k);
  pimpl->index = std::move(mg_vector_index_t::make(metric));
  LOG_FATAL("Not Yet Implemented: Vector Index");
}

}  // namespace memgraph::storage
