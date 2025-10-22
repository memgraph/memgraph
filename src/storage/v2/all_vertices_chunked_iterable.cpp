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

#include "storage/v2/all_vertices_chunked_iterable.hpp"

namespace memgraph::storage {

namespace {
auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::ChunkedIterator it, std::optional<VertexAccessor> *vertex,
                            Storage *storage, Transaction *tx, View view) {
  while (it) {
    if (VertexAccessor::IsVisible(&*it, tx, view)) [[likely]] {
      vertex->emplace(&*it, storage, tx);
      break;
    }
    ++it;
  }
  return it;
}
}  // namespace

AllVerticesChunkedIterable::Iterator::Iterator(AllVerticesChunkedIterable *self, std::optional<VertexAccessor> *cache,
                                               utils::SkipList<Vertex>::ChunkedIterator it)
    : self_(self),
      cache_{cache},
      it_(AdvanceToVisibleVertex(it, cache_, self->storage_, self->transaction_, self->view_)) {}

VertexAccessor const &AllVerticesChunkedIterable::Iterator::operator*() const { return cache_->value(); }

AllVerticesChunkedIterable::Iterator &AllVerticesChunkedIterable::Iterator::operator++() {
  it_ = AdvanceToVisibleVertex(it_ ? ++it_ : it_, cache_, self_->storage_, self_->transaction_, self_->view_);
  return *this;
}

}  // namespace memgraph::storage
