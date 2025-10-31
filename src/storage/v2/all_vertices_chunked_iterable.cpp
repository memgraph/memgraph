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
auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::ChunkedIterator it, utils::SkipList<Vertex>::ChunkedIterator end,
                            std::optional<VertexAccessor> *vertex, Storage *storage, Transaction *tx, View view) {
  while (it != end) [[likely]] {
      if (VertexAccessor::IsVisible(&*it, tx, view)) [[likely]] {
        vertex->emplace(&*it, storage, tx);
        break;
      }
      ++it;
    }
  return it;
}

auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::ChunkedIterator it, std::optional<VertexAccessor> *vertex,
                            Storage *storage, Transaction *tx, View view) {
  // NOTE: Using the skiplist end here to not store the end iterator in the class
  // The higher level != end will still be correct
  return AdvanceToVisibleVertex(it, utils::SkipList<Vertex>::ChunkedIterator{}, vertex, storage, tx, view);
}
}  // namespace

AllVerticesChunkedIterable::Iterator::Iterator(AllVerticesChunkedIterable *self, std::optional<VertexAccessor> *cache,
                                               utils::SkipList<Vertex>::Chunk &chunk)
    : self_(self),
      cache_{cache},
      it_(AdvanceToVisibleVertex(chunk.begin(), chunk.end(), cache_, self->storage_, self->transaction_, self->view_)) {
}

AllVerticesChunkedIterable::Iterator::Iterator(utils::SkipList<Vertex>::ChunkedIterator end) : it_(end) {}

VertexAccessor const &AllVerticesChunkedIterable::Iterator::operator*() const { return cache_->value(); }

AllVerticesChunkedIterable::Iterator &AllVerticesChunkedIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, cache_, self_->storage_, self_->transaction_, self_->view_);
  return *this;
}

}  // namespace memgraph::storage
