// Copyright 2026 Memgraph Ltd.
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
auto AdvanceToVisibleVertex(utils::SkipListDb<Vertex>::ChunkedIterator it,
                            utils::SkipListDb<Vertex>::ChunkedIterator end, std::optional<VertexAccessor> *vertex,
                            Storage *storage, Transaction *tx, View view, Gid max_gid) {
  while (it != end) [[likely]] {
    // chunks are gid-ordered → no later entry in this chunk can be in scope either.
    if (it->gid >= max_gid) [[unlikely]] {
      return end;
    }
    if (VertexAccessor::IsVisible(&*it, tx, view)) [[likely]] {
      vertex->emplace(&*it, storage, tx);
      break;
    }
    ++it;
  }
  return it;
}

auto AdvanceToVisibleVertex(utils::SkipListDb<Vertex>::ChunkedIterator it, std::optional<VertexAccessor> *vertex,
                            Storage *storage, Transaction *tx, View view, Gid max_gid) {
  // NOTE: Using the skiplist end here to not store the end iterator in the class
  // The higher level != end will still be correct
  return AdvanceToVisibleVertex(it, utils::SkipListDb<Vertex>::ChunkedIterator{}, vertex, storage, tx, view, max_gid);
}
}  // namespace

AllVerticesChunkedIterable::Iterator::Iterator(AllVerticesChunkedIterable *self,
                                               utils::SkipListDb<Vertex>::Chunk &chunk)
    : self_(self),
      it_(AdvanceToVisibleVertex(chunk.begin(), chunk.end(), &cache_, self->storage_, self->transaction_, self->view_,
                                 self->max_gid_)) {}

AllVerticesChunkedIterable::Iterator::Iterator(utils::SkipListDb<Vertex>::ChunkedIterator end) : it_(end) {}

VertexAccessor const &AllVerticesChunkedIterable::Iterator::operator*() const { return cache_.value(); }

AllVerticesChunkedIterable::Iterator &AllVerticesChunkedIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, &cache_, self_->storage_, self_->transaction_, self_->view_, self_->max_gid_);
  return *this;
}

}  // namespace memgraph::storage
