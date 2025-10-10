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

#include "storage/v2/all_vertices_iterable.hpp"

namespace memgraph::storage {

auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::Iterator it, utils::SkipList<Vertex>::Iterator end,
                            std::optional<VertexAccessor> *vertex, Storage *storage, Transaction *tx, View view) {
  while (it != end) {
    if (VertexAccessor::IsVisible(&*it, tx, view)) [[likely]] {
      vertex->emplace(&*it, storage, tx);
      break;
    }
    ++it;
  }
  return it;
}

AllVerticesIterable::Iterator::Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it)
    : self_(self),
      it_(AdvanceToVisibleVertex(it, self->vertices_accessor_.end(), &self->vertex_, self->storage_, self->transaction_,
                                 self->view_)) {}

VertexAccessor const &AllVerticesIterable::Iterator::operator*() const { return *self_->vertex_; }

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  it_ = AdvanceToVisibleVertex(std::next(it_), self_->vertices_accessor_.end(), &self_->vertex_, self_->storage_,
                               self_->transaction_, self_->view_);
  return *this;
}

AllVerticesIterable::ChunkIterator::ChunkIterator(Storage *storage, Transaction *transaction, View view,
                                                  utils::SkipList<Vertex>::ChunkIterator it)
    : storage_(storage), transaction_(transaction), view_(view), it_(it) {
  // Advance to first visible vertex in the chunk
  if (it_ != utils::SkipList<Vertex>::ChunkIterator{}) {
    // Find first visible vertex
    while (it_ != utils::SkipList<Vertex>::ChunkIterator{} && !VertexAccessor::IsVisible(&*it_, transaction_, view_)) {
      ++it_;
    }

    if (it_ != utils::SkipList<Vertex>::ChunkIterator{}) {
      vertex_.emplace(const_cast<Vertex *>(&*it_), storage_, transaction_);
    } else {
      vertex_.reset();
    }
  } else {
    // Iterator is at end, no vertex to set
    vertex_.reset();
  }
}

VertexAccessor const &AllVerticesIterable::ChunkIterator::operator*() const { return *vertex_; }

AllVerticesIterable::ChunkIterator &AllVerticesIterable::ChunkIterator::operator++() {
  ++it_;
  // Advance to next visible vertex
  while (it_ != utils::SkipList<Vertex>::ChunkIterator{} && !VertexAccessor::IsVisible(&*it_, transaction_, view_)) {
    ++it_;
  }

  if (it_ != utils::SkipList<Vertex>::ChunkIterator{}) {
    vertex_.emplace(const_cast<Vertex *>(&*it_), storage_, transaction_);
  } else {
    vertex_.reset();
  }

  return *this;
}

AllVerticesIterable::Chunk::Chunk(Storage *storage, Transaction *transaction, View view,
                                  utils::SkipList<Vertex>::ChunkIterator begin,
                                  utils::SkipList<Vertex>::ChunkIterator end)
    : storage_(storage), transaction_(transaction), view_(view), begin_it_(begin), end_it_(end) {}

AllVerticesIterable::ChunkIterator AllVerticesIterable::Chunk::begin() const {
  return {storage_, transaction_, view_, begin_it_};
}

AllVerticesIterable::ChunkIterator AllVerticesIterable::Chunk::end() const {
  return {storage_, transaction_, view_, end_it_};
}

AllVerticesIterable::ChunkCollection AllVerticesIterable::create_chunks(size_t num_chunks) const {
  auto skiplist_chunks = vertices_accessor_.create_chunks(num_chunks);
  ChunkCollection chunks;
  chunks.reserve(skiplist_chunks.size());

  for (const auto &skiplist_chunk : skiplist_chunks) {
    chunks.emplace_back(storage_, transaction_, view_, skiplist_chunk.begin(), skiplist_chunk.end());
  }

  return chunks;
}

}  // namespace memgraph::storage
