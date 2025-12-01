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

#pragma once

#include <optional>
#include "storage/v2/vertex_accessor.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class Storage;

class AllVerticesChunkedIterable final {
  utils::SkipList<Vertex>::Accessor vertices_accessor_;  // Main pin accessor
  Storage *storage_;                                     // Read only
  Transaction *transaction_;                             // Read only
  View view_;
  utils::SkipList<Vertex>::ChunkCollection chunks_;

 public:
  class Iterator final {
    AllVerticesChunkedIterable *self_{nullptr};
    std::optional<VertexAccessor> cache_{std::nullopt};
    utils::SkipList<Vertex>::ChunkedIterator it_;

   public:
    Iterator(AllVerticesChunkedIterable *self, utils::SkipList<Vertex>::Chunk &chunk);
    explicit Iterator(utils::SkipList<Vertex>::ChunkedIterator end);

    VertexAccessor const &operator*() const;
    Iterator &operator++();

    bool operator==(const Iterator &other) const { return it_ == other.it_; }
    bool operator!=(const Iterator &other) const { return it_ != other.it_; }
  };

  AllVerticesChunkedIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, size_t num_chunks, Storage *storage,
                             Transaction *transaction, View view)
      : vertices_accessor_(std::move(vertices_accessor)),
        storage_(storage),
        transaction_(transaction),
        view_(view),
        chunks_{vertices_accessor_.create_chunks(num_chunks)} {}

  class Chunk {
    Iterator begin_;
    Iterator end_;

   public:
    Chunk(AllVerticesChunkedIterable *self, utils::SkipList<Vertex>::Chunk &chunk)
        : begin_{self, chunk}, end_{chunk.end()} {}

    Iterator begin() { return begin_; }
    Iterator end() { return end_; }
  };

  Chunk get_chunk(size_t id) { return {this, chunks_[id]}; }
  size_t size() const { return chunks_.size(); }
};

}  // namespace memgraph::storage
