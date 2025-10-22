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
  std::vector<std::optional<VertexAccessor>> chunk_cache_;

 public:
  class Iterator final {
    AllVerticesChunkedIterable *self_;
    std::optional<VertexAccessor> *cache_;
    utils::SkipList<Vertex>::ChunkedIterator it_;

   public:
    Iterator(AllVerticesChunkedIterable *self, std::optional<VertexAccessor> *cache,
             utils::SkipList<Vertex>::ChunkedIterator it);
    VertexAccessor const &operator*() const;
    Iterator &operator++();
    explicit operator bool() const { return bool(it_); }
  };

  AllVerticesChunkedIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, size_t num_chunks, Storage *storage,
                             Transaction *transaction, View view)
      : vertices_accessor_(std::move(vertices_accessor)),
        storage_(storage),
        transaction_(transaction),
        view_(view),
        chunks_{vertices_accessor_.create_chunks(num_chunks)},
        chunk_cache_(chunks_.size(), std::nullopt) {}

  Iterator get_iterator(size_t id) { return {this, &chunk_cache_[id], chunks_[id]}; }
  size_t size() const { return chunks_.size(); }
};

}  // namespace memgraph::storage
