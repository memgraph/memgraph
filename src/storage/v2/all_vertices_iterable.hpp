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

#include "storage/v2/vertex_accessor.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class Storage;

class AllVerticesIterable final {
  utils::SkipList<Vertex>::Accessor vertices_accessor_;
  Storage *storage_;
  Transaction *transaction_;
  View view_;
  std::optional<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    utils::SkipList<Vertex>::Iterator it_;

   public:
    Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it);

    VertexAccessor const &operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const { return self_ == other.self_ && it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  /// Iterator that iterates over chunks of vertices for parallel processing.
  /// Each chunk can be processed in parallel by different threads.
  class ChunkIterator final {
    Storage *storage_;
    Transaction *transaction_;
    View view_;
    utils::SkipList<Vertex>::ChunkIterator it_;
    std::optional<VertexAccessor> vertex_;

   public:
    ChunkIterator(Storage *storage, Transaction *transaction, View view, utils::SkipList<Vertex>::ChunkIterator it);

    VertexAccessor const &operator*() const;

    ChunkIterator &operator++();

    bool operator==(const ChunkIterator &other) const { return it_ == other.it_; }

    bool operator!=(const ChunkIterator &other) const { return !(*this == other); }
  };

  /// Represents a chunk of vertices that can be processed in parallel.
  class Chunk final {
    Storage *storage_;
    Transaction *transaction_;
    View view_;
    utils::SkipList<Vertex>::ChunkIterator begin_it_;
    utils::SkipList<Vertex>::ChunkIterator end_it_;

   public:
    Chunk(Storage *storage, Transaction *transaction, View view, utils::SkipList<Vertex>::ChunkIterator begin,
          utils::SkipList<Vertex>::ChunkIterator end);

    ChunkIterator begin() const;
    ChunkIterator end() const;
  };

  /// Collection of chunks for parallel processing.
  using ChunkCollection = std::vector<Chunk>;

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, Storage *storage, Transaction *transaction,
                      View view)
      : vertices_accessor_(std::move(vertices_accessor)), storage_(storage), transaction_(transaction), view_(view) {}

  Iterator begin() { return {this, vertices_accessor_.begin()}; }
  Iterator end() { return {this, vertices_accessor_.end()}; }

  /// Creates chunks for parallel processing of vertices.
  /// Each chunk contains approximately equal number of elements.
  /// This method is thread-safe and can be called concurrently.
  ///
  /// @param num_chunks The number of chunks to create
  /// @return ChunkCollection containing the chunks
  ChunkCollection create_chunks(size_t num_chunks) const;
};

}  // namespace memgraph::storage
