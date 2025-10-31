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

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"

namespace memgraph::storage {

class EdgesChunkedIterable final {
  enum class Type {
    BY_EDGE_TYPE_IN_MEMORY_CHUNKED,
    BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED,
    BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED
  };

  Type type_;
  union {
    InMemoryEdgeTypeIndex::ChunkedIterable in_memory_chunked_edges_by_type_;
    InMemoryEdgeTypePropertyIndex::ChunkedIterable in_memory_chunked_edges_by_type_property_;
    InMemoryEdgePropertyIndex::ChunkedIterable in_memory_chunked_edges_by_property_;
  };

  void Destroy() noexcept;

 public:
  explicit EdgesChunkedIterable(InMemoryEdgeTypeIndex::ChunkedIterable);
  explicit EdgesChunkedIterable(InMemoryEdgeTypePropertyIndex::ChunkedIterable);
  explicit EdgesChunkedIterable(InMemoryEdgePropertyIndex::ChunkedIterable);

  EdgesChunkedIterable(const EdgesChunkedIterable &) = delete;
  EdgesChunkedIterable &operator=(const EdgesChunkedIterable &) = delete;

  EdgesChunkedIterable(EdgesChunkedIterable &&) noexcept;
  EdgesChunkedIterable &operator=(EdgesChunkedIterable &&) noexcept;

  ~EdgesChunkedIterable();

  class Iterator final {
    Type type_;
    union {
      InMemoryEdgeTypeIndex::ChunkedIterable::Iterator in_memory_chunked_by_type_it_;
      InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator in_memory_chunked_by_type_property_it_;
      InMemoryEdgePropertyIndex::ChunkedIterable::Iterator in_memory_chunked_by_property_it_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(InMemoryEdgeTypeIndex::ChunkedIterable::Iterator);
    explicit Iterator(InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator);
    explicit Iterator(InMemoryEdgePropertyIndex::ChunkedIterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);
    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    EdgeAccessor const &operator*() const;
    Iterator &operator++();
    bool operator==(const Iterator &other) const;
    bool operator!=(const Iterator &other) const;
  };

  class Chunk {
    Iterator begin_;
    Iterator end_;

   public:
    explicit Chunk(auto &&chunk) : begin_{chunk.begin()}, end_{chunk.end()} {}

    Iterator begin() { return begin_; }
    Iterator end() { return end_; }
  };

  Chunk get_chunk(size_t id);
  size_t size() const;
};

}  // namespace memgraph::storage
