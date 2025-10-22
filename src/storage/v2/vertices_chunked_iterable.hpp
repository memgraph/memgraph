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

#include "storage/v2/all_vertices_chunked_iterable.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"

namespace memgraph::storage {

class VerticesChunkedIterable final {
  enum class Type { ALL_CHUNKED, BY_LABEL_IN_MEMORY_CHUNKED, BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED };

  Type type_;
  union {
    AllVerticesChunkedIterable all_chunked_vertices_;
    InMemoryLabelIndex::ChunkedIterable in_memory_chunked_vertices_by_label_;
    InMemoryLabelPropertyIndex::ChunkedIterable in_memory_chunked_vertices_by_label_property_;
  };

 public:
  explicit VerticesChunkedIterable(AllVerticesChunkedIterable);
  explicit VerticesChunkedIterable(InMemoryLabelIndex::ChunkedIterable);
  explicit VerticesChunkedIterable(InMemoryLabelPropertyIndex::ChunkedIterable);

  VerticesChunkedIterable(const VerticesChunkedIterable &) = delete;
  VerticesChunkedIterable &operator=(const VerticesChunkedIterable &) = delete;

  VerticesChunkedIterable(VerticesChunkedIterable &&) noexcept;
  VerticesChunkedIterable &operator=(VerticesChunkedIterable &&) noexcept;

  ~VerticesChunkedIterable();

  class Iterator final {
    Type type_;
    union {
      AllVerticesChunkedIterable::Iterator all_chunked_it_;
      InMemoryLabelIndex::ChunkedIterable::Iterator in_memory_chunked_by_label_it_;
      InMemoryLabelPropertyIndex::ChunkedIterable::Iterator in_memory_chunked_by_label_property_it_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(AllVerticesChunkedIterable::Iterator);
    explicit Iterator(InMemoryLabelIndex::ChunkedIterable::Iterator);
    explicit Iterator(InMemoryLabelPropertyIndex::ChunkedIterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);

    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    VertexAccessor const &operator*() const;

    Iterator &operator++();

    explicit operator bool() const {
      switch (type_) {
        case Type::ALL_CHUNKED:
          return bool(all_chunked_it_);
        case Type::BY_LABEL_IN_MEMORY_CHUNKED:
          return bool(in_memory_chunked_by_label_it_);
        case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
          return bool(in_memory_chunked_by_label_property_it_);
      }
    }
  };

  Iterator get_iterator(size_t id) {
    switch (type_) {
      case Type::ALL_CHUNKED:
        return Iterator(all_chunked_vertices_.get_iterator(id));
      case Type::BY_LABEL_IN_MEMORY_CHUNKED:
        return Iterator(in_memory_chunked_vertices_by_label_.get_iterator(id));
      case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
        return Iterator(in_memory_chunked_vertices_by_label_property_.get_iterator(id));
    }
  }

  size_t size() const;
};

}  // namespace memgraph::storage
