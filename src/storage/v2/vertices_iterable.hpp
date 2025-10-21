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
#include "storage/v2/all_vertices_iterable.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"

namespace memgraph::storage {

class VerticesIterable final {
  enum class Type {
    ALL,
    BY_LABEL_IN_MEMORY,
    BY_LABEL_PROPERTY_IN_MEMORY,
    ALL_CHUNKED,
    BY_LABEL_IN_MEMORY_CHUNKED,
    BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED
  };

  Type type_;
  union {
    AllVerticesIterable all_vertices_;
    AllVerticesChunkedIterable all_chunked_vertices_;
    InMemoryLabelIndex::Iterable in_memory_vertices_by_label_;
    InMemoryLabelIndex::ChunkedIterable in_memory_chunked_vertices_by_label_;
    InMemoryLabelPropertyIndex::Iterable in_memory_vertices_by_label_property_;
    InMemoryLabelPropertyIndex::ChunkedIterable in_memory_chunked_vertices_by_label_property_;
  };

 public:
  explicit VerticesIterable(AllVerticesIterable);
  explicit VerticesIterable(AllVerticesChunkedIterable);
  explicit VerticesIterable(InMemoryLabelIndex::Iterable);
  explicit VerticesIterable(InMemoryLabelIndex::ChunkedIterable);
  explicit VerticesIterable(InMemoryLabelPropertyIndex::Iterable);
  explicit VerticesIterable(InMemoryLabelPropertyIndex::ChunkedIterable);

  VerticesIterable(const VerticesIterable &) = delete;
  VerticesIterable &operator=(const VerticesIterable &) = delete;

  VerticesIterable(VerticesIterable &&) noexcept;
  VerticesIterable &operator=(VerticesIterable &&) noexcept;

  ~VerticesIterable();

  class Iterator final {
    Type type_;
    union {
      AllVerticesIterable::Iterator all_it_;
      AllVerticesChunkedIterable::Iterator all_chunked_it_;
      InMemoryLabelIndex::Iterable::Iterator in_memory_by_label_it_;
      InMemoryLabelIndex::ChunkedIterable::Iterator in_memory_chunked_by_label_it_;
      InMemoryLabelPropertyIndex::Iterable::Iterator in_memory_by_label_property_it_;
      InMemoryLabelPropertyIndex::ChunkedIterable::Iterator in_memory_chunked_by_label_property_it_;
    };

    void Destroy() noexcept;

   public:
    using difference_type = std::ptrdiff_t;
    using value_type = VertexAccessor;

    explicit Iterator(AllVerticesIterable::Iterator);
    explicit Iterator(AllVerticesChunkedIterable::Iterator);
    explicit Iterator(InMemoryLabelIndex::Iterable::Iterator);
    explicit Iterator(InMemoryLabelIndex::ChunkedIterable::Iterator);
    explicit Iterator(InMemoryLabelPropertyIndex::Iterable::Iterator);
    explicit Iterator(InMemoryLabelPropertyIndex::ChunkedIterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);

    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    value_type const &operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const;
  };

  Iterator begin();
  Iterator end();
  Iterator begin(size_t);
  Iterator end(size_t);
  size_t size() const;
};

}  // namespace memgraph::storage
