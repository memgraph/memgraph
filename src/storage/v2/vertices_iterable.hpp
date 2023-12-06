// Copyright 2023 Memgraph Ltd.
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

#include "storage/v2/all_vertices_iterable.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"

namespace memgraph::storage {

class InMemoryEdgeTypeIndex;

class VerticesIterable final {
  enum class Type { ALL, BY_LABEL_IN_MEMORY, BY_LABEL_PROPERTY_IN_MEMORY };

  Type type_;
  union {
    AllVerticesIterable all_vertices_;
    InMemoryLabelIndex::Iterable in_memory_vertices_by_label_;
    InMemoryLabelPropertyIndex::Iterable in_memory_vertices_by_label_property_;
  };

 public:
  explicit VerticesIterable(AllVerticesIterable);
  explicit VerticesIterable(InMemoryLabelIndex::Iterable);
  explicit VerticesIterable(InMemoryLabelPropertyIndex::Iterable);

  VerticesIterable(const VerticesIterable &) = delete;
  VerticesIterable &operator=(const VerticesIterable &) = delete;

  VerticesIterable(VerticesIterable &&) noexcept;
  VerticesIterable &operator=(VerticesIterable &&) noexcept;

  ~VerticesIterable();

  class Iterator final {
    Type type_;
    union {
      AllVerticesIterable::Iterator all_it_;
      InMemoryLabelIndex::Iterable::Iterator in_memory_by_label_it_;
      InMemoryLabelPropertyIndex::Iterable::Iterator in_memory_by_label_property_it_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(AllVerticesIterable::Iterator);
    explicit Iterator(InMemoryLabelIndex::Iterable::Iterator);
    explicit Iterator(InMemoryLabelPropertyIndex::Iterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);

    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    VertexAccessor const &operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const;
    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  Iterator begin();
  Iterator end();
};

class EdgesIterable final {
  enum class Type { BY_EDGE_TYPE_IN_MEMORY };

  Type type_;
  union {
    InMemoryEdgeTypeIndex::Iterable in_memory_edges_by_edge_type_;
  };

 public:
  explicit EdgesIterable(InMemoryEdgeTypeIndex::Iterable);

  EdgesIterable(const EdgesIterable &) = delete;
  EdgesIterable &operator=(const EdgesIterable &) = delete;

  EdgesIterable(EdgesIterable &&) noexcept;
  EdgesIterable &operator=(EdgesIterable &&) noexcept;

  ~EdgesIterable();

  class Iterator final {
    Type type_;
    union {
      InMemoryEdgeTypeIndex::Iterable::Iterator in_memory_edges_by_edge_type_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(InMemoryEdgeTypeIndex::Iterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);

    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    EdgeAccessor const &operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const;
    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  Iterator begin();
  Iterator end();
};

}  // namespace memgraph::storage
