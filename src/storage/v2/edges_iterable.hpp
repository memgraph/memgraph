// Copyright 2024 Memgraph Ltd.
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
#include "storage/v2/inmemory/edge_type_property_index.hpp"

namespace memgraph::storage {

class InMemoryEdgeTypeIndex;

class EdgesIterable final {
  enum class Type { BY_EDGE_TYPE_IN_MEMORY, BY_EDGE_TYPE_PROPERTY_IN_MEMORY };

  Type type_;
  union {
    InMemoryEdgeTypeIndex::Iterable in_memory_edges_by_edge_type_;
    InMemoryEdgeTypePropertyIndex::Iterable in_memory_edges_by_edge_type_property_;
  };

  void Destroy() noexcept;

 public:
  explicit EdgesIterable(InMemoryEdgeTypeIndex::Iterable);
  explicit EdgesIterable(InMemoryEdgeTypePropertyIndex::Iterable);

  EdgesIterable(const EdgesIterable &) = delete;
  EdgesIterable &operator=(const EdgesIterable &) = delete;

  EdgesIterable(EdgesIterable &&) noexcept;
  EdgesIterable &operator=(EdgesIterable &&) noexcept;

  ~EdgesIterable();

  class Iterator final {
    Type type_;
    union {
      InMemoryEdgeTypeIndex::Iterable::Iterator in_memory_edges_by_edge_type_;
      InMemoryEdgeTypePropertyIndex::Iterable::Iterator in_memory_edges_by_edge_type_property_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(InMemoryEdgeTypeIndex::Iterable::Iterator);
    explicit Iterator(InMemoryEdgeTypePropertyIndex::Iterable::Iterator);

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
