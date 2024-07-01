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

#include "storage/v2/edges_iterable.hpp"

namespace memgraph::storage {

EdgesIterable::EdgesIterable(InMemoryEdgeTypeIndex::Iterable edges) : type_(Type::BY_EDGE_TYPE_IN_MEMORY) {
  new (&in_memory_edges_by_edge_type_) InMemoryEdgeTypeIndex::Iterable(std::move(edges));
}

EdgesIterable::EdgesIterable(InMemoryEdgeTypePropertyIndex::Iterable edges)
    : type_(Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY) {
  new (&in_memory_edges_by_edge_type_property_) InMemoryEdgeTypePropertyIndex::Iterable(std::move(edges));
}

EdgesIterable::EdgesIterable(EdgesIterable &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_)
          InMemoryEdgeTypeIndex::Iterable(std::move(other.in_memory_edges_by_edge_type_));
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_property_)
          InMemoryEdgeTypePropertyIndex::Iterable(std::move(other.in_memory_edges_by_edge_type_property_));
      break;
  }
}

EdgesIterable &EdgesIterable::operator=(EdgesIterable &&other) noexcept {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_)
          InMemoryEdgeTypeIndex::Iterable(std::move(other.in_memory_edges_by_edge_type_));
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_property_)
          InMemoryEdgeTypePropertyIndex::Iterable(std::move(other.in_memory_edges_by_edge_type_property_));
      break;
  }
  return *this;
}

EdgesIterable::~EdgesIterable() { Destroy(); }

void EdgesIterable::Destroy() noexcept {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      in_memory_edges_by_edge_type_.InMemoryEdgeTypeIndex::Iterable::~Iterable();
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      in_memory_edges_by_edge_type_property_.InMemoryEdgeTypePropertyIndex::Iterable::~Iterable();
      break;
  }
}

EdgesIterable::Iterator EdgesIterable::begin() {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      return Iterator(in_memory_edges_by_edge_type_.begin());
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      return Iterator(in_memory_edges_by_edge_type_property_.begin());
  }
}

EdgesIterable::Iterator EdgesIterable::end() {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      return Iterator(in_memory_edges_by_edge_type_.end());
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      return Iterator(in_memory_edges_by_edge_type_property_.end());
  }
}

EdgesIterable::Iterator::Iterator(InMemoryEdgeTypeIndex::Iterable::Iterator it) : type_(Type::BY_EDGE_TYPE_IN_MEMORY) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_edges_by_edge_type_) InMemoryEdgeTypeIndex::Iterable::Iterator(std::move(it));
}

EdgesIterable::Iterator::Iterator(InMemoryEdgeTypePropertyIndex::Iterable::Iterator it)
    : type_(Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_edges_by_edge_type_property_) InMemoryEdgeTypePropertyIndex::Iterable::Iterator(std::move(it));
}

EdgesIterable::Iterator::Iterator(const EdgesIterable::Iterator &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_)
          InMemoryEdgeTypeIndex::Iterable::Iterator(other.in_memory_edges_by_edge_type_);
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_property_)
          InMemoryEdgeTypePropertyIndex::Iterable::Iterator(other.in_memory_edges_by_edge_type_property_);
      break;
  }
}

// NOLINTNEXTLINE(cert-oop54-cpp)
EdgesIterable::Iterator &EdgesIterable::Iterator::operator=(const EdgesIterable::Iterator &other) {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_)
          InMemoryEdgeTypeIndex::Iterable::Iterator(other.in_memory_edges_by_edge_type_);
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_property_)
          InMemoryEdgeTypePropertyIndex::Iterable::Iterator(other.in_memory_edges_by_edge_type_property_);
      break;
  }
  return *this;
}

EdgesIterable::Iterator::Iterator(EdgesIterable::Iterator &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_)
          // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
          InMemoryEdgeTypeIndex::Iterable::Iterator(std::move(other.in_memory_edges_by_edge_type_));
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_property_)
          // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
          InMemoryEdgeTypePropertyIndex::Iterable::Iterator(std::move(other.in_memory_edges_by_edge_type_property_));
      break;
  }
}

EdgesIterable::Iterator &EdgesIterable::Iterator::operator=(EdgesIterable::Iterator &&other) noexcept {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_)
          // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
          InMemoryEdgeTypeIndex::Iterable::Iterator(std::move(other.in_memory_edges_by_edge_type_));
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      new (&in_memory_edges_by_edge_type_property_)
          // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
          InMemoryEdgeTypePropertyIndex::Iterable::Iterator(std::move(other.in_memory_edges_by_edge_type_property_));
      break;
  }
  return *this;
}

EdgesIterable::Iterator::~Iterator() { Destroy(); }

void EdgesIterable::Iterator::Destroy() noexcept {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      in_memory_edges_by_edge_type_.InMemoryEdgeTypeIndex::Iterable::Iterator::~Iterator();
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      in_memory_edges_by_edge_type_property_.InMemoryEdgeTypePropertyIndex::Iterable::Iterator::~Iterator();
      break;
  }
}

EdgeAccessor const &EdgesIterable::Iterator::operator*() const {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      return *in_memory_edges_by_edge_type_;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      return *in_memory_edges_by_edge_type_property_;
  }
}

EdgesIterable::Iterator &EdgesIterable::Iterator::operator++() {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      ++in_memory_edges_by_edge_type_;
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      ++in_memory_edges_by_edge_type_property_;
      break;
  }
  return *this;
}

bool EdgesIterable::Iterator::operator==(const Iterator &other) const {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY:
      return in_memory_edges_by_edge_type_ == other.in_memory_edges_by_edge_type_;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY:
      return in_memory_edges_by_edge_type_property_ == other.in_memory_edges_by_edge_type_property_;
  }
}

}  // namespace memgraph::storage
