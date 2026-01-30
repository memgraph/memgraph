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

#include "storage/v2/edges_chunked_iterable.hpp"

namespace memgraph::storage {

EdgesChunkedIterable::EdgesChunkedIterable(InMemoryEdgeTypeIndex::ChunkedIterable edges)
    : type_(Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED) {
  new (&in_memory_chunked_edges_by_type_) InMemoryEdgeTypeIndex::ChunkedIterable(std::move(edges));
}

EdgesChunkedIterable::EdgesChunkedIterable(InMemoryEdgeTypePropertyIndex::ChunkedIterable edges)
    : type_(Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED) {
  new (&in_memory_chunked_edges_by_type_property_) InMemoryEdgeTypePropertyIndex::ChunkedIterable(std::move(edges));
}

EdgesChunkedIterable::EdgesChunkedIterable(InMemoryEdgePropertyIndex::ChunkedIterable edges)
    : type_(Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED) {
  new (&in_memory_chunked_edges_by_property_) InMemoryEdgePropertyIndex::ChunkedIterable(std::move(edges));
}

EdgesChunkedIterable::EdgesChunkedIterable(EdgesChunkedIterable &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_edges_by_type_)
          InMemoryEdgeTypeIndex::ChunkedIterable(std::move(other.in_memory_chunked_edges_by_type_));
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_edges_by_type_property_)
          InMemoryEdgeTypePropertyIndex::ChunkedIterable(std::move(other.in_memory_chunked_edges_by_type_property_));
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_edges_by_property_)
          InMemoryEdgePropertyIndex::ChunkedIterable(std::move(other.in_memory_chunked_edges_by_property_));
      break;
  }
}

void EdgesChunkedIterable::Destroy() noexcept {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      in_memory_chunked_edges_by_type_.InMemoryEdgeTypeIndex::ChunkedIterable::~ChunkedIterable();
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_edges_by_type_property_.InMemoryEdgeTypePropertyIndex::ChunkedIterable::~ChunkedIterable();
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_edges_by_property_.InMemoryEdgePropertyIndex::ChunkedIterable::~ChunkedIterable();
      break;
  }
}

EdgesChunkedIterable &EdgesChunkedIterable::operator=(EdgesChunkedIterable &&other) noexcept {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_edges_by_type_)
          InMemoryEdgeTypeIndex::ChunkedIterable(std::move(other.in_memory_chunked_edges_by_type_));
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_edges_by_type_property_)
          InMemoryEdgeTypePropertyIndex::ChunkedIterable(std::move(other.in_memory_chunked_edges_by_type_property_));
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_edges_by_property_)
          InMemoryEdgePropertyIndex::ChunkedIterable(std::move(other.in_memory_chunked_edges_by_property_));
      break;
  }
  return *this;
}

EdgesChunkedIterable::~EdgesChunkedIterable() {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      in_memory_chunked_edges_by_type_.InMemoryEdgeTypeIndex::ChunkedIterable::~ChunkedIterable();
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_edges_by_type_property_.InMemoryEdgeTypePropertyIndex::ChunkedIterable::~ChunkedIterable();
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_edges_by_property_.InMemoryEdgePropertyIndex::ChunkedIterable::~ChunkedIterable();
      break;
  }
}

EdgesChunkedIterable::Chunk EdgesChunkedIterable::get_chunk(size_t id) {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      return EdgesChunkedIterable::Chunk{in_memory_chunked_edges_by_type_.get_chunk(id)};
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      return EdgesChunkedIterable::Chunk{in_memory_chunked_edges_by_type_property_.get_chunk(id)};
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      return EdgesChunkedIterable::Chunk{in_memory_chunked_edges_by_property_.get_chunk(id)};
  }
}

size_t EdgesChunkedIterable::size() const {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      return in_memory_chunked_edges_by_type_.size();
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_edges_by_type_property_.size();
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_edges_by_property_.size();
  }
}

EdgesChunkedIterable::Iterator::Iterator(InMemoryEdgeTypeIndex::ChunkedIterable::Iterator it)
    : type_(Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_chunked_by_type_it_) InMemoryEdgeTypeIndex::ChunkedIterable::Iterator(std::move(it));
}

EdgesChunkedIterable::Iterator::Iterator(InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator it)
    : type_(Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_chunked_by_type_property_it_) InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator(std::move(it));
}

EdgesChunkedIterable::Iterator::Iterator(InMemoryEdgePropertyIndex::ChunkedIterable::Iterator it)
    : type_(Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_chunked_by_property_it_) InMemoryEdgePropertyIndex::ChunkedIterable::Iterator(std::move(it));
}

EdgesChunkedIterable::Iterator::Iterator(const EdgesChunkedIterable::Iterator &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_type_it_)
          InMemoryEdgeTypeIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_it_);
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_type_property_it_)
          InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_property_it_);
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_property_it_)
          InMemoryEdgePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_property_it_);
      break;
  }
}

// NOLINTNEXTLINE(cert-oop54-cpp)
EdgesChunkedIterable::Iterator &EdgesChunkedIterable::Iterator::operator=(const EdgesChunkedIterable::Iterator &other) {
  if (this != &other) {
    Destroy();
    type_ = other.type_;
    switch (other.type_) {
      case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_type_it_)
            InMemoryEdgeTypeIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_it_);
        break;
      case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_type_property_it_)
            InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_property_it_);
        break;
      case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_property_it_)
            InMemoryEdgePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_property_it_);
        break;
    }
  }
  return *this;
}

EdgesChunkedIterable::Iterator::Iterator(EdgesChunkedIterable::Iterator &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_type_it_)
          InMemoryEdgeTypeIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_it_);
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_type_property_it_)
          InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_property_it_);
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_property_it_)
          InMemoryEdgePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_property_it_);
      break;
  }
}

// NOLINTNEXTLINE(cert-oop54-cpp)
EdgesChunkedIterable::Iterator &EdgesChunkedIterable::Iterator::operator=(
    EdgesChunkedIterable::Iterator &&other) noexcept {
  if (this != &other) {
    Destroy();
    type_ = other.type_;
    switch (other.type_) {
      case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_type_it_)
            InMemoryEdgeTypeIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_it_);
        break;
      case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_type_property_it_)
            InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_type_property_it_);
        break;
      case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_property_it_)
            InMemoryEdgePropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_property_it_);
        break;
    }
  }
  return *this;
}

EdgesChunkedIterable::Iterator::~Iterator() { Destroy(); }

void EdgesChunkedIterable::Iterator::Destroy() noexcept {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      in_memory_chunked_by_type_it_.InMemoryEdgeTypeIndex::ChunkedIterable::Iterator::~Iterator();
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_by_type_property_it_.InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator::~Iterator();
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_by_property_it_.InMemoryEdgePropertyIndex::ChunkedIterable::Iterator::~Iterator();
      break;
  }
}

EdgeAccessor const &EdgesChunkedIterable::Iterator::operator*() const {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      return *in_memory_chunked_by_type_it_;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      return *in_memory_chunked_by_type_property_it_;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      return *in_memory_chunked_by_property_it_;
  }
}

EdgesChunkedIterable::Iterator &EdgesChunkedIterable::Iterator::operator++() {
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      ++in_memory_chunked_by_type_it_;
      break;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      ++in_memory_chunked_by_type_property_it_;
      break;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      ++in_memory_chunked_by_property_it_;
      break;
  }
  return *this;
}

bool EdgesChunkedIterable::Iterator::operator==(const Iterator &other) const {
  DMG_ASSERT(type_ == other.type_, "Trying to compare different types of chunked iterators.");
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_type_it_ == other.in_memory_chunked_by_type_it_;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_type_property_it_ == other.in_memory_chunked_by_type_property_it_;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_property_it_ == other.in_memory_chunked_by_property_it_;
  }
}

bool EdgesChunkedIterable::Iterator::operator!=(const Iterator &other) const {
  DMG_ASSERT(type_ == other.type_, "Trying to compare different types of chunked iterators.");
  switch (type_) {
    case Type::BY_EDGE_TYPE_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_type_it_ != other.in_memory_chunked_by_type_it_;
    case Type::BY_EDGE_TYPE_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_type_property_it_ != other.in_memory_chunked_by_type_property_it_;
    case Type::BY_EDGE_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_property_it_ != other.in_memory_chunked_by_property_it_;
  }
}

}  // namespace memgraph::storage
