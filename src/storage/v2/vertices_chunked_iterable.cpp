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

#include "storage/v2/vertices_chunked_iterable.hpp"

namespace memgraph::storage {

VerticesChunkedIterable::VerticesChunkedIterable(AllVerticesChunkedIterable vertices) : type_(Type::ALL_CHUNKED) {
  new (&all_chunked_vertices_) AllVerticesChunkedIterable(std::move(vertices));
}

VerticesChunkedIterable::VerticesChunkedIterable(InMemoryLabelIndex::ChunkedIterable vertices)
    : type_(Type::BY_LABEL_IN_MEMORY_CHUNKED) {
  new (&in_memory_chunked_vertices_by_label_) InMemoryLabelIndex::ChunkedIterable(std::move(vertices));
}

VerticesChunkedIterable::VerticesChunkedIterable(InMemoryLabelPropertyIndex::ChunkedIterable vertices)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED) {
  new (&in_memory_chunked_vertices_by_label_property_) InMemoryLabelPropertyIndex::ChunkedIterable(std::move(vertices));
}

VerticesChunkedIterable::VerticesChunkedIterable(VerticesChunkedIterable &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL_CHUNKED:
      new (&all_chunked_vertices_) AllVerticesChunkedIterable(std::move(other.all_chunked_vertices_));
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_vertices_by_label_)
          InMemoryLabelIndex::ChunkedIterable(std::move(other.in_memory_chunked_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_vertices_by_label_property_)
          InMemoryLabelPropertyIndex::ChunkedIterable(std::move(other.in_memory_chunked_vertices_by_label_property_));
      break;
  }
}

VerticesChunkedIterable &VerticesChunkedIterable::operator=(VerticesChunkedIterable &&other) noexcept {
  switch (type_) {
    case Type::ALL_CHUNKED:
      all_chunked_vertices_.AllVerticesChunkedIterable::~AllVerticesChunkedIterable();
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      in_memory_chunked_vertices_by_label_.InMemoryLabelIndex::ChunkedIterable::~ChunkedIterable();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_vertices_by_label_property_.InMemoryLabelPropertyIndex::ChunkedIterable::~ChunkedIterable();
      break;
  }
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL_CHUNKED:
      new (&all_chunked_vertices_) AllVerticesChunkedIterable(std::move(other.all_chunked_vertices_));
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_vertices_by_label_)
          InMemoryLabelIndex::ChunkedIterable(std::move(other.in_memory_chunked_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_vertices_by_label_property_)
          InMemoryLabelPropertyIndex::ChunkedIterable(std::move(other.in_memory_chunked_vertices_by_label_property_));
      break;
  }
  return *this;
}

VerticesChunkedIterable::~VerticesChunkedIterable() {
  switch (type_) {
    case Type::ALL_CHUNKED:
      all_chunked_vertices_.AllVerticesChunkedIterable::~AllVerticesChunkedIterable();
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      in_memory_chunked_vertices_by_label_.InMemoryLabelIndex::ChunkedIterable::~ChunkedIterable();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_vertices_by_label_property_.InMemoryLabelPropertyIndex::ChunkedIterable::~ChunkedIterable();
      break;
  }
}

VerticesChunkedIterable::Chunk VerticesChunkedIterable::get_chunk(size_t id) {
  switch (type_) {
    case Type::ALL_CHUNKED:
      return Chunk{all_chunked_vertices_.get_chunk(id)};
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return Chunk(in_memory_chunked_vertices_by_label_.get_chunk(id));
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return Chunk(in_memory_chunked_vertices_by_label_property_.get_chunk(id));
  }
}

size_t VerticesChunkedIterable::size() const {
  switch (type_) {
    case Type::ALL_CHUNKED:
      return all_chunked_vertices_.size();
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return in_memory_chunked_vertices_by_label_.size();
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_vertices_by_label_property_.size();
  }
}

VerticesChunkedIterable::Iterator::Iterator(AllVerticesChunkedIterable::Iterator it) : type_(Type::ALL_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&all_chunked_it_) AllVerticesChunkedIterable::Iterator(std::move(it));
}

VerticesChunkedIterable::Iterator::Iterator(InMemoryLabelIndex::ChunkedIterable::Iterator it)
    : type_(Type::BY_LABEL_IN_MEMORY_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_chunked_by_label_it_) InMemoryLabelIndex::ChunkedIterable::Iterator(std::move(it));
}

VerticesChunkedIterable::Iterator::Iterator(InMemoryLabelPropertyIndex::ChunkedIterable::Iterator it)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_chunked_by_label_property_it_) InMemoryLabelPropertyIndex::ChunkedIterable::Iterator(std::move(it));
}

VerticesChunkedIterable::Iterator::Iterator(const VerticesChunkedIterable::Iterator &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL_CHUNKED:
      new (&all_chunked_it_) AllVerticesChunkedIterable::Iterator(other.all_chunked_it_);
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_label_it_)
          InMemoryLabelIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_label_property_it_)
          InMemoryLabelPropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_label_property_it_);
      break;
  }
}

// NOLINTNEXTLINE(cert-oop54-cpp)
VerticesChunkedIterable::Iterator &VerticesChunkedIterable::Iterator::operator=(
    const VerticesChunkedIterable::Iterator &other) {
  if (this != &other) {
    Destroy();
    type_ = other.type_;
    switch (other.type_) {
      case Type::ALL_CHUNKED:
        new (&all_chunked_it_) AllVerticesChunkedIterable::Iterator(other.all_chunked_it_);
        break;
      case Type::BY_LABEL_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_label_it_)
            InMemoryLabelIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_label_it_);
        break;
      case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_label_property_it_)
            InMemoryLabelPropertyIndex::ChunkedIterable::Iterator(other.in_memory_chunked_by_label_property_it_);
        break;
    }
  }
  return *this;
}

VerticesChunkedIterable::Iterator::Iterator(VerticesChunkedIterable::Iterator &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL_CHUNKED:
      new (&all_chunked_it_) AllVerticesChunkedIterable::Iterator(std::move(other.all_chunked_it_));
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_label_it_)
          InMemoryLabelIndex::ChunkedIterable::Iterator(std::move(other.in_memory_chunked_by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      new (&in_memory_chunked_by_label_property_it_) InMemoryLabelPropertyIndex::ChunkedIterable::Iterator(
          std::move(other.in_memory_chunked_by_label_property_it_));
      break;
  }
}

// NOLINTNEXTLINE(cert-oop54-cpp)
VerticesChunkedIterable::Iterator &VerticesChunkedIterable::Iterator::operator=(
    VerticesChunkedIterable::Iterator &&other) noexcept {
  if (this != &other) {
    Destroy();
    type_ = other.type_;
    switch (other.type_) {
      case Type::ALL_CHUNKED:
        new (&all_chunked_it_) AllVerticesChunkedIterable::Iterator(std::move(other.all_chunked_it_));
        break;
      case Type::BY_LABEL_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_label_it_)
            InMemoryLabelIndex::ChunkedIterable::Iterator(std::move(other.in_memory_chunked_by_label_it_));
        break;
      case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
        new (&in_memory_chunked_by_label_property_it_) InMemoryLabelPropertyIndex::ChunkedIterable::Iterator(
            std::move(other.in_memory_chunked_by_label_property_it_));
        break;
    }
  }
  return *this;
}

VerticesChunkedIterable::Iterator::~Iterator() { Destroy(); }

void VerticesChunkedIterable::Iterator::Destroy() noexcept {
  switch (type_) {
    case Type::ALL_CHUNKED:
      all_chunked_it_.AllVerticesChunkedIterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      in_memory_chunked_by_label_it_.InMemoryLabelIndex::ChunkedIterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      in_memory_chunked_by_label_property_it_.InMemoryLabelPropertyIndex::ChunkedIterable::Iterator::~Iterator();
      break;
  }
}

VertexAccessor const &VerticesChunkedIterable::Iterator::operator*() const {
  switch (type_) {
    case Type::ALL_CHUNKED:
      return *all_chunked_it_;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return *in_memory_chunked_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return *in_memory_chunked_by_label_property_it_;
  }
}

VerticesChunkedIterable::Iterator &VerticesChunkedIterable::Iterator::operator++() {
  switch (type_) {
    case Type::ALL_CHUNKED:
      ++all_chunked_it_;
      break;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      ++in_memory_chunked_by_label_it_;
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      ++in_memory_chunked_by_label_property_it_;
      break;
  }
  return *this;
}

bool VerticesChunkedIterable::Iterator::operator==(const Iterator &other) const {
  DMG_ASSERT(type_ == other.type_, "Trying to compare different types of chunked iterators.");
  switch (type_) {
    case Type::ALL_CHUNKED:
      return all_chunked_it_ == other.all_chunked_it_;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_label_it_ == other.in_memory_chunked_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_label_property_it_ == other.in_memory_chunked_by_label_property_it_;
  }
}

bool VerticesChunkedIterable::Iterator::operator!=(const Iterator &other) const {
  DMG_ASSERT(type_ == other.type_, "Trying to compare different types of chunked iterators.");
  switch (type_) {
    case Type::ALL_CHUNKED:
      return all_chunked_it_ != other.all_chunked_it_;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_label_it_ != other.in_memory_chunked_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_label_property_it_ != other.in_memory_chunked_by_label_property_it_;
  }
}

}  // namespace memgraph::storage
