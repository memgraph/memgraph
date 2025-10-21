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

#include "storage/v2/vertices_iterable.hpp"
namespace memgraph::storage {

VerticesIterable::VerticesIterable(AllVerticesIterable vertices) : type_(Type::ALL) {
  new (&all_vertices_) AllVerticesIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(AllVerticesChunkedIterable vertices) : type_(Type::ALL_CHUNKED) {
  new (&all_chunked_vertices_) AllVerticesChunkedIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(InMemoryLabelIndex::Iterable vertices) : type_(Type::BY_LABEL_IN_MEMORY) {
  new (&in_memory_vertices_by_label_) InMemoryLabelIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(InMemoryLabelPropertyIndex::Iterable vertices)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY) {
  new (&in_memory_vertices_by_label_property_) InMemoryLabelPropertyIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(InMemoryLabelPropertyIndex::ChunkedIterable vertices)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED) {
  new (&in_memory_chunked_vertices_by_label_property_) InMemoryLabelPropertyIndex::ChunkedIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(InMemoryLabelIndex::ChunkedIterable vertices)
    : type_(Type::BY_LABEL_IN_MEMORY_CHUNKED) {
  new (&in_memory_chunked_vertices_by_label_) InMemoryLabelIndex::ChunkedIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(VerticesIterable &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_vertices_by_label_) InMemoryLabelIndex::Iterable(std::move(other.in_memory_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_vertices_by_label_property_)
          InMemoryLabelPropertyIndex::Iterable(std::move(other.in_memory_vertices_by_label_property_));
      break;
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

VerticesIterable &VerticesIterable::operator=(VerticesIterable &&other) noexcept {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL_IN_MEMORY:
      in_memory_vertices_by_label_.InMemoryLabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      in_memory_vertices_by_label_property_.InMemoryLabelPropertyIndex::Iterable::~Iterable();
      break;
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
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_vertices_by_label_) InMemoryLabelIndex::Iterable(std::move(other.in_memory_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_vertices_by_label_property_)
          InMemoryLabelPropertyIndex::Iterable(std::move(other.in_memory_vertices_by_label_property_));
      break;
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

VerticesIterable::~VerticesIterable() {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL_IN_MEMORY:
      in_memory_vertices_by_label_.InMemoryLabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      in_memory_vertices_by_label_property_.InMemoryLabelPropertyIndex::Iterable::~Iterable();
      break;
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

VerticesIterable::Iterator VerticesIterable::begin() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.begin());
    case Type::BY_LABEL_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_.begin());
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_property_.begin());
    case Type::ALL_CHUNKED:
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      throw 1;  // not supported without chunk id
  }
}

VerticesIterable::Iterator VerticesIterable::end() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.end());
    case Type::BY_LABEL_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_.end());
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_property_.end());
    case Type::ALL_CHUNKED:
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      throw 1;  // not supported without chunk id
  }
}

VerticesIterable::Iterator VerticesIterable::begin(size_t id) {
  switch (type_) {
    case Type::ALL:
    case Type::BY_LABEL_IN_MEMORY:
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      throw 1;  // TODO
    case Type::ALL_CHUNKED:
      return Iterator{all_chunked_vertices_.begin(id)};
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return Iterator{in_memory_chunked_vertices_by_label_.begin(id)};
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return Iterator{in_memory_chunked_vertices_by_label_property_.begin(id)};
  }
}

VerticesIterable::Iterator VerticesIterable::end(size_t id) {
  switch (type_) {
    case Type::ALL:
    case Type::BY_LABEL_IN_MEMORY:
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      throw 1;  // TODO
    case Type::ALL_CHUNKED:
      return Iterator{all_chunked_vertices_.end(id)};
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return Iterator{in_memory_chunked_vertices_by_label_.end(id)};
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return Iterator{in_memory_chunked_vertices_by_label_property_.end(id)};
  }
}

size_t VerticesIterable::size() const {
  switch (type_) {
    case Type::ALL:
    case Type::BY_LABEL_IN_MEMORY:
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      throw 1;  // TODO
    case Type::ALL_CHUNKED:
      return all_chunked_vertices_.size();
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return in_memory_chunked_vertices_by_label_.size();
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_vertices_by_label_property_.size();
  }
}

VerticesIterable::Iterator::Iterator(AllVerticesIterable::Iterator it) : type_(Type::ALL) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&all_it_) AllVerticesIterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(InMemoryLabelIndex::Iterable::Iterator it) : type_(Type::BY_LABEL_IN_MEMORY) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(InMemoryLabelPropertyIndex::Iterable::Iterator it)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_by_label_property_it_) InMemoryLabelPropertyIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(AllVerticesChunkedIterable::Iterator it) : type_(Type::ALL_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&all_chunked_it_) AllVerticesChunkedIterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(InMemoryLabelIndex::ChunkedIterable::Iterator it)
    : type_(Type::BY_LABEL_IN_MEMORY_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_chunked_by_label_it_) InMemoryLabelIndex::ChunkedIterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(InMemoryLabelPropertyIndex::ChunkedIterable::Iterator it)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_chunked_by_label_property_it_) InMemoryLabelPropertyIndex::ChunkedIterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(const VerticesIterable::Iterator &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(other.in_memory_by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_by_label_property_it_)
          InMemoryLabelPropertyIndex::Iterable::Iterator(other.in_memory_by_label_property_it_);
      break;
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
VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(const VerticesIterable::Iterator &other) {
  if (this != &other) {
    Destroy();
    type_ = other.type_;
    switch (other.type_) {
      case Type::ALL:
        new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
        break;
      case Type::BY_LABEL_IN_MEMORY:
        new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(other.in_memory_by_label_it_);
        break;
      case Type::BY_LABEL_PROPERTY_IN_MEMORY:
        new (&in_memory_by_label_property_it_)
            InMemoryLabelPropertyIndex::Iterable::Iterator(other.in_memory_by_label_property_it_);
        break;
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

VerticesIterable::Iterator::Iterator(VerticesIterable::Iterator &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(std::move(other.all_it_));
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(std::move(other.in_memory_by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_by_label_property_it_)
          InMemoryLabelPropertyIndex::Iterable::Iterator(std::move(other.in_memory_by_label_property_it_));
      break;
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
VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(VerticesIterable::Iterator &&other) noexcept {
  if (this != &other) {
    Destroy();
    type_ = other.type_;
    switch (other.type_) {
      case Type::ALL:
        new (&all_it_) AllVerticesIterable::Iterator(std::move(other.all_it_));
        break;
      case Type::BY_LABEL_IN_MEMORY:
        new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(std::move(other.in_memory_by_label_it_));
        break;
      case Type::BY_LABEL_PROPERTY_IN_MEMORY:
        new (&in_memory_by_label_property_it_)
            InMemoryLabelPropertyIndex::Iterable::Iterator(std::move(other.in_memory_by_label_property_it_));
        break;
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

VerticesIterable::Iterator::~Iterator() { Destroy(); }

void VerticesIterable::Iterator::Destroy() noexcept {
  switch (type_) {
    case Type::ALL:
      all_it_.AllVerticesIterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_IN_MEMORY:
      in_memory_by_label_it_.InMemoryLabelIndex::Iterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      in_memory_by_label_property_it_.InMemoryLabelPropertyIndex::Iterable::Iterator::~Iterator();
      break;
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

VertexAccessor const &VerticesIterable::Iterator::operator*() const {
  switch (type_) {
    case Type::ALL:
      return *all_it_;
    case Type::BY_LABEL_IN_MEMORY:
      return *in_memory_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return *in_memory_by_label_property_it_;
    case Type::ALL_CHUNKED:
      return *all_chunked_it_;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return *in_memory_chunked_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return *in_memory_chunked_by_label_property_it_;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator++() {
  switch (type_) {
    case Type::ALL:
      ++all_it_;
      break;
    case Type::BY_LABEL_IN_MEMORY:
      ++in_memory_by_label_it_;
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      ++in_memory_by_label_property_it_;
      break;
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

bool VerticesIterable::Iterator::operator==(const VerticesIterable::Iterator &other) const {
  if (type_ != other.type_) return false;
  switch (type_) {
    case Type::ALL:
      return all_it_ == other.all_it_;
    case Type::BY_LABEL_IN_MEMORY:
      return in_memory_by_label_it_ == other.in_memory_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return in_memory_by_label_property_it_ == other.in_memory_by_label_property_it_;
    case Type::ALL_CHUNKED:
      return all_chunked_it_ == other.all_chunked_it_;
    case Type::BY_LABEL_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_label_it_ == other.in_memory_chunked_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY_CHUNKED:
      return in_memory_chunked_by_label_property_it_ == other.in_memory_chunked_by_label_property_it_;
  }
}

}  // namespace memgraph::storage
