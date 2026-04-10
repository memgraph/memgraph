// Copyright 2026 Memgraph Ltd.
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

using AscIterable = InMemoryLabelPropertyIndex::Iterable<InMemoryLabelPropertyIndex::Entry>;
using DescIterable = InMemoryLabelPropertyIndex::Iterable<InMemoryLabelPropertyIndex::DescEntry>;

VerticesIterable::VerticesIterable(AllVerticesIterable vertices) : type_(Type::ALL) {
  new (&all_vertices_) AllVerticesIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(InMemoryLabelIndex::Iterable vertices) : type_(Type::BY_LABEL_IN_MEMORY) {
  new (&in_memory_vertices_by_label_) InMemoryLabelIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(AscIterable vertices) : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY) {
  new (&in_memory_vertices_by_label_property_) AscIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(DescIterable vertices) : type_(Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY) {
  new (&in_memory_vertices_by_label_property_desc_) DescIterable(std::move(vertices));
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
      new (&in_memory_vertices_by_label_property_) AscIterable(std::move(other.in_memory_vertices_by_label_property_));
      break;
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      new (&in_memory_vertices_by_label_property_desc_)
          DescIterable(std::move(other.in_memory_vertices_by_label_property_desc_));
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
      in_memory_vertices_by_label_property_.~AscIterable();
      break;
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      in_memory_vertices_by_label_property_desc_.~DescIterable();
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
      new (&in_memory_vertices_by_label_property_) AscIterable(std::move(other.in_memory_vertices_by_label_property_));
      break;
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      new (&in_memory_vertices_by_label_property_desc_)
          DescIterable(std::move(other.in_memory_vertices_by_label_property_desc_));
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
      in_memory_vertices_by_label_property_.~AscIterable();
      break;
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      in_memory_vertices_by_label_property_desc_.~DescIterable();
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
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_property_desc_.begin());
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
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_property_desc_.end());
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

VerticesIterable::Iterator::Iterator(AscIterable::Iterator it) : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_by_label_property_it_) AscIterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(DescIterable::Iterator it) : type_(Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY) {
  // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
  new (&in_memory_by_label_property_desc_it_) DescIterable::Iterator(std::move(it));
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
      new (&in_memory_by_label_property_it_) AscIterable::Iterator(other.in_memory_by_label_property_it_);
      break;
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      new (&in_memory_by_label_property_desc_it_) DescIterable::Iterator(other.in_memory_by_label_property_desc_it_);
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
        new (&in_memory_by_label_property_it_) AscIterable::Iterator(other.in_memory_by_label_property_it_);
        break;
      case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
        new (&in_memory_by_label_property_desc_it_) DescIterable::Iterator(other.in_memory_by_label_property_desc_it_);
        break;
    }
  }
  return *this;
}

VerticesIterable::Iterator::Iterator(VerticesIterable::Iterator &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(other.in_memory_by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_by_label_property_it_) AscIterable::Iterator(other.in_memory_by_label_property_it_);
      break;
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      new (&in_memory_by_label_property_desc_it_) DescIterable::Iterator(other.in_memory_by_label_property_desc_it_);
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
        new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
        break;
      case Type::BY_LABEL_IN_MEMORY:
        new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(other.in_memory_by_label_it_);
        break;
      case Type::BY_LABEL_PROPERTY_IN_MEMORY:
        new (&in_memory_by_label_property_it_) AscIterable::Iterator(other.in_memory_by_label_property_it_);
        break;
      case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
        new (&in_memory_by_label_property_desc_it_) DescIterable::Iterator(other.in_memory_by_label_property_desc_it_);
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
      in_memory_by_label_property_it_.AscIterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      in_memory_by_label_property_desc_it_.DescIterable::Iterator::~Iterator();
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
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      return *in_memory_by_label_property_desc_it_;
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
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      ++in_memory_by_label_property_desc_it_;
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
    case Type::BY_LABEL_PROPERTY_DESC_IN_MEMORY:
      return in_memory_by_label_property_desc_it_ == other.in_memory_by_label_property_desc_it_;
  }
}

}  // namespace memgraph::storage
