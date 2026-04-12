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

#pragma once

#include <variant>

#include "storage/v2/all_vertices_iterable.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"

namespace memgraph::storage {

class VerticesIterable final {
  using AscIterable = InMemoryLabelPropertyIndex::Iterable<InMemoryLabelPropertyIndex::Entry>;
  using DescIterable = InMemoryLabelPropertyIndex::Iterable<InMemoryLabelPropertyIndex::DescEntry>;

  using Data = std::variant<AllVerticesIterable, InMemoryLabelIndex::Iterable, AscIterable, DescIterable>;

  Data data_;

 public:
  explicit VerticesIterable(AllVerticesIterable v) : data_(std::move(v)) {}

  explicit VerticesIterable(InMemoryLabelIndex::Iterable v) : data_(std::move(v)) {}

  explicit VerticesIterable(AscIterable v) : data_(std::move(v)) {}

  explicit VerticesIterable(DescIterable v) : data_(std::move(v)) {}

  VerticesIterable(const VerticesIterable &) = delete;
  VerticesIterable &operator=(const VerticesIterable &) = delete;

  VerticesIterable(VerticesIterable &&) noexcept = default;
  VerticesIterable &operator=(VerticesIterable &&) noexcept = default;

  ~VerticesIterable() = default;

  class Iterator final {
    using Data = std::variant<AllVerticesIterable::Iterator, InMemoryLabelIndex::Iterable::Iterator,
                              AscIterable::Iterator, DescIterable::Iterator>;

    Data data_;

   public:
    using difference_type = std::ptrdiff_t;
    using value_type = VertexAccessor;

    explicit Iterator(AllVerticesIterable::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(InMemoryLabelIndex::Iterable::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(AscIterable::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(DescIterable::Iterator it) : data_(std::move(it)) {}

    Iterator(const Iterator &) = default;
    Iterator &operator=(const Iterator &) = default;

    Iterator(Iterator &&) noexcept = default;
    Iterator &operator=(Iterator &&) noexcept = default;

    ~Iterator() = default;

    value_type const &operator*() const {
      return std::visit([](auto const &it) -> value_type const & { return *it; }, data_);
    }

    Iterator &operator++() {
      std::visit([](auto &it) { ++it; }, data_);
      return *this;
    }

    bool operator==(const Iterator &other) const = default;
  };

  Iterator begin() {
    return std::visit([](auto &v) -> Iterator { return Iterator(v.begin()); }, data_);
  }

  Iterator end() {
    return std::visit([](auto &v) -> Iterator { return Iterator(v.end()); }, data_);
  }
};

}  // namespace memgraph::storage
