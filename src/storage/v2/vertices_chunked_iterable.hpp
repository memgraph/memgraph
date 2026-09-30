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

#include "storage/v2/all_vertices_chunked_iterable.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/vertex_property_index.hpp"

namespace memgraph::storage {

class VerticesChunkedIterable final {
  using AscChunked = InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::Entry<>>;
  using DescChunked = InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::DescEntry<>>;
  using Asc1Chunked = InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::Entry<1>>;
  using Desc1Chunked = InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::DescEntry<1>>;
  using Asc2Chunked = InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::Entry<2>>;
  using Desc2Chunked = InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::DescEntry<2>>;

  using VertexPropertyChunked = InMemoryVertexPropertyIndex::ChunkedIterable;

  using Data = std::variant<AllVerticesChunkedIterable, InMemoryLabelIndex::ChunkedIterable, AscChunked, DescChunked,
                            Asc1Chunked, Desc1Chunked, Asc2Chunked, Desc2Chunked, VertexPropertyChunked>;

  Data data_;

 public:
  explicit VerticesChunkedIterable(AllVerticesChunkedIterable v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(InMemoryLabelIndex::ChunkedIterable v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(AscChunked v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(DescChunked v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(Asc1Chunked v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(Desc1Chunked v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(Asc2Chunked v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(Desc2Chunked v) : data_(std::move(v)) {}

  explicit VerticesChunkedIterable(VertexPropertyChunked v) : data_(std::move(v)) {}

  VerticesChunkedIterable(const VerticesChunkedIterable &) = delete;
  VerticesChunkedIterable &operator=(const VerticesChunkedIterable &) = delete;

  VerticesChunkedIterable(VerticesChunkedIterable &&) noexcept = default;
  VerticesChunkedIterable &operator=(VerticesChunkedIterable &&) noexcept = default;

  ~VerticesChunkedIterable() = default;

  class Iterator final {
    using Data =
        std::variant<AllVerticesChunkedIterable::Iterator, InMemoryLabelIndex::ChunkedIterable::Iterator,
                     AscChunked::Iterator, DescChunked::Iterator, Asc1Chunked::Iterator, Desc1Chunked::Iterator,
                     Asc2Chunked::Iterator, Desc2Chunked::Iterator, VertexPropertyChunked::Iterator>;

    Data data_;

   public:
    explicit Iterator(AllVerticesChunkedIterable::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(InMemoryLabelIndex::ChunkedIterable::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(AscChunked::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(DescChunked::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(Asc1Chunked::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(Desc1Chunked::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(Asc2Chunked::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(Desc2Chunked::Iterator it) : data_(std::move(it)) {}

    explicit Iterator(VertexPropertyChunked::Iterator it) : data_(std::move(it)) {}

    Iterator(const Iterator &) = default;
    Iterator &operator=(const Iterator &) = default;

    Iterator(Iterator &&) noexcept = default;
    Iterator &operator=(Iterator &&) noexcept = default;

    ~Iterator() = default;

    VertexAccessor const &operator*() const {
      return std::visit([](auto const &it) -> VertexAccessor const & { return *it; }, data_);
    }

    Iterator &operator++() {
      std::visit([](auto &it) { ++it; }, data_);
      return *this;
    }

    bool operator==(const Iterator &other) const = default;
    bool operator!=(const Iterator &other) const = default;
  };

  class Chunk {
    Iterator begin_;
    Iterator end_;

   public:
    explicit Chunk(auto &&chunk) : begin_{chunk.begin()}, end_{chunk.end()} {}

    Iterator begin() { return begin_; }

    Iterator end() { return end_; }
  };

  Chunk get_chunk(size_t id) {
    return std::visit([&](auto &v) -> Chunk { return Chunk(v.get_chunk(id)); }, data_);
  }

  size_t size() const {
    return std::visit([](auto const &v) -> size_t { return v.size(); }, data_);
  }
};

}  // namespace memgraph::storage
