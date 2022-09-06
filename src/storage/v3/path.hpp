// Copyright 2022 Memgraph Ltd.
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

#include "storage/v3/vertex_accessor.hpp"
#include "utils/memory.hpp"

namespace memgraph::storage::v3 {

class Path {
 public:
  using allocator_type = utils::Allocator<char>;
  explicit Path(const VertexAccessor & /*vertex*/) {}

  template <typename... TOthers>
  explicit Path(const VertexAccessor &vertex, const TOthers &...others) {}

  template <typename... TOthers>
  Path(std::allocator_arg_t, utils::MemoryResource *memory, const VertexAccessor &vertex, const TOthers &...others) {}

  Path(const Path & /*other*/) {}

  Path(const Path & /*other*/, utils::MemoryResource * /*memory*/) {}

  Path(Path &&other) noexcept : Path(std::move(other), other.GetMemoryResource()) {}

  Path(Path && /*other*/, utils::MemoryResource * /*memory*/) {}

  Path &operator=(const Path &) = default;

  Path &operator=(Path &&) = default;

  ~Path() = default;

  void Expand(const VertexAccessor &vertex) {}

  void Expand(const EdgeAccessor &edge) {}

  template <typename TFirst, typename... TOthers>
  void Expand(const TFirst &first, const TOthers &...others) {}

  auto size() const;

  std::pmr::vector<VertexAccessor> &vertices();
  std::pmr::vector<EdgeAccessor> &edges();
  const std::pmr::vector<VertexAccessor> &vertices() const;
  const std::pmr::vector<EdgeAccessor> &edges() const;

  utils::MemoryResource *GetMemoryResource() const;

  bool operator==(const Path &other) const;
};
}  // namespace memgraph::storage::v3
