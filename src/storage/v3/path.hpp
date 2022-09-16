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

#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"

namespace memgraph::storage::v3 {

class Path {
 public:
  using allocator_type = utils::Allocator<char>;
  explicit Path(const VertexAccessor & /*vertex*/) {}

  template <typename... TOthers>
  explicit Path(const VertexAccessor &vertex, const TOthers &...others) {}

  template <typename... TOthers>
  Path(std::allocator_arg_t alloc, utils::MemoryResource *memory, const VertexAccessor &vertex,
       const TOthers &...others) {}

  Path(const Path & /*other*/) {}

  Path(const Path & /*other*/, utils::MemoryResource * /*memory*/) {}

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
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

  std::pmr::vector<VertexAccessor> &vertices() {
    MG_ASSERT(false, "Using vertices on Path from storage!");
    return vertices_;
  }

  std::pmr::vector<EdgeAccessor> &edges() {
    MG_ASSERT(false, "Using edges on Path from storage!");
    return edges_;
  }

  const std::pmr::vector<VertexAccessor> &vertices() const {
    MG_ASSERT(false, "Using vertices on Path from storage!");
    return vertices_;
  }

  const std::pmr::vector<EdgeAccessor> &edges() const {
    MG_ASSERT(false, "Using edges on Path from storage!");
    return edges_;
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  utils::MemoryResource *GetMemoryResource() const {
    MG_ASSERT(false, "Using GetMemoryResource on Path from storage!");
    return nullptr;
  }

  bool operator==(const Path & /*other*/) const {
    MG_ASSERT(false, "Using operator= on Path from storage!");
    return false;
  };

 private:
  std::pmr::vector<VertexAccessor> vertices_;
  std::pmr::vector<EdgeAccessor> edges_;
};
}  // namespace memgraph::storage::v3
