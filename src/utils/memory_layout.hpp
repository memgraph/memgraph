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

#include <cstddef>

namespace memgraph::utils {

// a utility type to have correctly aligned and sized storage for a given type
template <typename T>
struct uninitialised_storage {
  alignas(T) std::byte buf[sizeof(T)];
  auto as() -> T * { return reinterpret_cast<T *>(buf); }
  auto as() const -> T const * { return reinterpret_cast<T const *>(buf); }
};

}  // namespace memgraph::utils
