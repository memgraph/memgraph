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

#pragma once

#include <ranges>
#include <vector>

namespace r = std::ranges;
namespace rv = r::views;

namespace memgraph::utils {

template <template <typename, typename...> class Container, typename T, typename Allocator = std::allocator<T>,
          typename F = std::identity, typename R = std::decay_t<std::invoke_result_t<F, T>>>
requires r::range<Container<T, Allocator>> &&
    (!std::same_as<Container<T, Allocator>, std::string>)auto fmap(const Container<T, Allocator> &v, F &&f = {})
        -> std::vector<R> {
  return v | rv::transform(std::forward<F>(f)) | r::to<std::vector<R>>();
}

}  // namespace memgraph::utils
