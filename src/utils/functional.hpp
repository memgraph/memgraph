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

#include <algorithm>
#include <vector>

namespace memgraph::utils {

template <class F, class T, class R = typename std::result_of<F(T)>::type, class V = std::vector<R>>
V fmap(F &&f, const std::vector<T> &v) {
  V r;
  r.reserve(v.size());
  std::ranges::transform(v, std::back_inserter(r), std::forward<F>(f));
  return r;
}

}  // namespace memgraph::utils
