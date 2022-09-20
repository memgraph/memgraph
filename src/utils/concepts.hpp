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
#include <concepts>
#include <iterator>

namespace memgraph::utils {
template <typename T, typename... Args>
concept SameAsAnyOf = (std::same_as<T, Args> || ...);

template <typename T>
concept Enum = std::is_enum_v<T>;

// WithRef, CanReference and Dereferenceable is based on the similarly named concepts in GCC 11.2.0
// bits/iterator_concepts.h
template <typename T>
using WithRef = T &;

template <typename T>
concept CanReference = requires {
  typename WithRef<T>;
};

template <typename T>
concept Dereferenceable = requires(T t) {
  { *t } -> CanReference;
};
}  // namespace memgraph::utils
