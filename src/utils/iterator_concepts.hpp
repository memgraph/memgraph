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

// All of these concepts are exposition-only concepts from cppreference.com

template <class T>
concept Referenceable = !std::same_as<T, void>;

template <class I>
concept LegacyIterator = requires(I i) {
  { *i } -> Referenceable;
  { ++i } -> std::same_as<I &>;
  { *i++ } -> Referenceable;
}
&&std::copyable<I>;

template <class I>
concept LegacyInputIterator = LegacyIterator<I> && std::equality_comparable<I> && requires(I i) {
  typename std::incrementable_traits<I>::difference_type;
  typename std::indirectly_readable_traits<I>::value_type;
  typename std::common_reference_t<std::iter_reference_t<I> &&,
                                   typename std::indirectly_readable_traits<I>::value_type &>;
  *i++;
  typename std::common_reference_t<decltype(*i++) &&, typename std::indirectly_readable_traits<I>::value_type &>;
  requires std::signed_integral<typename std::incrementable_traits<I>::difference_type>;
};

template <class I>
concept LegacyForwardIterator =
    LegacyInputIterator<I> && std::constructible_from<I> && std::is_lvalue_reference_v<std::iter_reference_t<I>> &&
    std::same_as < std::remove_cvref_t<std::iter_reference_t<I>>,
typename std::indirectly_readable_traits<I>::value_type > &&requires(I i) {
  { i++ } -> std::convertible_to<const I &>;
  { *i++ } -> std::same_as<std::iter_reference_t<I>>;
};
}  // namespace memgraph::utils
