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

#include "storage/v2/id_types.hpp"

#include <cstdint>

namespace memgraph::storage {

// custom datastructure to hold LabelIds
// design goals:
// - 16B, so we are smaller than std::vector<LabelId> (24B)
// - small representation to avoid allocation
// layout:
//  Heap allocation
//  ┌─────────┬─────────┐
//  │SIZE     │CAPACITY │
//  ├─────────┴─────────┤    ┌────┬────┬────┬─
//  │PTR                ├───►│    │    │    │...
//  └───────────────────┘    └────┴────┴────┴─
//  Small representation
//  ┌─────────┬─────────┐
//  │<=2      │2        │
//  ├─────────┼─────────┤
//  │Label1   │Label2   │
//  └─────────┴─────────┘

using old_label_set = std::vector<LabelId>;

namespace in_progress {
struct label_set {
  using value_type = LabelId;
  using reference = value_type &;
  using const_reference = value_type const &;
  using size_type = uint32_t;

  using iterator = LabelId *;
  using const_iterator = LabelId const *;

  using reverse_iterator = LabelId *;
  using const_reverse_iterator = LabelId const *;

  auto begin() -> iterator;
  auto end() -> iterator;
  auto begin() const -> const_iterator;
  auto end() const -> const_iterator;

  auto rbegin() -> reverse_iterator;
  auto rend() -> reverse_iterator;
  auto rbegin() const -> const_reverse_iterator;
  auto rend() const -> const_reverse_iterator;

  auto cbegin() const -> const_iterator;
  auto cend() const -> const_iterator;

  void push_back(LabelId);
  void emplace_back(LabelId);
  reference back();
  const_reference back() const;
  void pop_back();
  void reserve(size_type new_cap);

  auto size() const noexcept -> std::size_t;

  template <typename It>
  label_set(It begin, It end);

  auto operator[](size_t idx) -> reference;
  auto operator[](size_t idx) const -> const_reference;

  label_set() = default;
  label_set(label_set const &);
  label_set(label_set &&) noexcept;
  label_set &operator=(label_set const &);
  label_set &operator=(label_set &&) noexcept;
  ~label_set();

 private:
  static constexpr size_type kSmallCapacity = sizeof(LabelId *) / sizeof(LabelId);

  size_type size_ = 0;
  size_type capacity_ = kSmallCapacity;
  union {
    std::array<LabelId, kSmallCapacity> small_buffer_;
    LabelId *buffer_;
  };
};

static_assert(sizeof(label_set) == 16);
static_assert(sizeof(label_set) < sizeof(old_label_set));

}  // namespace in_progress

using label_set = old_label_set;  // in_progress::label_set;

}  // namespace memgraph::storage
