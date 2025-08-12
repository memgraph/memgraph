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

#include <algorithm>
#include <atomic>

// Replace with C++26 code when available
template <class T>
T atomic_fetch_max_explicit(std::atomic<T> *current, typename std::atomic<T>::value_type const value,
                            std::memory_order m_order = std::memory_order_acq_rel) noexcept {
  auto old = current->load(std::memory_order_acquire);
  while (std::max(old, value) != old) {
    if (current->compare_exchange_weak(old, value, m_order, std::memory_order_acquire)) {
      return old;
    }
  }
  // dummy write if release operation
  if (m_order == std::memory_order::release || m_order == std::memory_order::acq_rel ||
      m_order == std::memory_order::seq_cst) {
    current->fetch_add(0, m_order);
  }
  return old;
}

template <typename T>
concept AtomicStruct =
    std::is_trivially_copyable_v<T> && std::is_copy_constructible_v<T> && std::is_move_constructible_v<T> &&
    std::is_copy_assignable_v<T> && std::is_move_assignable_v<T> && std::is_same_v<T, std::remove_cv_t<T>>;

template <AtomicStruct T, typename F>
requires std::is_trivially_copyable_v<T>
void atomic_struct_update(std::atomic<T> &data, F &&func) {
  auto curr_info = data.load(std::memory_order_acquire);
  while (
      !data.compare_exchange_weak(curr_info, func(curr_info), std::memory_order_acq_rel, std::memory_order_acquire)) {
  }
}
