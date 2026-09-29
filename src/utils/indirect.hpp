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

#include <concepts>
#include <memory>
#include <type_traits>
#include <utility>

namespace memgraph::utils {

/// One `T` on the heap, with value semantics: a copy copies the `T`, and `T` may be incomplete where the member
/// is declared, so a type can hold itself. The subset of C++26 `std::indirect` (P3019) without allocators;
/// replace it with the standard type once the project builds as C++26.
template <typename T>
class indirect {
 public:
  using value_type = T;

  indirect() : value_(std::make_unique<T>()) {}

  template <typename... Args>
  explicit indirect(std::in_place_t /*tag*/, Args &&...args)
      : value_(std::make_unique<T>(std::forward<Args>(args)...)) {}

  template <typename U = T>
    requires(!std::same_as<std::remove_cvref_t<U>, indirect> &&
             !std::same_as<std::remove_cvref_t<U>, std::in_place_t> && std::constructible_from<T, U>)
  explicit indirect(U &&value) : value_(std::make_unique<T>(std::forward<U>(value))) {}

  indirect(const indirect &other) : value_(other.value_ ? std::make_unique<T>(*other.value_) : nullptr) {}

  /// Leaves `other` valueless, as `std::indirect` does.
  indirect(indirect &&other) noexcept = default;

  indirect &operator=(const indirect &other) {
    if (this != &other) value_ = other.value_ ? std::make_unique<T>(*other.value_) : nullptr;
    return *this;
  }

  indirect &operator=(indirect &&other) noexcept = default;

  ~indirect() = default;

  const T &operator*() const & { return *value_; }

  T &operator*() & { return *value_; }

  const T &&operator*() const && { return std::move(*value_); }

  T &&operator*() && { return std::move(*value_); }

  const T *operator->() const { return value_.get(); }

  T *operator->() { return value_.get(); }

  /// Only a moved-from `indirect` has no value; reading one is undefined, as with `std::indirect`.
  bool valueless_after_move() const noexcept { return value_ == nullptr; }

  void swap(indirect &other) noexcept { value_.swap(other.value_); }

  friend void swap(indirect &lhs, indirect &rhs) noexcept { lhs.swap(rhs); }

 private:
  std::unique_ptr<T> value_;
};

}  // namespace memgraph::utils
