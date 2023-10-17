// Copyright 2023 Memgraph Ltd.
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

namespace memgraph::utils {

/**
 * Determines whether the value of bound expression should be included or
 * excluded.
 */
enum class BoundType { INCLUSIVE, EXCLUSIVE };

/** Defines a bounding value for a range. */
template <typename TValue>
class Bound {
 public:
  using Type = BoundType;

  Bound(TValue value, Type type) : value_(value), type_(type) {}

  Bound(const Bound &other) = default;
  Bound(Bound &&other) = default;

  Bound &operator=(const Bound &other) = default;
  Bound &operator=(Bound &&other) = default;

  // bool operator<(const Bound &other) const {
  //   return std::make_tuple(value_, type_) < std::make_tuple(other.value_, other.type_);
  // }

  // bool operator==(const Bound &other) const {
  //   return std::make_tuple(value_, type_) == std::make_tuple(other.value_, other.type_);
  // }

  /** Value for the bound. */
  const auto &value() const { return value_; }
  /** Whether the bound is inclusive or exclusive. */
  auto type() const { return type_; }
  auto IsInclusive() const { return type_ == BoundType::INCLUSIVE; }
  auto IsExclusive() const { return type_ == BoundType::EXCLUSIVE; }

 private:
  TValue value_;
  Type type_;
};

/**
 * Creates an inclusive @c Bound.
 *
 * @param value - Bound value
 * @tparam TValue - value type
 */
template <typename TValue>
Bound<TValue> MakeBoundInclusive(TValue value) {
  return Bound<TValue>(value, BoundType::INCLUSIVE);
};

/**
 * Creates an exclusive @c Bound.
 *
 * @param value - Bound value
 * @tparam TValue - value type
 */
template <typename TValue>
Bound<TValue> MakeBoundExclusive(TValue value) {
  return Bound<TValue>(value, BoundType::EXCLUSIVE);
};

}  // namespace memgraph::utils
