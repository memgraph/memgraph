// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <optional>

#include "utils/logging.hpp"

namespace utils {

template <class TError, class TValue = void>
class [[nodiscard]] BasicResult final {
 public:
  using ErrorType = TError;
  using ValueType = TValue;
  BasicResult(const TValue &value) : value_(value) {}
  BasicResult(TValue &&value) noexcept : value_(std::move(value)) {}
  BasicResult(const TError &error) : error_(error) {}
  BasicResult(TError &&error) noexcept : error_(std::move(error)) {}

  bool HasValue() const { return value_.has_value(); }
  bool HasError() const { return error_.has_value(); }

  TValue &GetValue() & {
    MG_ASSERT(value_, "The storage result is an error!");
    return *value_;
  }

  TValue &&GetValue() && {
    MG_ASSERT(value_, "The storage result is an error!");
    return std::move(*value_);
  }

  const TValue &GetValue() const & {
    MG_ASSERT(value_, "The storage result is an error!");
    return *value_;
  }

  const TValue &&GetValue() const && {
    MG_ASSERT(value_, "The storage result is an error!");
    return std::move(*value_);
  }

  TValue &operator*() & {
    MG_ASSERT(value_, "The storage result is an error!");
    return *value_;
  }

  TValue &&operator*() && {
    MG_ASSERT(value_, "The storage result is an error!");
    return std::move(*value_);
  }

  const TValue &operator*() const & {
    MG_ASSERT(value_, "The storage result is an error!");
    return *value_;
  }

  const TValue &&operator*() const && {
    MG_ASSERT(value_, "The storage result is an error!");
    return std::move(*value_);
  }

  TValue *operator->() {
    MG_ASSERT(value_, "The storage result is an error!");
    return &*value_;
  }

  const TValue *operator->() const {
    MG_ASSERT(value_, "The storage result is an error!");
    return &*value_;
  }

  TError &GetError() & {
    MG_ASSERT(error_, "The storage result is a value!");
    return *error_;
  }

  TError &&GetError() && {
    MG_ASSERT(error_, "The storage result is a value!");
    return std::move(*error_);
  }

  const TError &GetError() const & {
    MG_ASSERT(error_, "The storage result is a value!");
    return *error_;
  }

  const TError &&GetError() const && {
    MG_ASSERT(error_, "The storage result is a value!");
    return std::move(*error_);
  }

 private:
  std::optional<TValue> value_;
  std::optional<TError> error_;
};

template <class TError>
class [[nodiscard]] BasicResult<TError, void> final {
 public:
  using ErrorType = TError;
  using ValueType = void;
  BasicResult() = default;
  BasicResult(const TError &error) : error_(error) {}
  BasicResult(TError &&error) noexcept : error_(std::move(error)) {}

  bool HasError() const { return error_.has_value(); }

  TError &GetError() & {
    MG_ASSERT(error_, "The storage result is a value!");
    return *error_;
  }

  TError &&GetError() && {
    MG_ASSERT(error_, "The storage result is a value!");
    return std::move(*error_);
  }

  const TError &GetError() const & {
    MG_ASSERT(error_, "The storage result is a value!");
    return *error_;
  }

  const TError &&GetError() const && {
    MG_ASSERT(error_, "The storage result is a value!");
    return std::move(*error_);
  }

 private:
  std::optional<TError> error_;
};

}  // namespace utils
