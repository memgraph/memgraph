/// @file
#pragma once

#include <optional>

#include "utils/logging.hpp"

namespace utils {

template <class TError, class TValue = void>
class [[nodiscard]] BasicResult final {
 public:
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
