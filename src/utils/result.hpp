/// @file
#pragma once

#include <optional>

#include <glog/logging.h>

namespace utils {

template <class TError, class TValue>
class [[nodiscard]] BasicResult final {
 public:
  explicit BasicResult(const TValue &value) : value_(value) {}
  explicit BasicResult(TValue &&value) noexcept : value_(std::move(value)) {}
  explicit BasicResult(const TError &error) : error_(error) {}
  explicit BasicResult(TError &&error) noexcept : error_(std::move(error)) {}

  bool HasValue() const { return value_.has_value(); }
  bool HasError() const { return error_.has_value(); }

  TValue &GetValue() & {
    CHECK(value_) << "The storage result is an error!";
    return *value_;
  }

  TValue &&GetValue() && {
    CHECK(value_) << "The storage result is an error!";
    return std::move(*value_);
  }

  const TValue &GetValue() const & {
    CHECK(value_) << "The storage result is an error!";
    return *value_;
  }

  const TValue &&GetValue() const && {
    CHECK(value_) << "The storage result is an error!";
    return std::move(*value_);
  }

  TValue &operator*() & {
    CHECK(value_) << "The storage result is an error!";
    return *value_;
  }

  TValue &&operator*() && {
    CHECK(value_) << "The storage result is an error!";
    return std::move(*value_);
  }

  const TValue &operator*() const & {
    CHECK(value_) << "The storage result is an error!";
    return *value_;
  }

  const TValue &&operator*() const && {
    CHECK(value_) << "The storage result is an error!";
    return std::move(*value_);
  }

  TValue *operator->() {
    CHECK(value_) << "The storage result is an error!";
    return &*value_;
  }

  const TValue *operator->() const {
    CHECK(value_) << "The storage result is an error!";
    return &*value_;
  }

  TError &GetError() & {
    CHECK(error_) << "The storage result is a value!";
    return *error_;
  }

  TError &&GetError() && {
    CHECK(error_) << "The storage result is a value!";
    return std::move(*error_);
  }

  const TError &GetError() const & {
    CHECK(error_) << "The storage result is a value!";
    return *error_;
  }

  const TError &&GetError() const && {
    CHECK(error_) << "The storage result is a value!";
    return std::move(*error_);
  }

 private:
  std::optional<TValue> value_;
  std::optional<TError> error_;
};

}  // namespace utils
