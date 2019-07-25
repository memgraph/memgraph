#pragma once

#include <optional>

#include <glog/logging.h>

namespace storage {

enum class Error : uint8_t {
  SERIALIZATION_ERROR,
  DELETED_OBJECT,
  VERTEX_HAS_EDGES,
};

template <typename TValue>
class [[nodiscard]] Result final {
 public:
  explicit Result(const TValue &value) : value_(value) {}
  explicit Result(TValue &&value) : value_(std::move(value)) {}
  explicit Result(const Error &error) : error_(error) {}

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

  Error GetError() const {
    CHECK(error_) << "The storage result is a value!";
    return *error_;
  }

 private:
  std::optional<TValue> value_;
  std::optional<Error> error_;
};

}  // namespace storage
