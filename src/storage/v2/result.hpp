#pragma once

#include <optional>

#include <glog/logging.h>

namespace storage {

enum class Error : uint8_t {
  SERIALIZATION_ERROR,
  DELETED_OBJECT,
  VERTEX_HAS_EDGES,
};

template <typename TReturn>
class Result final {
 public:
  explicit Result(const TReturn &ret) : return_(ret) {}
  explicit Result(TReturn &&ret) : return_(std::move(ret)) {}
  explicit Result(const Error &error) : error_(error) {}

  bool IsReturn() const { return return_.has_value(); }
  bool IsError() const { return error_.has_value(); }

  TReturn &GetReturn() {
    CHECK(return_) << "The storage result is an error!";
    return return_.value();
  }

  const TReturn &GetReturn() const {
    CHECK(return_) << "The storage result is an error!";
    return return_.value();
  }

  Error GetError() const {
    CHECK(error_) << "The storage result is a return value!";
    return *error_;
  }

 private:
  std::optional<TReturn> return_;
  std::optional<Error> error_;
};

}  // namespace storage
