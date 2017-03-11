#pragma once

#include <vector>

#include "query/backend/cpp/typed_value.hpp"

namespace query {

class Frame {
 public:
  Frame(int size) : size_(size), elems_(size_) {}

  auto& operator[](int pos) { return elems_[pos]; }
  const auto& operator[](int pos) const { return elems_[pos]; }

 private:
  int size_;
  std::vector<TypedValue> elems_;
};

}
