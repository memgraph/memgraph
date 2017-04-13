#pragma once

#include <vector>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/typed_value.hpp"

namespace query {

class Frame {
 public:
  Frame(int size) : size_(size), elems_(size_) {}

  TypedValue &operator[](const Symbol &symbol) {
    return elems_[symbol.position_];
  }
  const TypedValue &operator[](const Symbol &symbol) const {
    return elems_[symbol.position_];
  }

 private:
  int size_;
  std::vector<TypedValue> elems_;
};

}  // namespace query
