#pragma once

#include <vector>

#include <glog/logging.h>

#include "query/frontend/semantic/symbol_table.hpp"
#include "query/typed_value.hpp"
#include "utils/memory.hpp"

namespace query {

class Frame {
 public:
  /// Create a Frame of given size backed by a utils::NewDeleteResource()
  explicit Frame(int64_t size)
      : size_(size), elems_(size_, utils::NewDeleteResource()) {
    CHECK(size >= 0);
  }

  Frame(int64_t size, utils::MemoryResource *memory)
      : size_(size), elems_(size_, memory) {
    CHECK(size >= 0);
  }

  TypedValue &operator[](const Symbol &symbol) {
    return elems_[symbol.position()];
  }
  const TypedValue &operator[](const Symbol &symbol) const {
    return elems_[symbol.position()];
  }

  TypedValue &at(const Symbol &symbol) { return elems_.at(symbol.position()); }
  const TypedValue &at(const Symbol &symbol) const {
    return elems_.at(symbol.position());
  }

  auto &elems() { return elems_; }

  utils::MemoryResource *GetMemoryResource() const {
    return elems_.get_allocator().GetMemoryResource();
  }

 private:
  int64_t size_;
  utils::AVector<TypedValue> elems_;
};

}  // namespace query
