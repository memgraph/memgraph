#pragma once

#include "utils/assert.hpp"

// Like option just for pointers. More efficent than option.
template <class T>
class OptionPtr {
 public:
  OptionPtr() {}
  OptionPtr(T *ptr) : ptr(ptr) {}

  bool is_present() { return ptr != nullptr; }

  T *get() {
    debug_assert(is_present(), "Data is not present.");
    return ptr;
  }

 private:
  T *ptr = nullptr;
};

template <class T>
auto make_option_ptr(T *t) {
  return OptionPtr<T>(t);
}
