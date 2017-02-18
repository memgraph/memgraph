#pragma once

#include "utils/assert.hpp"

// data structure namespace short ds
// TODO: document strategy related to namespace naming
// (namespace names should be short but eazy to memorize)
namespace ds {

// static array is data structure which size (capacity) can be known at compile
// time
// this data structure isn't concurrent
template <typename T, size_t N>
class static_array {
 public:
  // default constructor
  static_array() {}

  // explicit constructor which populates the data array with
  // initial values, array structure after initialization
  // is N * [initial_value]
  explicit static_array(const T &initial_value) {
    for (size_t i = 0; i < size(); ++i) {
      data[i] = initial_value;
    }
  }

  // returns array size
  size_t size() const { return N; }

  // returns element reference on specific index
  T &operator[](size_t index) {
    runtime_assert(index < N, "Index " << index << " must be less than " << N);
    return data[index];
  }

  // returns const element reference on specific index
  const T &operator[](size_t index) const {
    runtime_assert(index < N, "Index " << index << " must be less than " << N);
    return data[index];
  }

  // returns begin iterator
  T *begin() { return &data[0]; }

  // returns const begin iterator
  const T *begin() const { return &data[0]; }

  // returns end iterator
  T *end() { return &data[N]; }

  // returns const end iterator
  const T *end() const { return &data[N]; }

 private:
  T data[N];
};
}
