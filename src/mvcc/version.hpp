#pragma once

#include <atomic>

namespace mvcc {

template <class T>
class Version {
 public:
  Version() = default;
  explicit Version(T *older) : older_(older) {}

  ~Version() { delete older_.load(std::memory_order_seq_cst); }

  // return a pointer to an older version stored in this record
  T *next(std::memory_order order = std::memory_order_seq_cst) {
    return older_.load(order);
  }

  const T *next(std::memory_order order = std::memory_order_seq_cst) const {
    return older_.load(order);
  }

  // set the older version of this record
  void next(T *value, std::memory_order order = std::memory_order_seq_cst) {
    older_.store(value, order);
  }

 private:
  std::atomic<T *> older_{nullptr};
};
}  // namespace mvcc
