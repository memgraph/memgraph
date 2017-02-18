#pragma once

#include <atomic>
#include <type_traits>

template <class T,
          typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
class AtomicCounter {
 public:
  AtomicCounter(T initial = 0) : counter(initial) {}

  T next(std::memory_order order = std::memory_order_seq_cst) {
    return counter.fetch_add(1, order);
  }

  T operator++() { return next(); }

 private:
  std::atomic<T> counter;
};
