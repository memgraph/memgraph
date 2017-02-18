#pragma once

#include "utils/option.hpp"
#include "utils/total_ordering.hpp"

// Defines Including as [ and Excluding < for ranges.
enum BorderType {
  Including = 0,
  Excluding = 1,
};

// If key is not present he is both the largest and the smallest of the keys.
template <class T>
class Border {
 public:
  Border() : key(Option<T>()), type(Including) {}
  Border(T Tey, BorderType type) : key(Option<T>(std::move(key))), type(type) {}

  // Border(Border &other) = default;
  Border(Border &other) = default;
  Border(Border &&other) = default;

  Border &operator=(Border &&other) = default;
  Border &operator=(Border &other) = default;

  friend bool operator<(const Border<T> &a, const T &other) {
    return !a.key.is_present() ||
           (a.type == Excluding && a.key.get() <= other) ||
           (a.type == Including && a.key.get() < other);
  }

  friend bool operator==(const Border<T> &a, const T &other) {
    return a.type == Including && a.key.is_present() && a.key.get() == other;
  }

  friend bool operator>(const Border<T> &a, const T &other) {
    return !a.key.is_present() ||
           (a.type == Excluding && a.key.get() >= other) ||
           (a.type == Including && a.key.get() > other);
  }

  friend bool operator!=(const Border<T> &a, const T &b) { return !(a == b); }

  friend bool operator<=(const Border<T> &a, const T &b) {
    return a < b || a == b;
  }

  friend bool operator>=(const Border<T> &a, const T &b) {
    return a > b || a == b;
  }

  Option<T> key;
  BorderType type;
};

template <class T>
auto make_inf_border() {
  return Border<T>();
}
