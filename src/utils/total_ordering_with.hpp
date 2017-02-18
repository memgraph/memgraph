#pragma once

template <class Derived, class T>
struct TotalOrderingWith {
  friend constexpr bool operator!=(const Derived& a, const T& b) {
    return !(a == b);
  }

  friend constexpr bool operator<=(const Derived& a, const T& b) {
    return a < b || a == b;
  }

  friend constexpr bool operator>(const Derived& a, const T& b) {
    return !(a <= b);
  }

  friend constexpr bool operator>=(const Derived& a, const T& b) {
    return !(a < b);
  }

  friend constexpr bool operator!=(const T& a, const Derived& b) {
    return !(a == b);
  }

  friend constexpr bool operator<=(const T& a, const Derived& b) {
    return a < b || a == b;
  }

  friend constexpr bool operator>(const T& a, const Derived& b) {
    return !(a <= b);
  }

  friend constexpr bool operator>=(const T& a, const Derived& b) {
    return !(a < b);
  }
};
