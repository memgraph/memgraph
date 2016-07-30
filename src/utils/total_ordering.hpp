#pragma once

template <class Derived, class Other = Derived> struct TotalOrdering {
  friend constexpr bool operator!=(const Derived &a, const Other &b) {
    return !(a == b);
  }

  friend constexpr bool operator<=(const Derived &a, const Other &b) {
    return a < b || a == b;
  }

  friend constexpr bool operator>(const Derived &a, const Other &b) {
    return !(a <= b);
  }

  friend constexpr bool operator>=(const Derived &a, const Other &b) {
    return !(a < b);
  }
};
