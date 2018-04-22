#pragma once

namespace utils {

/**
 * Implements all the logical comparison operators based on '=='
 * and '<' operators.
 *
 * @tparam TLhs First operand type.
 * @tparam TRhs Second operand type. Defaults to the same type
 * as first operand.
 * @tparam TReturn Return type, defaults to bool.
 */
template <typename TLhs, typename TRhs = TLhs, typename TReturn = bool>
struct TotalOrdering {
  virtual ~TotalOrdering() {}

  friend constexpr TReturn operator!=(const TLhs &a, const TRhs &b) {
    return !(a == b);
  }

  friend constexpr TReturn operator<=(const TLhs &a, const TRhs &b) {
    return a < b || a == b;
  }

  friend constexpr TReturn operator>(const TLhs &a, const TRhs &b) {
    return !(a <= b);
  }

  friend constexpr TReturn operator>=(const TLhs &a, const TRhs &b) {
    return !(a < b);
  }
};

template <class Derived, class T>
struct TotalOrderingWith {
  virtual ~TotalOrderingWith() {}

  friend constexpr bool operator!=(const Derived &a, const T &b) {
    return !(a == b);
  }

  friend constexpr bool operator<=(const Derived &a, const T &b) {
    return a < b || a == b;
  }

  friend constexpr bool operator>(const Derived &a, const T &b) {
    return !(a <= b);
  }

  friend constexpr bool operator>=(const Derived &a, const T &b) {
    return !(a < b);
  }

  friend constexpr bool operator!=(const T &a, const Derived &b) {
    return !(a == b);
  }

  friend constexpr bool operator<=(const T &a, const Derived &b) {
    return a < b || a == b;
  }

  friend constexpr bool operator>(const T &a, const Derived &b) {
    return !(a <= b);
  }

  friend constexpr bool operator>=(const T &a, const Derived &b) {
    return !(a < b);
  }
};

}  // namespace utils
