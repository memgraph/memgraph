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
 protected:
  ~TotalOrdering() {}

 public:
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

}  // namespace utils
