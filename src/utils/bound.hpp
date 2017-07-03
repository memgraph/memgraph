#pragma once

namespace utils {

/**
 * Determines whether the value of bound expression should be included or
 * excluded.
 */
enum class BoundType { INCLUSIVE, EXCLUSIVE };

/** Defines a bounding value for a range. */
template <typename TValue>
class Bound {
 public:
  using Type = BoundType;

  Bound(TValue value, Type type) : value_(value), type_(type) {}

  Bound(const Bound &other) = default;
  Bound(Bound &&other) = default;

  Bound &operator=(const Bound &other) = default;
  Bound &operator=(Bound &&other) = default;

  /** Value for the bound. */
  const auto &value() const { return value_; }
  /** Whether the bound is inclusive or exclusive. */
  auto type() const { return type_; }

 private:
  TValue value_;
  Type type_;
};

}  // namespace utils
