#pragma once

#include <cstdint>
#include <cstdlib>

// TODO: implement better hash function

/**
 * Class calculates hash of the data dynamically.
 */
class Hasher {
  /** Prime number used in calculating hash. */
  static constexpr uint64_t kPrime = 3137;

 public:
  /**
   * Updates hash from given data.
   *
   * @param data  - Data from which hash will be updated.
   * @param n - Length of the data.
   */
  void Update(const uint8_t *data, size_t n) {
    for (size_t i = 0; i < n; ++i) hash_ = hash_ * kPrime + data[i] + 1;
  }

  /** Returns current hash value. */
  uint64_t hash() const { return hash_; }

 private:
  uint64_t hash_ = 0;
};
