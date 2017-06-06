#pragma once

// TODO: implement better hash function

/**
 * Class calculates hash of the data dynamically.
 */
class Hasher {
 public:
  Hasher() = default;
  /**
   * Sets hash to 0.
   */
  void Reset() { hash_ = 0; }
  /**
   * Updates hash from given data.
   * @param data data from which hash will be updated
   * @param n length of the data
   */
  void Update(const uint8_t *data, size_t n) {
    for (int i = 0; i < n; ++i) hash_ = hash_ * kPrime + data[i] + 1;
  }
  /**
   * Returns current hash value.
   */
  uint64_t hash() const { return hash_; }

 private:
  /**
   * Prime number used in calculating hash.
   */
  const uint64_t kPrime = 3137;
  /**
   * Hash of data.
   */
  uint64_t hash_ = 0;
};
