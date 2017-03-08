#pragma once

#include <algorithm>
#include <bitset>
#include <iostream>
#include <string>

#include "utils/assert.hpp"

/**
 * Bitset data structure with a number of bits provided in constructor.
 * @tparam TStore type of underlying bit storage: int32, int64, char, etc.
 */
template <typename TStore>
class Bitset {
 public:
  /**
   *  Create bitset.
   *  @param sz size of bitset
   */
  Bitset(size_t sz) : block_size_(8 * sizeof(TStore)) {
    if (sz % block_size_ != 0) sz += block_size_;
    blocks_.resize(sz / block_size_);
  }
  /**
   *  Set bit to one.
   *  @param idx position of bit.
   */
  void Set(int idx) {
    debug_assert(idx >= 0, "Invalid bit location.");
    debug_assert(idx < static_cast<int64_t>(blocks_.size()) * block_size_,
                 "Invalid bit location.");
    int bucket = idx / block_size_;
    blocks_[bucket] |= TStore(1) << idx % block_size_;
  }
  /**
   *  Return bit at position.
   *  @param idx position of bit.
   *  @return 1/0.
   */
  bool At(int idx) const {
    debug_assert(idx >= 0, "Invalid bit location.");
    debug_assert(idx < static_cast<int64_t>(blocks_.size()) * block_size_,
                 "Invalid bit location.");
    int bucket = idx / block_size_;
    return (blocks_[bucket] >> (idx % block_size_)) & 1;
  }
  /**
   *  Intersect two bitsets
   *  @param other bitset.
   *  @return intersection.
   */
  Bitset<TStore> Intersect(const Bitset<TStore> &other) const {
    debug_assert(this->blocks_.size() == other.blocks_.size(),
                 "Bitsets are not of equal size.");
    Bitset<TStore> ret(this->blocks_.size() * this->block_size_);
    for (int i = 0; i < (int)this->blocks_.size(); ++i) {
      ret.blocks_[i] = this->blocks_[i] & other.blocks_[i];
      continue;
    }
    return ret;
  }
  /**
   *  Positions of bits set to 1.
   *  @return positions of bits set to 1.
   */
  std::vector<int> Ones() const {
    std::vector<int> ret;
    int ret_idx = 0;
    for (auto x : blocks_) {
      while (x) {
        auto pos = CountTrailingZeroes(x & -x);
        x -= x & -x;
        ret.push_back(ret_idx + pos);
      }
      ret_idx += block_size_;
    }
    return ret;
  }

 private:
  /**
   * Calculate number of trailing zeroes in a binary number, usually a power
   * of two.
   * @return number of trailing zeroes.
   */
  size_t CountTrailingZeroes(TStore v) const {
    size_t ret = 0;
    while (v >> 32) ret += 32, v >>= 32;
    if (v >> 16) ret += 16, v >>= 16;
    if (v >> 8) ret += 8, v >>= 8;
    if (v >> 4) ret += 4, v >>= 4;
    if (v >> 2) ret += 2, v >>= 2;
    if (v >> 1) ret += 1, v >>= 1;
    return ret;
  }
  std::vector<TStore> blocks_;
  const size_t block_size_;
};
