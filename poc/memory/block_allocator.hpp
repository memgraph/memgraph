#pragma once

#include <memory>
#include <vector>

#include "utils/auto_scope.hpp"

/* @brief Allocates blocks of block_size and stores
 * the pointers on allocated blocks inside a vector.
 */
template <size_t block_size>
class BlockAllocator {
  struct Block {
    Block() { data = malloc(block_size); }

    Block(void *ptr) { data = ptr; }

    void *data;
  };

 public:
  static constexpr size_t size = block_size;

  BlockAllocator(size_t capacity = 0) {
    for (size_t i = 0; i < capacity; ++i) unused_.emplace_back();
  }

  ~BlockAllocator() {
    for (auto block : unused_) free(block.data);
    unused_.clear();
    for (auto block : release_) free(block.data);
    release_.clear();
  }

  size_t unused_size() const { return unused_.size(); }

  size_t release_size() const { return release_.size(); }

  // Returns nullptr on no memory.
  void *acquire() {
    if (unused_.size() == 0) unused_.emplace_back();

    auto ptr = unused_.back().data;
    Auto(unused_.pop_back());
    return ptr;
  }

  void release(void *ptr) { release_.emplace_back(ptr); }

 private:
  // TODO: try implement with just one vector
  // but consecutive acquire release calls should work
  // TODO: measure first!
  std::vector<Block> unused_;
  std::vector<Block> release_;
};
