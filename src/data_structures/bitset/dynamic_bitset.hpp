#pragma once

#include <atomic>

#include "glog/logging.h"

/**
 * A sequentially ordered non-unique lock-free concurrent collection of bits.
 *
 * Grows dynamically to accomodate the maximum set-bit position. Does not
 * dynamically decrease in size.
 *
 * Bits can be set, retrieved and cleared in groups. Note that all group
 * operations fail if the group spans multiple basic storage units. For example,
 * if basic storage is in 8 bits, calling at(5, 2) is legal, but calling at(7,
 * 2) is not.
 *
 * @tparam block_t - Basic storage type. Must have lock-free atomic. Should
 * probably always be uint_8.
 * @tparam chunk_size - number of bits in one chunk
 */
template <class block_t = uint8_t, size_t chunk_size = 32768>
class DynamicBitset {
  // Basic storage unit.
  struct Block {
    Block() = default;

    Block(const Block &) = delete;
    Block(Block &&) = delete;
    Block &operator=(const Block &) = delete;
    Block &operator=(Block &&) = delete;

    // The number of bits in one Block.
    static constexpr size_t kSize = sizeof(block_t) * 8;

    block_t at(size_t k, size_t n) const {
      DCHECK(k + n - 1 < kSize) << "Invalid index.";
      return (block_.load() >> k) & bitmask(n);
    }

    void set(size_t k, size_t n) {
      DCHECK(k + n - 1 < kSize) << "Invalid index.";
      block_.fetch_or(bitmask(n) << k);
    }

    void clear(size_t k, size_t n) {
      DCHECK(k + n - 1 < kSize) << "Invalid index.";
      block_.fetch_and(~(bitmask(n) << k));
    }

   private:
    std::atomic<block_t> block_{0};

    constexpr block_t bitmask(size_t group_size) const {
      return (block_t)(-1) >> (kSize - group_size);
    }
  };

  struct Chunk {
    Chunk(Chunk *next, int64_t chunk_id) : next_(next), chunk_id_(chunk_id) {
      static_assert(chunk_size % Block::kSize == 0,
                    "chunk size not divisible by block size");
    }

    Chunk(const Chunk &) = delete;
    Chunk(Chunk &&) = delete;
    Chunk &operator=(const Chunk &) = delete;
    Chunk &operator=(Chunk &&) = delete;

    // The number of bits in one chunk.
    static constexpr size_t kSize = chunk_size;
    // The number of blocks_ in one chunk.
    static constexpr size_t kNumBlocks = chunk_size / Block::kSize;

    block_t at(size_t k, size_t n) const {
      return blocks_[k / Block::kSize].at(k % Block::kSize, n);
    }

    void set(size_t k, size_t n) {
      blocks_[k / Block::kSize].set(k % Block::kSize, n);
    }

    void clear(size_t k, size_t n) {
      blocks_[k / Block::kSize].clear(k % Block::kSize, n);
    }

    // Range of the bits stored in this chunk is [low, high>.
    int64_t low() const { return chunk_id_ * kSize; }

    int64_t high() const { return (chunk_id_ + 1) * kSize; }

    Block blocks_[kNumBlocks];
    std::atomic<Chunk *> next_;
    const int64_t chunk_id_;
  };

 public:
  DynamicBitset() {}

  // Can't move nor copy a DynamicBitset because of atomic head and locking.
  DynamicBitset(const DynamicBitset &) = delete;
  DynamicBitset(DynamicBitset &&) = delete;
  DynamicBitset &operator=(const DynamicBitset &) = delete;
  DynamicBitset &operator=(DynamicBitset &&) = delete;

  ~DynamicBitset() {
    auto now = head_.load();
    while (now != nullptr) {
      auto next = now->next_.load();
      delete now;
      now = next;
    }
  }

  /**
   * Gets the block of bit starting at bit k and containing the the following n
   * bits. The bit index with k in this bitset is zeroth bit in the returned
   * value.
   */
  block_t at(size_t k, size_t n) const {
    if (k >= head_.load()->high()) return 0;

    const auto &chunk = FindChunk(k);
    return chunk.at(k, n);
  }

  /** Returns k-th bit's value. */
  bool at(size_t k) const { return at(k, 1); }

  /**
   * Set all the bits in the group of size `n`, starting from bit `k`.
   */
  void set(size_t k, size_t n = 1) {
    auto &chunk = FindOrCreateChunk(k);
    return chunk.set(k, n);
  }

  /**
   * Clears all the bits in the group of size `n`, starting from bit `k`.
   */
  void clear(size_t k, size_t n = 1) {
    // If desired bit is out of bounds, it's already clear.
    if (k >= head_.load()->high()) return;

    auto &chunk = FindOrCreateChunk(k);
    return chunk.clear(k, n);
  }

  /**
   * Deletes all blocks which contain all positions lower than pos, assumes that
   * there doesn't exist a pointer to those blocks, i.e. it's safe to delete
   * them
   */
  void delete_prefix(size_t pos) {
    // Never delete head as that might invalidate the whole structure which
    // depends on head being available
    Chunk *last = head_.load();
    Chunk *chunk = last->next_;

    // High is exclusive endpoint of interval
    while (chunk != nullptr && chunk->high() > pos) {
      last = chunk;
      chunk = chunk->next_;
    }

    if (chunk != nullptr) {
      // Unlink from last
      last->next_ = nullptr;
      // Deletes chunks
      Chunk *next;
      while (chunk) {
        next = chunk->next_;
        delete chunk;
        chunk = next;
      }
    }
  }

 private:
  // Finds the chunk to which k-th bit belongs fails if k is out of bounds.
  const Chunk &FindChunk(size_t &k) const {
    DCHECK(k < head_.load()->high()) << "Index out of bounds";
    Chunk *chunk = head_;

    while (k < chunk->low()) {
      chunk = chunk->next_;
      CHECK(chunk != nullptr) << "chunk is nullptr";
    }
    k -= chunk->low();
    return *chunk;
  }

  /**
   * Finds the chunk to which k-th bit belong. If k is out of bounds, the chunk
   * for it is created.
   */
  Chunk &FindOrCreateChunk(size_t &k) {
    Chunk *head = head_;

    while (k >= head->high()) {
      // The next chunk does not exist and we need it, so we will try to create
      // it.
      Chunk *new_head = new Chunk(head, head->chunk_id_ + 1);
      if (!head_.compare_exchange_strong(head, new_head)) {
        // Other thread updated head_ before us, so we need to delete new_head.
        head = head_;
        delete new_head;
        continue;
      }
    }

    // Now we are sure chunk exists and we can call find function.
    // const_cast is used to avoid code duplication.
    return const_cast<Chunk &>(FindChunk(k));
  }

  std::atomic<Chunk *> head_{new Chunk(nullptr, 0)};
};
