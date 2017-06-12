#pragma once

#include <atomic>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/assert.hpp"

/** A sequentially ordered non-unique
 * lock-free concurrent collection of bits.
 *
 * Grows dynamically to accomodate the maximum
 * set-bit position. Does not dynamically
 * decrease in size.
 *
 * Bits can be set, retrieved and cleared
 * in groups. Note that all group operations
 * fail if the group spans multiple basic
 * storage units. For example, if basic storage
 * is in 8 bits, calling at(5, 2) is legal,
 * but calling at(7, 2) is not.
 *
 * Organizes bits into chunks. Finding the
 * right chunk has has O(n) lookup performance,
 * so use large chunks for better speed in large
 * bitsets. At the same time larger chunks
 * mean coarser-grained memory allocation.
 *
 * @tparam block_t - Basic storage type. Must have
 * lock-free atomic. Should probably always be uint_8.
 * @tparam chunk_size - number of bits in one chunk
 */
template <class block_t = uint8_t, size_t chunk_size = 32768>
class DynamicBitset : Lockable<SpinLock> {
  // basic storage unit
  struct Block {
    Block() = default;

    // the number of bits in one Block
    static constexpr size_t size = sizeof(block_t) * 8;

    block_t at(size_t k, size_t n) const {
      debug_assert(k + n - 1 < size, "Invalid index.");
      return (block.load() >> k) & bitmask(n);
    }

    void set(size_t k, size_t n) {
      debug_assert(k + n - 1 < size, "Invalid index.");
      block.fetch_or(bitmask(n) << k);
    }

    void clear(size_t k, size_t n) {
      debug_assert(k + n - 1 < size, "Invalid index.");
      block.fetch_and(~(bitmask(n) << k));
    }

   private:
    std::atomic<block_t> block{0};

    constexpr block_t bitmask(size_t group_size) const {
      return (block_t)(-1) >> (size - group_size);
    }
  };

  struct Chunk {
    Chunk() : next(nullptr) {
      static_assert(chunk_size % sizeof(block_t) == 0,
                    "chunk size not divisible by block size");
    }

    Chunk(Chunk &) = delete;
    Chunk(Chunk &&) = delete;

    // the number of bits in one chunk
    static constexpr size_t size = chunk_size * Block::size;
    // the number of blocks in one chunk
    static constexpr size_t n_blocks = chunk_size / sizeof(block_t);

    block_t at(size_t k, size_t n) const {
      return blocks[k / Block::size].at(k % Block::size, n);
    }

    void set(size_t k, size_t n) {
      blocks[k / Block::size].set(k % Block::size, n);
    }

    void clear(size_t k, size_t n) {
      blocks[k / Block::size].clear(k % Block::size, n);
    }

    Block blocks[n_blocks];
    std::atomic<Chunk *> next;
  };

 public:
  DynamicBitset(){};

  // can't move nor copy a DynamicBitset because of atomic
  // head and locking
  DynamicBitset(const DynamicBitset &) = delete;
  DynamicBitset(DynamicBitset &&) = delete;
  DynamicBitset &operator=(const DynamicBitset &) = delete;
  DynamicBitset &operator=(DynamicBitset &&) = delete;

  ~DynamicBitset() {
    auto now = head.load();
    while (now != nullptr) {
      auto next = now->next.load();
      delete now;
      now = next;
    }
  }

  /** Gets the block of bit starting at bit k
   * and containing the the following n bits.
   * The bit index with k in this bitset is
   * zeroth bit in the returned value.
   */
  block_t at(size_t k, size_t n) const {
    if (k >= Chunk::size * chunk_count) return 0;

    auto &chunk = find_chunk(k);
    return chunk.at(k, n);
  }

  /** Returns k-th bit's value. */
  bool at(size_t k) const { return at(k, 1); }

  /** Set all the bits in the group of size `n`, starting from
   * bit `k`.
   */
  void set(size_t k, size_t n = 1) {
    auto &chunk = find_or_create_chunk(k);
    return chunk.set(k, n);
  }

  /** Clears all the bits in the group of size `n`, starting
   * from bit `k`.
   * */
  void clear(size_t k, size_t n = 1) {
    // if desired bit is out of bounds, it's already clear
    if (k >= Chunk::size * chunk_count) return;

    auto &chunk = find_or_create_chunk(k);
    return chunk.clear(k, n);
  }

 private:
  // finds the chunk to which k-th bit belong. if k is
  // out of bounds, the chunk for it is created
  Chunk &find_or_create_chunk(size_t &k) {
    Chunk *chunk = head.load(), *next = nullptr;

    // while i'm not in the right chunk
    // (my index is bigger than the size of this chunk)
    while (k >= Chunk::size) {
      next = chunk->next.load();

      // if a next chunk exists, switch to it and decrement my
      // pointer by the size of the current chunk
      if (next != nullptr) {
        chunk = next;
        k -= Chunk::size;
        continue;
      }

      // the next chunk does not exist and we need it. take an exclusive
      // lock to prevent others that also want to create a new chunk
      // from creating it
      auto guard = acquire_unique();

      // double-check locking. if the chunk exists now, some other thread
      // has just created it, continue searching for my chunk
      if (chunk->next.load() != nullptr) continue;

      chunk->next.store(new Chunk());
      chunk_count++;
    }

    debug_assert(chunk != nullptr, "Chunk is nullptr.");
    return *chunk;
  }

  // finds the chunk to which k-th bit belongs.
  // fails if k is out of bounds
  const Chunk &find_chunk(size_t &k) const {
    debug_assert(k < chunk_size * Chunk::size, "Index out  of bounds");
    Chunk *chunk = head.load(), *next = nullptr;

    // while i'm not in the right chunk
    // (my index is bigger than the size of this chunk)
    while (k >= Chunk::size) {
      next = chunk->next.load();

      // if a next chunk exists, switch to it and decrement my
      // pointer by the size of the current chunk
      if (next != nullptr) {
        chunk = next;
        k -= Chunk::size;
        continue;
      }

      // the next chunk does not exist, this is illegal state
      permanent_fail("Out of bounds");
    }

    debug_assert(chunk != nullptr, "Chunk is nullptr.");
    return *chunk;
  }

  std::atomic<Chunk *> head{new Chunk()};
  std::atomic<int64_t> chunk_count{1};
};
