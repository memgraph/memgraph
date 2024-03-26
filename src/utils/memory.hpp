// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
/// This file contains interface and implementation of various MemoryResource
/// and Allocator schemes. It is based on the C++17 <memory_resource> features.

#pragma once

#include <climits>
#include <cstddef>
#include <cstdint>
#include <forward_list>
#include <list>
#include <memory>
#include <mutex>
#include <new>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
// Although <memory_resource> is in C++17, gcc libstdc++ still needs to
// implement it fully. It should be available in the next major release
// version, i.e. gcc 9.x.
#if _GLIBCXX_RELEASE < 9
#include <experimental/memory_resource>
#else
#include <memory_resource>
#endif

#include "utils/logging.hpp"
#include "utils/math.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::utils {

/// std::bad_alloc has no constructor accepting a message, so we wrap our
/// exceptions in this class.
class BadAlloc final : public std::bad_alloc {
  std::string msg_;

 public:
  explicit BadAlloc(std::string msg) : msg_(std::move(msg)) {}

  const char *what() const noexcept override { return msg_.c_str(); }
};

/// Abstract class for writing custom memory management, i.e. allocators.
class MemoryResource {
 public:
  virtual ~MemoryResource() = default;

  /// Allocate storage with a size of at least `bytes` bytes.
  ///
  /// `bytes` must be greater than 0, while `alignment` must be a power of 2, if
  /// they are not, `std::bad_alloc` is thrown.
  ///
  /// Some concrete implementations may have stricter requirements on `bytes`
  /// and `alignment` values.
  ///
  /// @throw std::bad_alloc if the requested storage and alignment combination
  ///     cannot be obtained.
  void *Allocate(size_t bytes, size_t alignment = alignof(std::max_align_t)) {
    if (bytes == 0U || !IsPow2(alignment)) throw BadAlloc("Invalid allocation request");
    return DoAllocate(bytes, alignment);
  }

  void Deallocate(void *p, size_t bytes, size_t alignment = alignof(std::max_align_t)) {
    return DoDeallocate(p, bytes, alignment);
  }

  bool IsEqual(const MemoryResource &other) const noexcept { return DoIsEqual(other); }

 private:
  virtual void *DoAllocate(size_t bytes, size_t alignment) = 0;
  virtual void DoDeallocate(void *p, size_t bytes, size_t alignment) = 0;
  virtual bool DoIsEqual(const MemoryResource &other) const noexcept = 0;
};

inline bool operator==(const MemoryResource &a, const MemoryResource &b) noexcept { return &a == &b || a.IsEqual(b); }

inline bool operator!=(const MemoryResource &a, const MemoryResource &b) noexcept { return !(a == b); }

MemoryResource *NewDeleteResource() noexcept;

/// Allocator for a concrete type T using the underlying MemoryResource
///
/// Allocator does not propagate on container copy construction, copy
/// assignment, move assignment or swap. Propagation is forbidden because of
/// potential lifetime issues between the scope of the allocator and the
/// container using it. This is the same as with std::polymorphic_allocator.
/// As a result, move assignment of Allocator-using container classes may throw,
/// and swapping two Allocator-using containers whose allocators do not compare
/// equal results in undefined behaviour. The allocator does propagate on move
/// construction and Allocator-using containers may have a `noexcept` move
/// constructor.
///
/// The API of the Allocator is made such that it fits with the C++ STL
/// requirements. It can be freely used in STL containers while relying on our
/// implementations of MemoryResource.
///
/// Classes which have a member allocator_type typedef to this allocator will
/// receive an instance of `this->GetMemoryResource()` upon construction if the
/// outer container also uses this allocator. For concrete behaviour on how this
/// is done refer to `std::uses_allocator` in C++ reference.
template <class T>
class Allocator {
 public:
  using value_type = T;
  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::false_type;
  using propagate_on_container_swap = std::false_type;

  /// Implicit conversion from MemoryResource.
  /// This makes working with STL containers much easier.
  Allocator(MemoryResource *memory) : memory_(memory) {}

  template <class U>
  Allocator(const Allocator<U> &other) noexcept : memory_(other.GetMemoryResource()) {}

  template <class U>
  Allocator &operator=(const Allocator<U> &) = delete;

  MemoryResource *GetMemoryResource() const { return memory_; }

  T *allocate(size_t count_elements) {
    return static_cast<T *>(memory_->Allocate(count_elements * sizeof(T), alignof(T)));
  }

  void deallocate(T *p, size_t count_elements) { memory_->Deallocate(p, count_elements * sizeof(T), alignof(T)); }

  /// Return default NewDeleteResource() allocator.
  Allocator select_on_container_copy_construction() const { return utils::NewDeleteResource(); }

  template <class U, class... TArgs>
  void construct(U *ptr, TArgs &&...args) {
    if constexpr (std::uses_allocator_v<U, Allocator>) {
      if constexpr (std::is_constructible_v<U, std::allocator_arg_t, MemoryResource *, TArgs...>) {
        ::new (ptr) U(std::allocator_arg, memory_, std::forward<TArgs>(args)...);
      } else if constexpr (std::is_constructible_v<U, TArgs..., MemoryResource *>) {
        ::new (ptr) U(std::forward<TArgs>(args)..., memory_);
      } else {
        static_assert(!std::uses_allocator_v<U, Allocator>,
                      "Class declares std::uses_allocator but has no valid "
                      "constructor overload. Refer to 'Uses-allocator "
                      "construction' rules in C++ reference.");
      }
    } else {
      ::new (ptr) U(std::forward<TArgs>(args)...);
    }
  }

  // Overloads for constructing a std::pair. Needed until C++20, when allocator
  // propagation to std::pair in std::map is resolved. These are all modeled
  // after std::pmr::polymorphic_allocator<>::construct, documentation
  // referenced here:
  // https://en.cppreference.com/w/cpp/memory/polymorphic_allocator/construct

  template <class T1, class T2, class... Args1, class... Args2>
  void construct(std::pair<T1, T2> *p, std::piecewise_construct_t, std::tuple<Args1...> x, std::tuple<Args2...> y) {
    auto xprime = MakePairElementArguments<T1>(&x);
    auto yprime = MakePairElementArguments<T2>(&y);
    ::new (p) std::pair<T1, T2>(std::piecewise_construct, std::move(xprime), std::move(yprime));
  }

  template <class T1, class T2>
  void construct(std::pair<T1, T2> *p) {
    construct(p, std::piecewise_construct, std::tuple<>(), std::tuple<>());
  }

  template <class T1, class T2, class U, class V>
  void construct(std::pair<T1, T2> *p, U &&x, V &&y) {
    construct(p, std::piecewise_construct, std::forward_as_tuple(std::forward<U>(x)),
              std::forward_as_tuple(std::forward<V>(y)));
  }

  template <class T1, class T2, class U, class V>
  void construct(std::pair<T1, T2> *p, const std::pair<U, V> &xy) {
    construct(p, std::piecewise_construct, std::forward_as_tuple(xy.first), std::forward_as_tuple(xy.second));
  }

  template <class T1, class T2, class U, class V>
  void construct(std::pair<T1, T2> *p, std::pair<U, V> &&xy) {
    construct(p, std::piecewise_construct, std::forward_as_tuple(std::forward<U>(xy.first)),
              std::forward_as_tuple(std::forward<V>(xy.second)));
  }

  template <class U>
  void destroy(U *p) {
    p->~U();
  }

  template <class U, class... TArgs>
  U *new_object(TArgs &&...args) {
    U *p = static_cast<U *>(memory_->Allocate(sizeof(U), alignof(U)));
    try {
      construct(p, std::forward<TArgs>(args)...);
    } catch (...) {
      memory_->Deallocate(p, sizeof(U), alignof(U));
      throw;
    }
    return p;
  }

  template <class U>
  void delete_object(U *p) {
    destroy(p);
    memory_->Deallocate(p, sizeof(U), alignof(U));
  }

 private:
  MemoryResource *memory_;

  template <class TElem, class... TArgs>
  auto MakePairElementArguments(std::tuple<TArgs...> *args) {
    if constexpr (std::uses_allocator_v<TElem, Allocator>) {
      if constexpr (std::is_constructible_v<TElem, std::allocator_arg_t, MemoryResource *, TArgs...>) {
        return std::tuple_cat(std::make_tuple(std::allocator_arg, memory_), std::move(*args));
      } else if constexpr (std::is_constructible_v<TElem, TArgs..., MemoryResource *>) {
        return std::tuple_cat(std::move(*args), std::make_tuple(memory_));
      } else {
        static_assert(!std::uses_allocator_v<TElem, Allocator>,
                      "Class declares std::uses_allocator but has no valid "
                      "constructor overload. Refer to 'Uses-allocator "
                      "construction' rules in C++ reference.");
      }
    } else {
      // Explicitly do a move as we don't want a needless copy of `*args`.
      // Previous return statements return a temporary, so the compiler should
      // optimize that.
      return std::move(*args);
    }
  }
};

template <class T, class U>
bool operator==(const Allocator<T> &a, const Allocator<U> &b) {
  return *a.GetMemoryResource() == *b.GetMemoryResource();
}

template <class T, class U>
bool operator!=(const Allocator<T> &a, const Allocator<U> &b) {
  return !(a == b);
}

auto NullMemoryResource() noexcept -> MemoryResource *;

/// Wraps std::pmr::memory_resource for use with out MemoryResource
class StdMemoryResource final : public MemoryResource {
 public:
#if _GLIBCXX_RELEASE < 9
  StdMemoryResource(std::experimental::pmr::memory_resource *memory) : memory_(memory) {}
#else
  /// Implicitly convert std::pmr::memory_resource to StdMemoryResource
  StdMemoryResource(std::pmr::memory_resource *memory) : memory_(memory) {}
#endif

 private:
  void *DoAllocate(size_t bytes, size_t alignment) override {
    // In the current implementation of libstdc++-8.3, standard memory_resource
    // implementations don't check alignment overflows. Below is the copied
    // implementation of _S_aligned_size, but we throw if it overflows.
    // Currently, this only concerns new_delete_resource as there are no other
    // memory_resource implementations available. This issue appears to persist
    // in newer implementations, additionally pool_resource does no alignment of
    // allocated pointers whatsoever.
    size_t aligned_size = ((bytes - 1) | (alignment - 1)) + 1;
    if (aligned_size < bytes) throw BadAlloc("Allocation alignment overflow");
    return memory_->allocate(bytes, alignment);
  }

  void DoDeallocate(void *p, size_t bytes, size_t alignment) override {
    return memory_->deallocate(p, bytes, alignment);
  }

  bool DoIsEqual(const MemoryResource &other) const noexcept override {
    const auto *other_std = dynamic_cast<const StdMemoryResource *>(&other);
    if (!other_std) return false;
    return *memory_ == *other_std->memory_;
  }

#if _GLIBCXX_RELEASE < 9
  std::experimental::pmr::memory_resource *memory_;
#else
  std::pmr::memory_resource *memory_;
#endif
};

inline MemoryResource *NewDeleteResource() noexcept {
#if _GLIBCXX_RELEASE < 9
  static StdMemoryResource memory(std::experimental::pmr::new_delete_resource());
#else
  static StdMemoryResource memory(std::pmr::new_delete_resource());
#endif
  return &memory;
}

/// MemoryResource which releases the memory only when the resource is
/// destroyed.
///
/// MonotonicBufferResource is not thread-safe!
///
/// It's meant to be used for very fast allocations in situations where memory
/// is used to build objects and release them all at once. The class is
/// constructed with initial buffer size for storing allocated objects. When the
/// buffer is exhausted, a new one is requested from the upstream memory
/// resource.
///
/// Note that each buffer of memory is actually a larger block of at *least*
/// `(size + sizeof(Buffer))` bytes due to bookkeeping `Buffer` object.
class MonotonicBufferResource final : public MemoryResource {
 public:
  /// Construct the resource with the buffer size of at least `initial_size`.
  ///
  /// Uses the default NewDeleteResource for requesting buffer memory.
  explicit MonotonicBufferResource(size_t initial_size);

  /// Construct the resource with the buffer size of at least `initial_size` and
  /// use the `memory` as the upstream resource.
  MonotonicBufferResource(size_t initial_size, MemoryResource *memory);

  /// Construct the resource with the initial buffer set to `buffer` of given
  /// `buffer_size`.
  ///
  /// This memory resource does not take ownership of `buffer` and will
  /// therefore not be freed in the destructor nor `Release` call. The `Release`
  /// call will indeed setup the `buffer` for reuse. Additional buffers are
  /// allocated from the given upstream `memory` resource.
  MonotonicBufferResource(void *buffer, size_t buffer_size, MemoryResource *memory = utils::NewDeleteResource());

  MonotonicBufferResource(const MonotonicBufferResource &) = delete;
  MonotonicBufferResource &operator=(const MonotonicBufferResource &) = delete;

  MonotonicBufferResource(MonotonicBufferResource &&other) noexcept;
  MonotonicBufferResource &operator=(MonotonicBufferResource &&other) noexcept;

  ~MonotonicBufferResource() override { Release(); }

  /// Release all allocated memory by calling Deallocate on upstream
  /// MemoryResource.
  ///
  /// All memory is released even though Deallocate may have not been called on
  /// this instance for some of the allocated blocks.
  void Release();

  MemoryResource *GetUpstreamResource() const { return memory_; }

 private:
  struct Buffer {
    Buffer *next;
    size_t capacity;
    size_t alignment;

    /// Get the size of the area reserved for `this`
    size_t bytes_for_buffer() const { return *RoundUint64ToMultiple(sizeof(*this), alignment); }

    /// Get total allocated size.
    size_t size() const { return bytes_for_buffer() + capacity; }

    /// Get the pointer to data which is after the Buffer instance itself.
    char *data() { return reinterpret_cast<char *>(this) + bytes_for_buffer(); }
  };

  MemoryResource *memory_{NewDeleteResource()};
  Buffer *current_buffer_{nullptr};
  void *initial_buffer_{nullptr};
  size_t initial_size_{0U};
  size_t next_buffer_size_{initial_size_};
  size_t allocated_{0U};

  void *DoAllocate(size_t bytes, size_t alignment) override;

  void DoDeallocate(void *, size_t, size_t) override {}

  bool DoIsEqual(const MemoryResource &other) const noexcept override { return this == &other; }
};

namespace impl {

template <class T>
using AList = std::forward_list<T, Allocator<T>>;

template <class T>
using AVector = std::vector<T, Allocator<T>>;

/// Holds a number of Chunks each serving blocks of particular size. When a
/// Chunk runs out of available blocks, a new Chunk is allocated.
class Pool final {
  /// Holds a pointer into a chunk of memory which consists of equal sized
  /// blocks. Blocks form a "free-list"
  struct Chunk {
    // TODO: make blocks_per_chunk a per chunk thing (ie. allow chunk growth)
    std::byte *raw_data;
    explicit Chunk(std::byte *rawData) : raw_data(rawData) {}
    std::byte *build_freelist(std::size_t block_size, std::size_t blocks_in_chunk) {
      auto current = raw_data;
      std::byte *prev = nullptr;
      auto end = current + (blocks_in_chunk * block_size);
      while (current != end) {
        std::byte **list_entry = reinterpret_cast<std::byte **>(current);
        *list_entry = std::exchange(prev, current);
        current += block_size;
      }
      DMG_ASSERT(prev != nullptr);
      return prev;
    }
  };

  std::byte *free_list_{nullptr};
  uint8_t blocks_per_chunk_{};
  std::size_t block_size_{};

  AList<Chunk> chunks_;  // TODO: do ourself so we can do fast Release (detect monotonic, do nothing)

 public:
  static constexpr auto MaxBlocksInChunk = std::numeric_limits<decltype(blocks_per_chunk_)>::max();

  Pool(size_t block_size, unsigned char blocks_per_chunk, MemoryResource *chunk_memory);

  Pool(const Pool &) = delete;
  Pool &operator=(const Pool &) = delete;
  Pool(Pool &&) noexcept = default;
  Pool &operator=(Pool &&) = default;

  /// Destructor does not free blocks, you have to call `Release` before.
  ~Pool();

  MemoryResource *GetUpstreamResource() const { return chunks_.get_allocator().GetMemoryResource(); }

  auto GetBlockSize() const { return block_size_; }

  /// Get a pointer to the next available block. Blocks are stored contiguously,
  /// so each one is aligned to block_size_ address starting from
  /// utils::Ceil2(block_size_) address.
  void *Allocate();

  void Deallocate(void *p);
};

// C++ overloads for clz
constexpr auto clz(unsigned int x) { return __builtin_clz(x); }
constexpr auto clz(unsigned long x) { return __builtin_clzl(x); }
constexpr auto clz(unsigned long long x) { return __builtin_clzll(x); }

template <typename T>
constexpr auto bits_sizeof = sizeof(T) * CHAR_BIT;

/// 0-based bit index of the most significant bit assumed that `n` != 0
template <typename T>
constexpr auto msb_index(T n) {
  return bits_sizeof<T> - clz(n) - T(1);
}

/* This function will in O(1) time provide a bin index based on:
 * B  - the number of most significant bits to be sensitive to
 * LB - the value that should be considered below the consideration for bin index of 0 (LB is exclusive)
 *
 * lets say we were:
 * - sensitive to two bits (B == 2)
 * - lowest bin is for 8 (LB == 8)
 *
 * our bin indexes would look like:
 *   0 - 0000'1100 12
 *   1 - 0001'0000 16
 *   2 - 0001'1000 24
 *   3 - 0010'0000 32
 *   4 - 0011'0000 48
 *   5 - 0100'0000 64
 *   6 - 0110'0000 96
 *   7 - 1000'0000 128
 *   8 - 1100'0000 192
 *   ...
 *
 * Example:
 * Given n == 70, we want to return the bin index to the first value which is
 * larger than n.
 * bin_index<2,8>(70) => 6, as 64 (index 5) < 70 and 70 <= 96 (index 6)
 */
template <std::size_t B = 2, std::size_t LB = 8>
constexpr std::size_t bin_index(std::size_t n) {
  static_assert(B >= 1U, "Needs to be sensitive to at least one bit");
  static_assert(LB != 0U, "Lower bound need to be non-zero");
  DMG_ASSERT(n > LB);

  // We will alway be sensitive to at least the MSB
  // exponent tells us how many bits we need to use to select within a level
  constexpr auto kExponent = B - 1U;
  // 2^exponent gives the size of each level
  constexpr auto kSize = 1U << kExponent;
  // offset help adjust results down to be inline with bin_index(LB) == 0
  constexpr auto kOffset = msb_index(LB);

  auto const msb_idx = msb_index(n);
  DMG_ASSERT(msb_idx != 0);

  auto const mask = (1u << msb_idx) - 1u;
  auto const under = n & mask;
  auto const selector = under >> (msb_idx - kExponent);

  auto const rest = under & (mask >> kExponent);
  auto const no_overflow = rest == 0U;

  auto const msb_level = kSize * (msb_idx - kOffset);
  return msb_level + selector - no_overflow;
}

// This is the inverse opperation for bin_index
// bin_size(bin_index(X)-1) < X <= bin_size(bin_index(X))
template <std::size_t B = 2, std::size_t LB = 8>
std::size_t bin_size(std::size_t idx) {
  constexpr auto kExponent = B - 1U;
  constexpr auto kSize = 1U << kExponent;
  constexpr auto kOffset = msb_index(LB);

  // no need to optimise `/` or `%` compiler can see `kSize` is a power of 2
  auto const level = (idx + 1) / kSize;
  auto const sub_level = (idx + 1) % kSize;
  return (1U << (level + kOffset)) | (sub_level << (level + kOffset - kExponent));
}

template <std::size_t Bits, std::size_t LB, std::size_t UB>
struct MultiPool {
  static_assert(LB < UB, "lower bound must be less than upper bound");
  static_assert(IsPow2(LB) && IsPow2(UB), "Design untested for non powers of 2");
  static_assert((LB << Bits) % sizeof(void *) == 0, "Smallest pool must have space and alignment for freelist");

  // upper bound is inclusive
  static bool is_size_handled(std::size_t size) { return LB < size && size <= UB; }
  static bool is_above_upper_bound(std::size_t size) { return UB < size; }

  static constexpr auto n_bins = bin_index<Bits, LB>(UB) + 1U;

  MultiPool(uint8_t blocks_per_chunk, MemoryResource *memory, MemoryResource *internal_memory)
      : blocks_per_chunk_{blocks_per_chunk}, memory_{memory}, internal_memory_{internal_memory} {}

  ~MultiPool() {
    if (pools_) {
      auto pool_alloc = Allocator<Pool>(internal_memory_);
      for (auto i = 0U; i != n_bins; ++i) {
        pool_alloc.destroy(&pools_[i]);
      }
      pool_alloc.deallocate(pools_, n_bins);
    }
  }

  void *allocate(std::size_t bytes) {
    auto idx = bin_index<Bits, LB>(bytes);
    if (!pools_) [[unlikely]] {
      initialise_pools();
    }
    return pools_[idx].Allocate();
  }

  void deallocate(void *ptr, std::size_t bytes) {
    auto idx = bin_index<Bits, LB>(bytes);
    pools_[idx].Deallocate(ptr);
  }

 private:
  void initialise_pools() {
    auto pool_alloc = Allocator<Pool>(internal_memory_);
    auto pools = pool_alloc.allocate(n_bins);
    try {
      for (auto i = 0U; i != n_bins; ++i) {
        auto block_size = bin_size<Bits, LB>(i);
        pool_alloc.construct(&pools[i], block_size, blocks_per_chunk_, memory_);
      }
      pools_ = pools;
    } catch (...) {
      pool_alloc.deallocate(pools, n_bins);
      throw;
    }
  }

  Pool *pools_{};
  uint8_t blocks_per_chunk_{};
  MemoryResource *memory_{};
  MemoryResource *internal_memory_{};
};

}  // namespace impl

/// MemoryResource which serves allocation requests for different block sizes.
///
/// PoolResource is not thread-safe!
///
/// This class has the following properties with regards to memory management.
///
///   * It consists of a collection of impl::Pool instances, each serving
///     requests for different block sizes. Each impl::Pool manages a collection
///     of impl::Pool::Chunk instances which are divided into blocks of uniform
///     size.
///   * Since this MemoryResource serves blocks of certain size, it cannot serve
///     arbitrary alignment requests. Each requested block size must be a
///     multiple of alignment or smaller than the alignment value.
///   * An allocation request within the limits of the maximum block size will
///     find a Pool serving the requested size. Some requests will share a larger
///     pool size.
///   * When a Pool exhausts its Chunk, a new one is allocated with the size for
///     the maximum number of blocks.
///   * Allocation requests which exceed the maximum block size will be
///     forwarded to upstream MemoryResource.
///   * Maximum number of blocks per chunk can be tuned by passing the
///     arguments to the constructor.

class PoolResource final : public MemoryResource {
 public:
  PoolResource(uint8_t blocks_per_chunk, MemoryResource *memory = NewDeleteResource(),
                MemoryResource *internal_memory = NewDeleteResource())
      : mini_pools_{
            impl::Pool{8, blocks_per_chunk, memory},
            impl::Pool{16, blocks_per_chunk, memory},
            impl::Pool{24, blocks_per_chunk, memory},
            impl::Pool{32, blocks_per_chunk, memory},
            impl::Pool{40, blocks_per_chunk, memory},
            impl::Pool{48, blocks_per_chunk, memory},
            impl::Pool{56, blocks_per_chunk, memory},
            impl::Pool{64, blocks_per_chunk, memory},
        },
        pools_3bit_(blocks_per_chunk, memory, internal_memory),
        pools_4bit_(blocks_per_chunk, memory, internal_memory),
        pools_5bit_(blocks_per_chunk, memory, internal_memory),
        unpooled_memory_{internal_memory} {}
  ~PoolResource() override = default;

 private:
  void *DoAllocate(size_t bytes, size_t alignment) override;
  void DoDeallocate(void *p, size_t bytes, size_t alignment) override;
  bool DoIsEqual(MemoryResource const &other) const noexcept override;

 private:
  std::array<impl::Pool, 8> mini_pools_;
  impl::MultiPool<3, 64, 128> pools_3bit_;
  impl::MultiPool<4, 128, 512> pools_4bit_;
  impl::MultiPool<5, 512, 1024> pools_5bit_;
  MemoryResource *unpooled_memory_;
};

class MemoryTrackingResource final : public utils::MemoryResource {
 public:
  explicit MemoryTrackingResource(utils::MemoryResource *memory, size_t max_allocated_bytes)
      : memory_(memory), max_allocated_bytes_(max_allocated_bytes) {}

  size_t GetAllocatedBytes() const noexcept { return max_allocated_bytes_ - available_bytes_; }

 private:
  utils::MemoryResource *memory_;
  size_t max_allocated_bytes_;
  size_t available_bytes_{max_allocated_bytes_};

  void *DoAllocate(size_t bytes, size_t alignment) override {
    available_bytes_ -= bytes;
    return memory_->Allocate(bytes, alignment);
  }

  void DoDeallocate(void *p, size_t bytes, size_t alignment) override {
    available_bytes_ += bytes;
    return memory_->Deallocate(p, bytes, alignment);
  }

  bool DoIsEqual(const MemoryResource &other) const noexcept override { return this == &other; }
};

// Allocate memory with the OutOfMemoryException enabled if the requested size
// puts total allocated amount over the limit.
class ResourceWithOutOfMemoryException : public MemoryResource {
 public:
  explicit ResourceWithOutOfMemoryException(utils::MemoryResource *upstream = utils::NewDeleteResource())
      : upstream_{upstream} {}

  utils::MemoryResource *GetUpstream() noexcept { return upstream_; }

 private:
  void *DoAllocate(size_t bytes, size_t alignment) override {
    utils::MemoryTracker::OutOfMemoryExceptionEnabler exception_enabler;
    return upstream_->Allocate(bytes, alignment);
  }

  void DoDeallocate(void *p, size_t bytes, size_t alignment) override { upstream_->Deallocate(p, bytes, alignment); }

  bool DoIsEqual(const utils::MemoryResource &other) const noexcept override { return upstream_->IsEqual(other); }

  MemoryResource *upstream_{utils::NewDeleteResource()};
};
}  // namespace memgraph::utils
