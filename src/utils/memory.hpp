// Copyright 2023 Memgraph Ltd.
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

#include <cstddef>
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
  explicit BadAlloc(const std::string &msg) : msg_(msg) {}

  const char *what() const noexcept override { return msg_.c_str(); }
};

/// Abstract class for writing custom memory management, i.e. allocators.
class MemoryResource {
 public:
  virtual ~MemoryResource() {}

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
using AVector = std::vector<T, Allocator<T>>;

/// Holds a number of Chunks each serving blocks of particular size. When a
/// Chunk runs out of available blocks, a new Chunk is allocated. The naming is
/// taken from `libstdc++` implementation, but the implementation details are
/// more similar to `FixedAllocator` described in "Small Object Allocation" from
/// "Modern C++ Design".
class Pool final {
  /// Holds a pointer into a chunk of memory which consists of equal sized
  /// blocks. Each Chunk can handle `std::numeric_limits<unsigned char>::max()`
  /// number of blocks. Blocks form a "free-list", where each unused block has
  /// an embedded index to the next unused block.
  struct Chunk {
    unsigned char *data;
    unsigned char first_available_block_ix;
    unsigned char blocks_available;
  };

  unsigned char blocks_per_chunk_;
  size_t block_size_;
  AVector<Chunk> chunks_;
  Chunk *last_alloc_chunk_{nullptr};
  Chunk *last_dealloc_chunk_{nullptr};

 public:
  static constexpr auto MaxBlocksInChunk() {
    return std::numeric_limits<decltype(Chunk::first_available_block_ix)>::max();
  }

  Pool(size_t block_size, unsigned char blocks_per_chunk, MemoryResource *memory);

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

  void Release();
};

}  // namespace impl

/// MemoryResource which serves allocation requests for different block sizes.
///
/// PoolResource is not thread-safe!
///
/// This class has the following properties with regards to memory management.
///
///   * All allocated memory will be freed upon destruction, even if Deallocate
///     has not been called for some of the allocated blocks.
///   * It consists of a collection of impl::Pool instances, each serving
///     requests for different block sizes. Each impl::Pool manages a collection
///     of impl::Pool::Chunk instances which are divided into blocks of uniform
///     size.
///   * Since this MemoryResource serves blocks of certain size, it cannot serve
///     arbitrary alignment requests. Each requested block size must be a
///     multiple of alignment or smaller than the alignment value.
///   * An allocation request within the limits of the maximum block size will
///     find a Pool serving the requested size. If there's no Pool serving such
///     a request, a new one is instantiated.
///   * When a Pool exhausts its Chunk, a new one is allocated with the size for
///     the maximum number of blocks.
///   * Allocation requests which exceed the maximum block size will be
///     forwarded to upstream MemoryResource.
///   * Maximum block size and maximum number of blocks per chunk can be tuned
///     by passing the arguments to the constructor.
class PoolResource final : public MemoryResource {
 public:
  /// Construct with given max_blocks_per_chunk, max_block_size and upstream
  /// memory.
  ///
  /// The implementation will use std::min(max_blocks_per_chunk,
  /// impl::Pool::MaxBlocksInChunk()) as the real maximum number of blocks per
  /// chunk. Allocation requests exceeding max_block_size are simply forwarded
  /// to upstream memory.
  PoolResource(size_t max_blocks_per_chunk, size_t max_block_size, MemoryResource *memory_pools = NewDeleteResource(),
               MemoryResource *memory_unpooled = NewDeleteResource());

  PoolResource(const PoolResource &) = delete;
  PoolResource &operator=(const PoolResource &) = delete;

  PoolResource(PoolResource &&) = default;
  PoolResource &operator=(PoolResource &&) = default;

  ~PoolResource() override { Release(); }

  MemoryResource *GetUpstreamResource() const { return pools_.get_allocator().GetMemoryResource(); }
  MemoryResource *GetUpstreamResourceBlocks() const { return unpooled_.get_allocator().GetMemoryResource(); }

  /// Release all allocated memory.
  void Release();

 private:
  // Big block larger than max_block_size_, doesn't go into a pool.
  struct BigBlock {
    size_t bytes;
    size_t alignment;
    void *data;
  };

  // TODO: Potential memory optimization is replacing `std::vector` with our
  // custom vector implementation which doesn't store a `MemoryResource *`.
  // Currently we have vectors for `pools_` and `unpooled_`, as well as each
  // `impl::Pool` stores a `chunks_` vector.

  // Pools are sorted by bound_size_, ascending.
  impl::AVector<impl::Pool> pools_;
  impl::Pool *last_alloc_pool_{nullptr};
  impl::Pool *last_dealloc_pool_{nullptr};
  // Unpooled BigBlocks are sorted by data pointer.
  impl::AVector<BigBlock> unpooled_;
  size_t max_blocks_per_chunk_;
  size_t max_block_size_;

  void *DoAllocate(size_t bytes, size_t alignment) override;

  void DoDeallocate(void *p, size_t bytes, size_t alignment) override;

  bool DoIsEqual(const MemoryResource &other) const noexcept override { return this == &other; }
};

/// Like PoolResource but uses SpinLock for thread safe usage.
class SynchronizedPoolResource final : public MemoryResource {
 public:
  SynchronizedPoolResource(size_t max_blocks_per_chunk, size_t max_block_size,
                           MemoryResource *memory = NewDeleteResource())
      : pool_memory_(max_blocks_per_chunk, max_block_size, memory) {}

 private:
  PoolResource pool_memory_;
  SpinLock lock_;

  void *DoAllocate(size_t bytes, size_t alignment) override {
    std::lock_guard<SpinLock> guard(lock_);
    return pool_memory_.Allocate(bytes, alignment);
  }

  void DoDeallocate(void *p, size_t bytes, size_t alignment) override {
    std::lock_guard<SpinLock> guard(lock_);
    pool_memory_.Deallocate(p, bytes, alignment);
  }

  bool DoIsEqual(const MemoryResource &other) const noexcept override { return this == &other; }
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
