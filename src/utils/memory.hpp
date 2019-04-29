/// @file
/// This file contains interface and implementation of various MemoryResource
/// and Allocator schemes. It is based on the C++17 <memory_resource> features.

#pragma once

#include <cstddef>
// Although <memory_resource> is in C++17, gcc libstdc++ still needs to
// implement it fully. It should be available in the next major release
// version, i.e. gcc 9.x.
#include <experimental/memory_resource>

namespace utils {

/// Abstract class for writing custom memory management, i.e. allocators.
class MemoryResource {
 public:
  virtual ~MemoryResource() {}

  /// Allocate storage with a size of at least `bytes` bytes.
  ///
  /// The returned storage is aligned to to `alignment` clamped to
  /// `alignof(std::max_align_t)`. This means that it is valid to request larger
  /// alignment, but the storage will actually be aligned to
  /// `alignof(std::max_align_t)`.
  //
  /// Additionaly, `alignment` must be a power of 2, if it is not
  /// `std::bad_alloc` is thrown.
  ///
  /// @throw std::bad_alloc if the requested storage and alignment combination
  ///     cannot be obtained.
  void *Allocate(size_t bytes, size_t alignment = alignof(std::max_align_t)) {
    return DoAllocate(bytes, alignment);
  }

  void Deallocate(void *p, size_t bytes,
                  size_t alignment = alignof(std::max_align_t)) {
    return DoDeallocate(p, bytes, alignment);
  }

  bool IsEqual(const MemoryResource &other) const noexcept {
    return DoIsEqual(other);
  }

 private:
  virtual void *DoAllocate(size_t bytes, size_t alignment) = 0;
  virtual void DoDeallocate(void *p, size_t bytes, size_t alignment) = 0;
  virtual bool DoIsEqual(const MemoryResource &other) const noexcept = 0;
};

inline bool operator==(const MemoryResource &a,
                       const MemoryResource &b) noexcept {
  return &a == &b || a.IsEqual(b);
}

inline bool operator!=(const MemoryResource &a,
                       const MemoryResource &b) noexcept {
  return !(a == b);
}

/// Allocator for a concrete type T using the underlying MemoryResource
///
/// The API of the Allocator is made such that it fits with the C++ STL
/// requirements. It can be freely used in STL containers while relying on our
/// implementations of MemoryResource.
template <class T>
class Allocator {
 public:
  using value_type = T;

  /// Implicit conversion from MemoryResource.
  /// This makes working with STL containers much easier.
  Allocator(MemoryResource *memory) : memory_(memory) {}

  template <class U>
  Allocator(const Allocator<U> &other) noexcept
      : memory_(other.GetMemoryResource()) {}

  template <class U>
  Allocator &operator=(const Allocator<U> &) = delete;

  T *allocate(size_t count_elements) {
    return static_cast<T *>(
        memory_->Allocate(count_elements * sizeof(T), alignof(T)));
  }

  void deallocate(T *p, size_t count_elements) {
    memory_->Deallocate(p, count_elements * sizeof(T), alignof(T));
  }

  MemoryResource *GetMemoryResource() const { return memory_; }

 private:
  MemoryResource *memory_;
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
  /// Implicitly convert std::pmr::memory_resource to StdMemoryResource
  StdMemoryResource(std::experimental::pmr::memory_resource *memory)
      : memory_(memory) {}

 private:
  void *DoAllocate(size_t bytes, size_t alignment) override {
    return memory_->allocate(bytes, alignment);
  }

  void DoDeallocate(void *p, size_t bytes, size_t alignment) override {
    return memory_->deallocate(p, bytes, alignment);
  }

  bool DoIsEqual(const MemoryResource &other) const noexcept override {
    const auto *other_std = dynamic_cast<const StdMemoryResource *>(&other);
    if (!other_std) return false;
    return memory_ == other_std->memory_;
  }

  std::experimental::pmr::memory_resource *memory_;
};

inline MemoryResource *NewDeleteResource() noexcept {
  static StdMemoryResource memory(
      std::experimental::pmr::new_delete_resource());
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
class MonotonicBufferResource final : public MemoryResource {
 public:
  /// Construct the resource with the buffer size of at least `initial_size`.
  ///
  /// Uses the default NewDeleteResource for requesting buffer memory.
  explicit MonotonicBufferResource(size_t initial_size);

  /// Construct the resource with the buffer size of at least `initial_size` and
  /// use the `memory` as the upstream resource.
  MonotonicBufferResource(size_t initial_size, MemoryResource *memory);

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
    char *data() { return reinterpret_cast<char *>(this) + sizeof(Buffer); }
  };

  MemoryResource *memory_{NewDeleteResource()};
  Buffer *current_buffer_{nullptr};
  size_t initial_size_{0U};
  size_t next_buffer_size_{initial_size_};
  size_t allocated_{0U};

  void *DoAllocate(size_t bytes, size_t alignment) override;

  void DoDeallocate(void *, size_t, size_t) override {}

  bool DoIsEqual(const MemoryResource &other) const noexcept override {
    return this == &other;
  }
};

}  // namespace utils
