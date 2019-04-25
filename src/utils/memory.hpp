/// @file
/// This file contains interface and implementation of various MemoryResource
/// and Allocator schemes. It is based on the C++17 <memory_resource> features.

#pragma once

#include <cstddef>
// Although <memory_resource> is in C++17, gcc libstdc++ still need to
// implement it fully. It should be available in the next major release
// version, i.e. gcc 9.x.
#include <experimental/memory_resource>

namespace utils {

/// Abstract class for writing custom memory management, i.e. allocators.
class MemoryResource {
 public:
  virtual ~MemoryResource() {}

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

}  // namespace utils
