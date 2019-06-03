/// @file
/// This file contains interface and implementation of various MemoryResource
/// and Allocator schemes. It is based on the C++17 <memory_resource> features.

#pragma once

#include <cstddef>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
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
  Allocator(const Allocator<U> &other) noexcept
      : memory_(other.GetMemoryResource()) {}

  template <class U>
  Allocator &operator=(const Allocator<U> &) = delete;

  MemoryResource *GetMemoryResource() const { return memory_; }

  T *allocate(size_t count_elements) {
    return static_cast<T *>(
        memory_->Allocate(count_elements * sizeof(T), alignof(T)));
  }

  void deallocate(T *p, size_t count_elements) {
    memory_->Deallocate(p, count_elements * sizeof(T), alignof(T));
  }

  /// Return default NewDeleteResource() allocator.
  Allocator select_on_container_copy_construction() const {
    return utils::NewDeleteResource();
  }

  template <class U, class... TArgs>
  void construct(U *ptr, TArgs &&... args) {
    if constexpr (std::uses_allocator_v<U, Allocator>) {
      if constexpr (std::is_constructible_v<U, std::allocator_arg_t,
                                            MemoryResource *, TArgs...>) {
        ::new (ptr)
            U(std::allocator_arg, memory_, std::forward<TArgs>(args)...);
      } else if constexpr (std::is_constructible_v<U, TArgs...,
                                                   MemoryResource *>) {
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
  void construct(std::pair<T1, T2> *p, std::piecewise_construct_t,
                 std::tuple<Args1...> x, std::tuple<Args2...> y) {
    auto xprime = MakePairElementArguments<T1>(&x);
    auto yprime = MakePairElementArguments<T2>(&y);
    ::new (p) std::pair<T1, T2>(std::piecewise_construct, std::move(xprime),
                                std::move(yprime));
  }

  template <class T1, class T2>
  void construct(std::pair<T1, T2> *p) {
    construct(p, std::piecewise_construct, std::tuple<>(), std::tuple<>());
  }

  template <class T1, class T2, class U, class V>
  void construct(std::pair<T1, T2> *p, U &&x, V &&y) {
    construct(p, std::piecewise_construct,
              std::forward_as_tuple(std::forward<U>(x)),
              std::forward_as_tuple(std::forward<V>(y)));
  }

  template <class T1, class T2, class U, class V>
  void construct(std::pair<T1, T2> *p, const std::pair<U, V> &xy) {
    construct(p, std::piecewise_construct, std::forward_as_tuple(xy.first),
              std::forward_as_tuple(xy.second));
  }

  template <class T1, class T2, class U, class V>
  void construct(std::pair<T1, T2> *p, std::pair<U, V> &&xy) {
    construct(p, std::piecewise_construct,
              std::forward_as_tuple(std::forward<U>(xy.first)),
              std::forward_as_tuple(std::forward<V>(xy.second)));
  }

 private:
  MemoryResource *memory_;

  template <class TElem, class... TArgs>
  auto MakePairElementArguments(std::tuple<TArgs...> *args) {
    if constexpr (std::uses_allocator_v<TElem, Allocator>) {
      if constexpr (std::is_constructible_v<TElem, std::allocator_arg_t,
                                            MemoryResource *, TArgs...>) {
        return std::tuple_cat(std::make_tuple(std::allocator_arg, memory_),
                              std::move(*args));
      } else if constexpr (std::is_constructible_v<TElem, TArgs...,
                                                   MemoryResource *>) {
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
    return *memory_ == *other_std->memory_;
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
