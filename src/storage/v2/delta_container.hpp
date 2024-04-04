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

#pragma once

#include "storage/v2/delta.hpp"

#include "utils/static_vector.hpp"

#include <forward_list>
#include <memory>
#include <ranges>

namespace memgraph::storage {

namespace {

// custom allocator, ensures all allocations are page aligned
template <typename T>
struct PageAlignedAllocator {
  using value_type = T;

  PageAlignedAllocator() = default;

  template <class U>
  explicit PageAlignedAllocator(const PageAlignedAllocator<U> &) noexcept {}

  auto allocate(std::size_t n) -> T * {
    auto size = std::max(n * sizeof(T), PAGE_SIZE);
    // Round up to the nearest multiple of PAGE_SIZE
    size = ((size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
    void *ptr = std::aligned_alloc(PAGE_SIZE, size);
    if (!ptr) throw std::bad_alloc();
    return static_cast<T *>(ptr);
  }

  void deallocate(T *p, std::size_t) const noexcept { std::free(p); }

  constexpr friend bool operator==(PageAlignedAllocator const &, PageAlignedAllocator const &) noexcept { return true; }
};

template <typename T>
using PageAlignedList = std::forward_list<T, PageAlignedAllocator<T>>;

template <std::size_t N>
using delta_slab = memgraph::utils::static_vector<Delta, N>;

// This is for how many deltas can exist in delta_slab no larger than PAGE_SIZE
// assumption `sizeof(void *)` if for the node pointer inside forward_list's node
constexpr auto kMaxDeltas = (PAGE_SIZE - sizeof(void *) - delta_slab<0>::header_size()) / sizeof(Delta);

// Flattern iterators used here becasue we can't use
// `std::views::join` becasue stack-use-after-scope
template <typename OuterContainer, typename InnerContainer>
struct FlattenIterator {
 private:
  using OuterIterator = typename OuterContainer::iterator;
  using InnerIterator = typename InnerContainer::iterator;
  using InnerValue = typename InnerContainer::value_type;

 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = InnerValue;
  using reference = value_type &;
  using pointer = value_type *;
  using difference_type = std::ptrdiff_t;

  FlattenIterator(OuterIterator outer_begin, OuterIterator outer_end) : outer_iter{outer_begin}, outer_end{outer_end} {
    if (outer_iter != outer_end) {
      inner_iter = outer_iter->begin();
      inner_end = outer_iter->end();
      // Move to the first valid inner iterator
      while (inner_iter == inner_end && outer_iter != outer_end) {
        next_inner();
      }
    } else {
      // sentinal
      inner_iter = InnerIterator{};
      inner_end = InnerIterator{};
    }
  }

  auto operator++() -> FlattenIterator & {
    ++inner_iter;
    // Move to the next valid inner iterator
    while (inner_iter == inner_end && outer_iter != outer_end) {
      next_inner();
    }
    return *this;
  }

  auto operator*() -> reference { return *inner_iter; }

  friend bool operator==(FlattenIterator const &lhs, FlattenIterator const &rhs) {
    return lhs.inner_iter == rhs.inner_iter && lhs.outer_iter == rhs.outer_iter;
  }

 private:
  void next_inner() {
    ++outer_iter;
    if (outer_iter != outer_end) {
      // setup for next inner
      inner_iter = outer_iter->begin();
      inner_end = outer_iter->end();
    } else {
      // sentinal
      inner_iter = InnerIterator{};
      inner_end = InnerIterator{};
    }
  }

  OuterIterator outer_iter{};
  InnerIterator inner_iter{};
  OuterIterator outer_end{};
  InnerIterator inner_end{};
};

template <typename OuterContainer, typename InnerContainer>
struct FlattenConstIterator {
 private:
  using OuterIterator = typename OuterContainer::const_iterator;
  using InnerIterator = typename InnerContainer::const_iterator;
  using InnerValue = typename InnerContainer::value_type const;

 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = InnerValue;
  using reference = value_type &;
  using pointer = value_type *;
  using difference_type = std::ptrdiff_t;

  FlattenConstIterator(OuterIterator outer_begin, OuterIterator outer_end)
      : outer_iter{outer_begin}, outer_end{outer_end} {
    if (outer_iter != outer_end) {
      inner_iter = outer_iter->begin();
      inner_end = outer_iter->end();
      // Move to the first valid inner iterator
      while (inner_iter == inner_end && outer_iter != outer_end) {
        next_inner();
      }
    } else {
      // sentinal
      inner_iter = InnerIterator{};
      inner_end = InnerIterator{};
    }
  }

  auto operator++() -> FlattenConstIterator & {
    ++inner_iter;
    // Move to the next valid inner iterator
    while (inner_iter == inner_end && outer_iter != outer_end) {
      next_inner();
    }
    return *this;
  }

  auto operator*() -> reference { return *inner_iter; }

  friend bool operator==(FlattenConstIterator const &lhs, FlattenConstIterator const &rhs) {
    return lhs.inner_iter == rhs.inner_iter && lhs.outer_iter == rhs.outer_iter;
  }

 private:
  void next_inner() {
    ++outer_iter;
    if (outer_iter != outer_end) {
      // setup for next inner
      inner_iter = outer_iter->begin();
      inner_end = outer_iter->end();
    } else {
      // sentinal
      inner_iter = InnerIterator{};
      inner_end = InnerIterator{};
    }
  }

  OuterIterator outer_iter{};
  InnerIterator inner_iter{};
  OuterIterator outer_end{};
  InnerIterator inner_end{};
};

// Helpers to make the iterators
template <typename OuterContainer, typename InnerContainer>
class Flatten {
 private:
  OuterContainer &outer_container;

 public:
  explicit Flatten(OuterContainer &outer) : outer_container(outer) {}

  auto begin() const {
    return FlattenIterator<OuterContainer, InnerContainer>(outer_container.begin(), outer_container.end());
  }

  auto end() const {
    return FlattenIterator<OuterContainer, InnerContainer>(outer_container.end(), outer_container.end());
  }
};

template <typename OuterContainer>
Flatten(OuterContainer &outer) -> Flatten<OuterContainer, typename OuterContainer::value_type>;

template <typename OuterContainer, typename InnerContainer>
class ConstFlatten {
 private:
  OuterContainer const &outer_container;

 public:
  explicit ConstFlatten(OuterContainer const &outer) : outer_container(outer) {}

  auto begin() const {
    return FlattenConstIterator<OuterContainer, InnerContainer>(outer_container.begin(), outer_container.end());
  }

  auto end() const {
    return FlattenConstIterator<OuterContainer, InnerContainer>(outer_container.end(), outer_container.end());
  }
};

template <typename OuterContainer>
ConstFlatten(OuterContainer &outer) -> ConstFlatten<OuterContainer, typename OuterContainer::value_type>;

}  // namespace

struct delta_container {
  using value_type = Delta;

  delta_container() = default;

  // move ctr: needed because of size_
  delta_container(delta_container &&other) noexcept
      : memory_resource_{std::move(other.memory_resource_)},
        deltas_{std::move(other.deltas_)},
        size_{std::exchange(other.size_, 0)} {}

  // move assign: needed because of size_
  delta_container &operator=(delta_container &&other) noexcept {
    std::swap(memory_resource_, other.memory_resource_);
    std::swap(deltas_, other.deltas_);
    std::swap(size_, other.size_);
    other.clear();
    return *this;
  }

  ~delta_container() = default;

  auto begin() { return Flatten(deltas_).begin(); }
  auto end() { return Flatten(deltas_).end(); }

  auto begin() const { return ConstFlatten(deltas_).begin(); }
  auto end() const { return ConstFlatten(deltas_).end(); }

  template <typename... Args>
  auto emplace(Args &&...args) -> Delta & {
    auto do_emplace = [&]() -> Delta & {
      if (!memory_resource_) [[unlikely]] {
        // TODO: only do for actions that maybe require allocation
        memory_resource_ = std::make_unique<DeltaMemoryResource>();
      }
      auto &delta = deltas_.front().emplace(std::forward<Args>(args)..., memory_resource_.get());
      ++size_;
      return delta;
    };

    if (deltas_.empty() || deltas_.front().is_full()) [[unlikely]] {
      deltas_.emplace_front();
      try {
        return do_emplace();
      } catch (...) {
        deltas_.pop_front();
        throw;
      }
    }

    return do_emplace();
  }

  void clear() {
    deltas_.clear();
    memory_resource_.reset();
    size_ = 0;
  }

  void unlink() {
    // Do I move GC code here?
  }

  bool empty() const { return deltas_.empty(); }

  auto size() const -> std::size_t { return size_; }

 private:
  // NOTE: destruction order important
  std::unique_ptr<DeltaMemoryResource> memory_resource_{};
  PageAlignedList<delta_slab<kMaxDeltas>> deltas_{};
  std::size_t size_{};
};

}  // namespace memgraph::storage
