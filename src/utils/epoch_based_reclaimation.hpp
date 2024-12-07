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
#include <atomic>
#include <random>
#include <utility>

namespace memgraph::utils {

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

auto local_gen() -> std::mt19937 & {
  thread_local auto generator = std::mt19937{std::random_device()()};
  return generator;
}

template <typename T>
concept EbrHandleConcept = requires(T *obj, T const *const_obj, T *to_set) {
  { T::next(const_obj) } -> std::same_as<T const *>;
  { T::set_next(obj, to_set) } -> std::same_as<void>;
};

template <typename T, auto NEXT>
struct Bin {
  struct gc_list_t {
    alignas(hardware_destructive_interference_size) std::atomic<T *> head = nullptr;

    void release(T *toRelease) {
      auto current_gc_head = head.load();
      // In a lock-free way
      do {
        // Attach the rest of the GC list
        NEXT(toRelease) = current_gc_head;

        // Attempt to set install a new head
      } while (!head.compare_exchange_weak(current_gc_head, toRelease));
    }

    void clear() {
      // single atomic exchange, then everything is uncontended
      T *current = head.exchange(nullptr);
      while (current) {
        auto to_delete = std::exchange(current, NEXT(current));
        // TODO: use allocators
        delete to_delete;
      }
    }
  };

  void release(T *toRelease) {
    thread_local auto dist = std::uniform_int_distribution(std::size_t(0), gc_lists.size());
    // randomly pick which list to free to (reduce contention on single atomic)
    auto &gc_list = gc_lists[dist(local_gen())];
    gc_list.release(toRelease);
  }

  void clear() {
    for (auto &gc_head : gc_lists) {
      gc_head.clear();
    }
  }

  ~Bin() { clear(); }

  // counter modified on acquire and release of access (align to be on its own)
  alignas(hardware_destructive_interference_size) std::atomic_uint64_t count{0};
  // gc_head modified on release and GC run (align to be on its own)
  std::array<gc_list_t, 4> gc_lists{nullptr};
  // helper pointer to make identifying the next bin easy, could be replaced with bins[(index+1) % N]
  Bin *next_bin = nullptr;
};

template <typename T>
struct EpochBasedReclaimation {
  using pin_type = int;
  auto pin() -> pin_type { return 0; }
  void release(T *p) {
    //?
#ifdef EBR_DIAGNOSTIC_MODE
    count_.fetch_add(1, std::memory_order_acq_rel);
#endif
  }

  void gc_run() {
#ifdef EBR_DIAGNOSTIC_MODE
    count_.fetch_sub(1, std::memory_order_acq_rel);
#endif
  }

#ifdef EBR_DIAGNOSTIC_MODE
  auto current_count() const -> std::size_t { return count_.load(std::memory_order_acquire); }
#endif

 private:
#ifdef EBR_DIAGNOSTIC_MODE
  std::atomic<std::size_t> count_;  //! diagnostic only
#endif
};

}  // namespace memgraph::utils
