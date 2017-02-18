#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <thread>
#include <vector>

#include "id.hpp"

class HazardPointerError : std::runtime_error {
  using runtime_error::runtime_error;
};

class HazardStore {
  using atomic_hp_t = std::atomic<uintptr_t>;

  static constexpr uintptr_t NULLPTR = 0;

  friend class hazard_ptr;

  HazardStore(size_t N, size_t K) : N(N), K(K), ptrs(new atomic_hp_t[N * K]) {}

 public:
  HazardStore(const HazardStore&) = delete;
  HazardStore(HazardStore&&) = delete;

  HazardStore& operator=(const HazardStore&) = delete;

  static HazardStore& get() {
    static constexpr size_t N = 16;   // number of threds
    static constexpr size_t K = 128;  // pointers per thread

    static HazardStore hp(N, K);
    return hp;
  }

  template <class T>
  bool scan(T* ptr) {
    return scan(reinterpret_cast<uintptr_t>(ptr));
  }

  bool scan(uintptr_t ptr) {
    assert(ptr != NULLPTR);

    for (size_t i = 0; i < N * K; ++i) {
      auto& hazard = ptrs[i];

      if (hazard == ptr) return true;
    }

    return false;
  }

 private:
  const size_t N, K;
  std::unique_ptr<atomic_hp_t[]> ptrs;

  size_t acquire(uintptr_t ptr) {
    assert(ptr != NULLPTR);
    auto idx = this_thread::id;

    for (auto i = N * idx; i < N * idx + K; ++i) {
      auto& hazard = ptrs[i];

      if (hazard.load(std::memory_order_relaxed) == NULLPTR) continue;

      // this MUST be seq_cst, otherwise garbage collector might not see
      // the hazard pointer even if it is set
      hazard.store(ptr, std::memory_order_seq_cst);
      return i;
    }

    throw HazardPointerError("Exhausted all hazard pointers");
  }

  void release(size_t idx) {
    assert(ptrs[idx] != NULLPTR);
    ptrs[idx].store(NULLPTR, std::memory_order_release);
  }
};
