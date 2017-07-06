#pragma once

#include <unistd.h>
#include <atomic>
#include <iostream>

namespace memory {

constexpr const size_t HP_SIZE = 128;

class HP {
 public:
  // this object can't be copied or moved
  HP(HP&) = delete;
  HP(HP&&) = delete;

  // grabs a singleton instance
  static HP& get() {
    static HP hp;
    return hp;
  }

  class reference {
    friend class HP;

   public:
    reference(reference&) = delete;

    // this type shouldn't be copyable to avoid calling its destructor
    // multiple times, but should be movable
    reference(reference&& other) {
      this->idx = other.idx;

      // set the index to a negative number to indicate that this
      // index has been moved and that you should not free its
      // hazard pointer
      other.idx = -1;
    }
    // hazard pointer is cleared once reference goes out of scope
    ~reference() {
      // TODO: remove
      // std::cout << "reference destructor called: ";
      // std::cout << this->idx;
      // std::cout << std::endl;

      // check if this reference was moved during its lifetime
      if (idx < 0) return;

      auto& hp = HP::get();
      hp.clear(*this);
    }

    // TODO: ???
    reference& operator=(reference&&) { return *this; }

   private:
    reference(int64_t idx) : idx(idx) {}
    int64_t idx;
  };

  friend class reference;

  template <class T>
  reference insert(T* ptr) {
    auto p = reinterpret_cast<uintptr_t>(ptr);

    while (true) {
      // try to find a free spot in the hazard pointer list
      for (size_t i = 0; i < HP_SIZE; ++i) {
        auto hazard = ptr_list[i].load();

        // if this spot isn't free, continue searching
        if (hazard != 0) continue;

        // try to take this spot, if we fail, then another thread has
        // just taken it. continue searching for a new one
        if (!ptr_list[i].compare_exchange_strong(hazard, p)) continue;

        // found a free spot! return a reference to this spot so it
        // can be cleared later
        return reference(i);
      }

      // we didn't find any free spots, sleep for a while and try again
      // from the beginning, some other thread might have freed a spot
      // while we were traversing the lsit.
      usleep(250);
    }
  }

  bool find(uintptr_t hptr) {
    for (size_t i = 0; i < HP_SIZE; ++i) {
      auto& hptr_i = ptr_list[i];

      if (hptr_i != hptr) continue;

      if (hptr_i.load() == 1) return true;

      if (hptr_i.load() == 0) return false;
    }

    return false;
  }

  friend std::ostream& operator<<(std::ostream& os, const HP& hp) {
    os << "Hazard pointers: ";
    for (size_t i = 0; i < HP_SIZE; ++i) {
      auto& hptr_i = hp.ptr_list[i];
      os << hptr_i.load() << " ";
    }
    return os << std::endl;
  }

 private:
  HP() {
    for (size_t i = 0; i < HP_SIZE; ++i) ptr_list[i].store(0);
  }

  void clear(reference& ref) { ptr_list[ref.idx].store(0); }

  std::atomic<uintptr_t> ptr_list[HP_SIZE];
};
}
