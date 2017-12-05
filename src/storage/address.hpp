#pragma once

#include <cstdint>

#include "glog/logging.h"
#include "storage/gid.hpp"

namespace storage {

/**
 * A data structure that tracks a Vertex/Edge location (address) that's either
 * local or remote. The remote address is a global id, while the local address
 * is simply the memory address in the current local process. Both types of
 * address are stored in the same storage space, so an Address always takes as
 * much memory as a pointer does.
 *
 * The memory layout for storage is on x64 architecture is the following:
 *  - the lowest bit stores 0 if address is local and 1 if address is global
 *  - if the address is local all 64 bits store the local memory address
 *  - if the address is global then most imporant 63 bits store the global id
 *
 * @tparam TRecord - Type of record this address points to. Either Vertex or
 * Edge.
 */
template <typename TLocalObj>
class Address {
  using Storage = uint64_t;
  static constexpr uintptr_t kTypeMaskSize{1};
  static constexpr uintptr_t kTypeMask{(1ULL << kTypeMaskSize) - 1};
  static constexpr uintptr_t kLocal{0};
  static constexpr uintptr_t kRemote{1};

 public:
  // Constructor for local Address.
  Address(TLocalObj *ptr) {
    uintptr_t ptr_no_type = reinterpret_cast<uintptr_t>(ptr);
    DCHECK((ptr_no_type & kTypeMask) == 0) << "Ptr has type_mask bit set";
    storage_ = ptr_no_type | kLocal;
  }

  // Constructor for remote Address.
  Address(gid::Gid global_id) {
    CHECK(global_id < (1ULL << (sizeof(Storage) * 8 - kTypeMaskSize)))
        << "Too large global id";

    storage_ = kRemote;
    storage_ |= global_id << kTypeMaskSize;
  }

  bool is_local() const { return (storage_ & kTypeMask) == kLocal; }
  bool is_remote() const { return (storage_ & kTypeMask) == kRemote; }

  TLocalObj *local() const {
    DCHECK(is_local()) << "Attempting to get local address from global";
    return reinterpret_cast<TLocalObj *>(storage_);
  }

  gid::Gid global_id() const {
    DCHECK(is_remote()) << "Attempting to get global ID from local address";
    return storage_ >> kTypeMaskSize;
  }

  bool operator==(const Address<TLocalObj> &other) const {
    return storage_ == other.storage_;
  }

 private:
  Storage storage_{0};
};
}  // namespace storage
