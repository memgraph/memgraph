#pragma once

#include <cstdint>

#include "glog/logging.h"

#include "storage/gid.hpp"
#include "storage/serialization.capnp.h"

namespace storage {

/**
 * A data structure that tracks a Vertex/Edge location (address) that's either
 * local or remote. The remote address is a global id alongside the id of the
 * worker on which it's currently stored, while the local address is simply the
 * memory address in the current local process. Both types of address are stored
 * in the same storage space, so an Address always takes as much memory as a
 * pointer does.
 *
 * The memory layout for storage is on x64 architecture is the following:
 *  - the lowest bit stores 0 if address is local and 1 if address is global
 *  - if the address is local all 64 bits store the local memory address
 *  - if the address is global then lowest bit stores 1. the following
 *    kWorkerIdSize bits contain the worker id and the final (upper) 64 - 1 -
 *    kWorkerIdSize bits contain the global id.
 *
 * @tparam TRecord - Type of record this address points to. Either Vertex or
 * Edge.
 */
template <typename TLocalObj>
class Address {
  static constexpr uint64_t kTypeMaskSize{1};
  static constexpr uint64_t kTypeMask{(1ULL << kTypeMaskSize) - 1};
  static constexpr uint64_t kWorkerIdSize{gid::kWorkerIdSize};
  static constexpr uint64_t kLocal{0};
  static constexpr uint64_t kRemote{1};

 public:
  using StorageT = uint64_t;

  Address() {}

  // Constructor for raw address value
  explicit Address(StorageT storage) : storage_(storage) {}

  // Constructor for local Address.
  explicit Address(TLocalObj *ptr) {
    uintptr_t ptr_no_type = reinterpret_cast<uintptr_t>(ptr);
    DCHECK((ptr_no_type & kTypeMask) == 0) << "Ptr has type_mask bit set";
    storage_ = ptr_no_type | kLocal;
  }

  // Constructor for remote Address, takes worker_id which specifies the worker
  // that is storing that vertex/edge
  Address(gid::Gid global_id, int worker_id) {
    CHECK(global_id <
          (1ULL << (sizeof(StorageT) * 8 - kWorkerIdSize - kTypeMaskSize)))
        << "Too large global id";
    CHECK(worker_id < (1ULL << kWorkerIdSize)) << "Too larger worker id";

    storage_ = kRemote;
    storage_ |= global_id << (kTypeMaskSize + kWorkerIdSize);
    storage_ |= worker_id << kTypeMaskSize;
  }

  bool is_local() const { return (storage_ & kTypeMask) == kLocal; }
  bool is_remote() const { return (storage_ & kTypeMask) == kRemote; }

  TLocalObj *local() const {
    DCHECK(is_local()) << "Attempting to get local address from global";
    return reinterpret_cast<TLocalObj *>(storage_);
  }

  gid::Gid gid() const {
    DCHECK(is_remote()) << "Attempting to get global ID from local address";
    return storage_ >> (kTypeMaskSize + kWorkerIdSize);
  }

  /// Returns worker id where the object is located
  int worker_id() const {
    DCHECK(is_remote()) << "Attempting to get worker ID from local address";
    return (storage_ >> 1) & ((1ULL << kWorkerIdSize) - 1);
  }

  /// Returns raw address value
  StorageT raw() const { return storage_; }

  bool operator==(const Address<TLocalObj> &other) const {
    return storage_ == other.storage_;
  }

  void Load(const capnp::Address::Reader &reader) {
    storage_ = reader.getStorage();
  }

 private:
  StorageT storage_{0};
};

template <typename TLocalObj>
void Save(const Address<TLocalObj> &address, capnp::Address::Builder *builder) {
  builder->setStorage(address.raw());
}

template <typename TLocalObj>
Address<TLocalObj> Load(const capnp::Address::Reader &reader) {
  return Address<TLocalObj>(reader.getStorage());
}

}  // namespace storage
