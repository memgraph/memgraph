#pragma once

#include <cstdint>

#include "glog/logging.h"

namespace storage {

/**
 * A data structure that tracks a Vertex/Edge location (address) that's either
 * local or remote. The remote address consists of a pair (shard_id, global_id),
 * while the local address is simply the memory address in the current local
 * process. Both types of address are stored in the same storage space, so an
 * Address always takes as much memory as a pointer does.
 *
 * The memory layout for storage is on x64 architecture is the following:
 *  - the lowest bit stores 0 if address is local and 1 if address is global
 *  - if the address is local all 64 bits store the local memory address
 *  - if the address is global then:
 *    - lower bits in [1, 1 + kShardIdSize] range contain the shard ID
 *    - upper (64 - 1 - kShardIdSize) bits, which is the [2 + kShardIdSize,
 *      63] range contain the globally unique element ID
 *
 * @tparam TRecord - Type of record this address points to. Either Vertex or
 * Edge.
 */
template <typename TLocalObj>
class Address {
  static constexpr uintptr_t kTypeMask{1};
  static constexpr uintptr_t kLocal{0};
  static constexpr uintptr_t kRemote{1};
  static constexpr size_t kShardIdPos{1};
  // To modify memory layout only change kShardIdSize.
  static constexpr size_t kShardIdSize{10};
  static constexpr size_t KGlobalIdPos{kShardIdPos + kShardIdSize};
  static constexpr size_t kGlobalIdSize{64 - 1 - kShardIdSize};

 public:
  // Constructor for local Address.
  Address(TLocalObj *ptr) {
    uintptr_t ptr_no_type = reinterpret_cast<uintptr_t>(ptr);
    DCHECK((ptr_no_type & kTypeMask) == 0) << "Ptr has type_mask bit set";
    storage_ = ptr_no_type | kLocal;
  }

  // Constructor for remote Address.
  Address(uint64_t shard_id, uint64_t global_id) {
    // TODO make a SSOT about max shard ID. Ensure that a shard with a larger ID
    // can't be created, and that this ID fits into kShardIdSize bits.
    CHECK(shard_id < (1ULL << (kShardIdSize - 1))) << "Shard ID too big.";
    CHECK(global_id < (1ULL << (kGlobalIdSize - 1)))
        << "Global element ID too big.";

    storage_ = kRemote;
    storage_ |= shard_id << kShardIdPos;
    storage_ |= global_id << KGlobalIdPos;
  }

  bool is_local() const { return (storage_ & kTypeMask) == kLocal; }
  bool is_remote() const { return (storage_ & kTypeMask) == kRemote; }

  TLocalObj *local() const {
    DCHECK(is_local()) << "Attempting to get local address from global";
    return reinterpret_cast<TLocalObj *>(storage_);
  }

  uint64_t shard_id() const {
    DCHECK(is_remote()) << "Attempting to get shard ID from local address";
    return (storage_ >> kShardIdPos) & ((1ULL << kShardIdSize) - 1);
  }

  uint64_t global_id() const {
    DCHECK(is_remote()) << "Attempting to get global ID from local address";
    return (storage_ >> KGlobalIdPos) & ((1ULL << kGlobalIdSize) - 1);
  }

  bool operator==(const Address<TLocalObj> &other) const {
    return storage_ == other.storage_;
  }

 private:
  uintptr_t storage_{0};
};
}
