#pragma once

#include <cstdint>
#include <vector>

/** A globally defined identifier. Defines a worker
 * and the sequence number on that worker */
class GlobalId {
 public:
  GlobalId(int64_t worker_id, int64_t sequence_number)
      : worker_id_(worker_id), sequence_number_(sequence_number) {}
  // TODO perhaps make members const and replace instead of changing
  // when migrating nodes
  int64_t worker_id_;
  int64_t sequence_number_;

  bool operator==(const GlobalId &other) const {
    return worker_id_ == other.worker_id_ &&
           sequence_number_ == other.sequence_number_;
  }

  bool operator!=(const GlobalId &other) const { return !(*this == other); }
};

/** Defines a location in the system where something can be found.
 * Something can be found on some worker, for some Id */
class GlobalAddress {
 public:
  GlobalAddress(int64_t worker_id, GlobalId id)
      : worker_id_(worker_id), id_(id) {}
  // TODO perhaps make members const and replace instead of changing
  // when migrating nodes
  int64_t worker_id_;
  GlobalId id_;

  bool operator==(const GlobalAddress &other) const {
    return worker_id_ == other.worker_id_ && id_ == other.id_;
  }

  bool operator!=(const GlobalAddress &other) const {
    return !(*this == other);
  }
};

namespace std {
template <>
struct hash<GlobalId> {
  size_t operator()(const GlobalId &id) const {
    return id.sequence_number_ << 4 ^ id.worker_id_;
  }
};

template <>
struct hash<GlobalAddress> {
  size_t operator()(const GlobalAddress &ga) const {
    return gid_hash(ga.id_) << 4 ^ ga.worker_id_;
  }

 private:
  std::hash<GlobalId> gid_hash{};
};
}
