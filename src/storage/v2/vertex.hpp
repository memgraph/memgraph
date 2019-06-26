#pragma once

#include <limits>
#include <unordered_map>
#include <vector>

#include "utils/spin_lock.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/gid.hpp"

namespace storage {

struct Vertex {
  // Default constructor necessary for utils::SkipList.
  Vertex()
      : gid(storage::Gid::FromUint(std::numeric_limits<uint64_t>::max())),
        delta(nullptr) {}

  Vertex(Gid gid, Delta *delta) : gid(gid), delta(delta) {
    CHECK(delta->action == Delta::Action::DELETE_OBJECT)
        << "Vertex must be created with an initial DELETE_OBJECT delta!";
  }

  Gid gid;
  std::vector<uint64_t> labels;

  // TODO: add
  // std::unordered_map<uint64_t, storage::PropertyValue> properties;

  utils::SpinLock lock;
  // uint32_t PAD;

  Delta *delta;
};

inline bool operator==(const Vertex &first, const Vertex &second) {
  return first.gid == second.gid;
}
inline bool operator<(const Vertex &first, const Vertex &second) {
  return first.gid < second.gid;
}
inline bool operator==(const Vertex &first, const Gid &second) {
  return first.gid == second;
}
inline bool operator<(const Vertex &first, const Gid &second) {
  return first.gid < second;
}

}  // namespace storage
