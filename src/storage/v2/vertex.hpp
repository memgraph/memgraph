#pragma once

#include <limits>
#include <unordered_map>
#include <vector>

#include "utils/spin_lock.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/gid.hpp"

namespace storage {

struct Vertex {
  Vertex(Gid gid, Delta *delta) : gid(gid), deleted(false), delta(delta) {
    CHECK(delta->action == Delta::Action::DELETE_OBJECT)
        << "Vertex must be created with an initial DELETE_OBJECT delta!";
  }

  Gid gid;
  std::vector<uint64_t> labels;
  std::unordered_map<uint64_t, storage::PropertyValue> properties;

  utils::SpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;

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
