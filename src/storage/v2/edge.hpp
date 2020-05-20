#pragma once

#include <limits>

#include "utils/spin_lock.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"

namespace storage {

struct Vertex;

struct Edge {
  Edge(Gid gid, Delta *delta) : gid(gid), deleted(false), delta(delta) {
    CHECK(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT)
        << "Edge must be created with an initial DELETE_OBJECT delta!";
  }

  Gid gid;

  PropertyStore properties;

  mutable utils::SpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;

  Delta *delta;
};

static_assert(alignof(Edge) >= 8, "The Edge should be aligned to at least 8!");

inline bool operator==(const Edge &first, const Edge &second) {
  return first.gid == second.gid;
}
inline bool operator<(const Edge &first, const Edge &second) {
  return first.gid < second.gid;
}
inline bool operator==(const Edge &first, const Gid &second) {
  return first.gid == second;
}
inline bool operator<(const Edge &first, const Gid &second) {
  return first.gid < second;
}

}  // namespace storage
