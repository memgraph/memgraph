#pragma once

#include <limits>
#include <map>
#include <tuple>
#include <vector>

#include "utils/spin_lock.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"

namespace storage {

struct Vertex {
  Vertex(Gid gid, Delta *delta) : gid(gid), deleted(false), delta(delta) {
    CHECK(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT)
        << "Vertex must be created with an initial DELETE_OBJECT delta!";
  }

  Gid gid;

  std::vector<LabelId> labels;
  std::map<PropertyId, storage::PropertyValue> properties;

  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  utils::SpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;

  Delta *delta;
};

static_assert(alignof(Vertex) >= 8,
              "The Vertex should be aligned to at least 8!");

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
