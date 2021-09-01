#pragma once

#include <type_traits>

#include "storage/v2/id_types.hpp"

namespace storage {

// Forward declaration because we only store a pointer here.
struct Edge;

struct EdgeRef {
  explicit EdgeRef(Gid gid) : gid(gid) {}
  explicit EdgeRef(Edge *ptr) : ptr(ptr) {}

  union {
    Gid gid;
    Edge *ptr;
  };
};

static_assert(sizeof(Gid) == sizeof(Edge *), "The Gid should be the same size as an Edge *!");
static_assert(std::is_standard_layout_v<Gid>, "The Gid must have a standard layout!");
static_assert(std::is_standard_layout_v<Edge *>, "The Edge * must have a standard layout!");
static_assert(std::is_standard_layout_v<EdgeRef>, "The EdgeRef must have a standard layout!");

inline bool operator==(const EdgeRef &a, const EdgeRef &b) noexcept { return a.gid == b.gid; }

inline bool operator!=(const EdgeRef &a, const EdgeRef &b) noexcept { return a.gid != b.gid; }

}  // namespace storage
