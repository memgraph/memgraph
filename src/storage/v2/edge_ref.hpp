// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include "flags/general.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/logging.hpp"

#include <cstdint>

namespace memgraph::storage {

struct Edge;

// EdgeRef encodes one of: Gid only (bit 63 set) or Edge* (heavy or light). One word, trivially copyable for Delta.
// Light edges: Edge* stored in vertex vectors only; no ref count. Deleted light edges are moved to a skiplist.
#if defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__) || defined(_M_ARM64)
static_assert(sizeof(void *) == 8, "EdgeRef tagging requires 64-bit pointers");
#else
#error "EdgeRef uses bit 63 for Gid tagging; requires 48-bit (or similar) pointer address space (x86_64 / AArch64)"
#endif

constexpr uint64_t kEdgeRefGidBit = 1ULL << 63;

struct EdgeRef {
  /// For "Gid only" (e.g. remove delta in light mode) or when !properties_on_edges.
  explicit EdgeRef(Gid gid) : storage_((kEdgeRefGidBit) | (gid.AsUint() & ((1ULL << 62) - 1))) {}

  /// Heavy edge (skiplist) or light edge (vertex lists only). Stores Edge* only; no holder in union.
  explicit EdgeRef(Edge *ptr) : ptr_(ptr) {
    MG_ASSERT((reinterpret_cast<uint64_t>(ptr_) & kEdgeRefGidBit) == 0,
              "Edge* must not have high bit set (48-bit pointer assumption)");
  }

  bool IsGidOnly() const noexcept { return (storage_ & kEdgeRefGidBit) != 0; }

  bool IsLight() const noexcept { return FLAGS_storage_light_edge; }

  bool HasPointer() const noexcept { return !IsGidOnly(); }

  /// Gid for comparison/hashing. Valid for all encodings (Gid-only, heavy, light).
  Gid GetGid() const;

  /// Edge pointer. Only valid when HasPointer(). Never call when IsGidOnly().
  Edge *GetEdgePtr() const;

  friend bool operator==(const EdgeRef &a, const EdgeRef &b) noexcept;
  friend bool operator<(const EdgeRef &first, const EdgeRef &second);

  template <typename H>
  friend H AbslHashValue(H h, EdgeRef const &edge_ref) {
    return H::combine(std::move(h), edge_ref.GetGid().AsUint());
  }

  union {
    uint64_t storage_;
    Gid gid_;
    Edge *ptr_;
  };
};

static_assert(sizeof(EdgeRef) == sizeof(Gid), "EdgeRef must stay one word");
static_assert(sizeof(EdgeRef) == sizeof(Edge *), "EdgeRef must stay one word");
static_assert(std::is_trivially_copyable_v<EdgeRef>, "EdgeRef must remain trivially copyable for Delta");

}  // namespace memgraph::storage
