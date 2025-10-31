// Copyright 2025 Memgraph Ltd.
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

#include <atomic>
#include <cstdint>

#include "storage/v2/delta_action.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/allocator/page_slab_memory_resource.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

// Forward declarations because we only store pointers here.
struct Vertex;
struct Edge;
struct Delta;

// This class stores one of three pointers (`Delta`, `Vertex` and `Edge`)
// without using additional memory for storing the type. The type is stored in
// the pointer itself in the lower bits. All of those structures contain large
// items in themselves (e.g. `uint64_t`) that require the pointer to be aligned
// to their size (for `uint64_t` it is 8). That means that the pointer will
// always be a multiple of 8 which implies that the lower 3 bits of the pointer
// will always be 0. We can use those 3 bits to store information about the type
// of the pointer stored (2 bits).
class PreviousPtr {
  static constexpr uintptr_t kDelta = 0b01UL;
  static constexpr uintptr_t kVertex = 0b10UL;
  static constexpr uintptr_t kEdge = 0b11UL;

  static constexpr uintptr_t kMask = 0b11UL;

 public:
  enum class Type {
    NULL_PTR,
    DELTA,
    VERTEX,
    EDGE,
  };

  struct Pointer {
    Pointer() = default;
    explicit Pointer(Delta *delta) : type(Type::DELTA), delta(delta) {}
    explicit Pointer(Vertex *vertex) : type(Type::VERTEX), vertex(vertex) {}
    explicit Pointer(Edge *edge) : type(Type::EDGE), edge(edge) {}

    Type type{Type::NULL_PTR};
    union {
      Delta *delta = nullptr;
      Vertex *vertex;
      Edge *edge;
    };
  };

  PreviousPtr() : storage_(0) {}

  PreviousPtr(const PreviousPtr &other) noexcept : storage_(other.storage_.load(std::memory_order_acquire)) {}

  Pointer Get() const {
    uintptr_t value = storage_.load(std::memory_order_acquire);
    if (value == 0) {
      return {};
    }
    uintptr_t type = value & kMask;
    if (type == kDelta) {
      return Pointer{reinterpret_cast<Delta *>(value & ~kMask)};
    } else if (type == kVertex) {
      return Pointer{reinterpret_cast<Vertex *>(value & ~kMask)};
    } else if (type == kEdge) {
      return Pointer{reinterpret_cast<Edge *>(value & ~kMask)};
    } else {
      LOG_FATAL("Invalid pointer type!");
    }
  }

  void Set(Delta *delta) {
    uintptr_t value = reinterpret_cast<uintptr_t>(delta);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kDelta, std::memory_order_release);
  }

  void Set(Vertex *vertex) {
    uintptr_t value = reinterpret_cast<uintptr_t>(vertex);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kVertex, std::memory_order_release);
  }

  void Set(Edge *edge) {
    uintptr_t value = reinterpret_cast<uintptr_t>(edge);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kEdge, std::memory_order_release);
  }

 private:
  std::atomic<uintptr_t> storage_;
};

inline bool operator==(const PreviousPtr::Pointer &a, const PreviousPtr::Pointer &b) {
  if (a.type != b.type) return false;
  switch (a.type) {
    case PreviousPtr::Type::VERTEX:
      return a.vertex == b.vertex;
    case PreviousPtr::Type::EDGE:
      return a.edge == b.edge;
    case PreviousPtr::Type::DELTA:
      return a.delta == b.delta;
    case PreviousPtr::Type::NULL_PTR:
      return b.type == PreviousPtr::Type::NULL_PTR;
  }
}

struct opt_str {
  opt_str(std::optional<std::string_view> other, utils::PageSlabMemoryResource *res)
      : str_{other ? new_cstr(*other, res) : nullptr} {}

  ~opt_str() = default;

  auto as_opt_str() const -> std::optional<std::string_view> {
    if (!str_) return std::nullopt;
    return std::optional<std::string_view>{std::in_place, str_};
  }

 private:
  static auto new_cstr(std::string_view str, utils::PageSlabMemoryResource *res) -> char const * {
    auto const n = str.size() + 1;
    auto alloc = std::pmr::polymorphic_allocator<char>{res};
    auto *mem = (std::string_view::pointer)alloc.allocate_bytes(n, alignof(char));
    std::copy(str.cbegin(), str.cend(), mem);
    mem[n - 1] = '\0';
    return mem;
  }

  char const *str_ = nullptr;
};

static_assert(!std::is_constructible_v<opt_str, std::optional<std::string_view>, std::pmr::memory_resource *>,
              "Use of PageSlabMemoryResource is deliberate here");
static_assert(std::is_constructible_v<opt_str, std::optional<std::string_view>, utils::PageSlabMemoryResource *>,
              "Use of PageSlabMemoryResource is deliberate here");
static_assert(std::is_trivially_destructible_v<opt_str>,
              "uses PageSlabMemoryResource, lifetime linked to that, dtr should be trivial");

struct Delta {
  using Action = DeltaAction;

  // Used for both Vertex and Edge
  struct DeleteDeserializedObjectTag {};
  struct DeleteObjectTag {};
  struct RecreateObjectTag {};
  struct SetPropertyTag {};

  // Used only for Vertex
  struct AddLabelTag {};
  struct RemoveLabelTag {};
  struct AddInEdgeTag {};
  struct AddOutEdgeTag {};
  struct RemoveInEdgeTag {};
  struct RemoveOutEdgeTag {};

  // DELETE_DESERIALIZED_OBJECT is used to load data from disk committed by past txs.
  // Because of this object was created in past txs, we create timestamp by ourselves inside instead of having it from
  // current tx. This timestamp we got from RocksDB timestamp stored in key.
  Delta(DeleteDeserializedObjectTag /*tag*/, uint64_t ts, std::optional<std::string_view> old_disk_key,
        utils::PageSlabMemoryResource *res)
      : timestamp(std::pmr::polymorphic_allocator<Delta>{res}.new_object<std::atomic<uint64_t>>(ts)),
        command_id(0),
        old_disk_key{.value = opt_str{old_disk_key, res}} {}

  Delta(DeleteObjectTag /*tag*/, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), action(Action::DELETE_OBJECT) {}

  Delta(RecreateObjectTag /*tag*/, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), action(Action::RECREATE_OBJECT) {}

  Delta(AddLabelTag /*tag*/, LabelId label, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), label{.action = Action::ADD_LABEL, .value = label} {}

  Delta(RemoveLabelTag /*tag*/, LabelId label, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), label{.action = Action::REMOVE_LABEL, .value = label} {}

  Delta(SetPropertyTag /*tag*/, PropertyId key, PropertyValue const &value, std::atomic<uint64_t> *timestamp,
        uint64_t command_id, utils::PageSlabMemoryResource *res)
      : timestamp(timestamp),
        command_id(command_id),
        property{.action = Action::SET_PROPERTY,
                 .key = key,
                 .value = std::pmr::polymorphic_allocator<Delta>{res}.new_object<pmr::PropertyValue>(value)} {}

  Delta(SetPropertyTag /*tag*/, Vertex *out_vertex, PropertyId key, PropertyValue value,
        std::atomic<uint64_t> *timestamp, uint64_t command_id, utils::PageSlabMemoryResource *res)
      : timestamp(timestamp),
        command_id(command_id),
        property{.action = Action::SET_PROPERTY,
                 .key = key,
                 .value = std::pmr::polymorphic_allocator<Delta>{res}.new_object<pmr::PropertyValue>(std::move(value)),
                 .out_vertex = out_vertex} {}

  Delta(AddInEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : timestamp(timestamp),
        command_id(command_id),
        vertex_edge{.action = Action::ADD_IN_EDGE, .edge_type = edge_type, vertex, edge} {}

  Delta(AddOutEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : timestamp(timestamp),
        command_id(command_id),
        vertex_edge{.action = Action::ADD_OUT_EDGE, .edge_type = edge_type, vertex, edge} {}

  Delta(RemoveInEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : timestamp(timestamp),
        command_id(command_id),
        vertex_edge{.action = Action::REMOVE_IN_EDGE, .edge_type = edge_type, vertex, edge} {}

  Delta(RemoveOutEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : timestamp(timestamp),
        command_id(command_id),
        vertex_edge{.action = Action::REMOVE_OUT_EDGE, .edge_type = edge_type, vertex, edge} {}

  Delta(const Delta &) = delete;
  Delta(Delta &&) = delete;
  Delta &operator=(const Delta &) = delete;
  Delta &operator=(Delta &&) = delete;

  ~Delta() = default;

  // TODO: optimize with in-place copy
  std::atomic<uint64_t> *timestamp;
  uint64_t command_id;
  PreviousPtr prev;
  std::atomic<Delta *> next{nullptr};

  union {
    Action action;
    struct {
      Action action = Action::DELETE_DESERIALIZED_OBJECT;
      opt_str value;
    } old_disk_key;
    struct {
      Action action;
      LabelId value;
    } label;
    struct {
      Action action;
      PropertyId key;
      storage::pmr::PropertyValue *value = nullptr;
      Vertex *out_vertex{nullptr};  // Used by edge's delta to easily rebuild the edge
    } property;
    struct {
      Action action;
      EdgeTypeId edge_type;
      Vertex *vertex;
      EdgeRef edge;
    } vertex_edge;
  };
};

// This is important, we want fast discard of unlinked deltas,
static_assert(std::is_trivially_destructible_v<Delta>,
              "any allocations use PageSlabMemoryResource, lifetime linked to that, dtr should be trivial");

static_assert(alignof(Delta) >= 8, "The Delta should be aligned to at least 8!");

}  // namespace memgraph::storage
