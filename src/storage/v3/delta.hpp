// Copyright 2022 Memgraph Ltd.
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

#include "storage/v3/edge_ref.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/vertex_id.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage::v3 {

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
 private:
  static constexpr uintptr_t kDelta = 0b01UL;
  static constexpr uintptr_t kVertex = 0b10UL;
  static constexpr uintptr_t kEdge = 0b11UL;

  static constexpr uintptr_t kMask = 0b11UL;

 public:
  enum class Type {
    NULLPTR,
    DELTA,
    VERTEX,
    EDGE,
  };

  struct Pointer {
    Pointer() = default;
    explicit Pointer(Delta *delta) : type(Type::DELTA), delta(delta) {}
    explicit Pointer(Vertex *vertex) : type(Type::VERTEX), vertex(vertex) {}
    explicit Pointer(Edge *edge) : type(Type::EDGE), edge(edge) {}

    Type type{Type::NULLPTR};
    Delta *delta{nullptr};
    Vertex *vertex{nullptr};
    Edge *edge{nullptr};
  };

  PreviousPtr() : storage_(0) {}

  PreviousPtr(const PreviousPtr &other) noexcept : storage_(other.storage_.load(std::memory_order_acquire)) {}
  PreviousPtr(PreviousPtr &&) = delete;
  PreviousPtr &operator=(const PreviousPtr &) = delete;
  PreviousPtr &operator=(PreviousPtr &&) = delete;
  ~PreviousPtr() = default;

  Pointer Get() const {
    uintptr_t value = storage_.load(std::memory_order_acquire);
    if (value == 0) {
      return {};
    }
    uintptr_t type = value & kMask;
    if (type == kDelta) {
      // NOLINTNEXTLINE(performance-no-int-to-ptr)
      return Pointer{reinterpret_cast<Delta *>(value & ~kMask)};
    }
    if (type == kVertex) {
      // NOLINTNEXTLINE(performance-no-int-to-ptr)
      return Pointer{reinterpret_cast<Vertex *>(value & ~kMask)};
    }
    if (type == kEdge) {
      // NOLINTNEXTLINE(performance-no-int-to-ptr)
      return Pointer{reinterpret_cast<Edge *>(value & ~kMask)};
    }
    LOG_FATAL("Invalid pointer type!");
  }

  void Set(Delta *delta) {
    auto value = reinterpret_cast<uintptr_t>(delta);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kDelta, std::memory_order_release);
  }

  void Set(Vertex *vertex) {
    auto value = reinterpret_cast<uintptr_t>(vertex);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kVertex, std::memory_order_release);
  }

  void Set(Edge *edge) {
    auto value = reinterpret_cast<uintptr_t>(edge);
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
    case PreviousPtr::Type::NULLPTR:
      return b.type == PreviousPtr::Type::NULLPTR;
  }
}

inline bool operator!=(const PreviousPtr::Pointer &a, const PreviousPtr::Pointer &b) { return !(a == b); }

struct Delta {
  enum class Action {
    // Used for both Vertex and Edge
    DELETE_OBJECT,
    RECREATE_OBJECT,
    SET_PROPERTY,

    // Used only for Vertex
    ADD_LABEL,
    REMOVE_LABEL,
    ADD_IN_EDGE,
    ADD_OUT_EDGE,
    REMOVE_IN_EDGE,
    REMOVE_OUT_EDGE,
  };

  // Used for both Vertex and Edge
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

  Delta(DeleteObjectTag /*unused*/, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::DELETE_OBJECT), timestamp(timestamp), command_id(command_id) {}

  Delta(RecreateObjectTag /*unused*/, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::RECREATE_OBJECT), timestamp(timestamp), command_id(command_id) {}

  Delta(AddLabelTag /*unused*/, LabelId label, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::ADD_LABEL), timestamp(timestamp), command_id(command_id), label(label) {}

  Delta(RemoveLabelTag /*unused*/, LabelId label, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::REMOVE_LABEL), timestamp(timestamp), command_id(command_id), label(label) {}

  Delta(SetPropertyTag /*unused*/, PropertyId key, const PropertyValue &value, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : action(Action::SET_PROPERTY), timestamp(timestamp), command_id(command_id), property({key, value}) {}

  Delta(AddInEdgeTag /*unused*/, EdgeTypeId edge_type, VertexId vertex_id, EdgeRef edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::ADD_IN_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, std::move(vertex_id), edge}) {}

  Delta(AddOutEdgeTag /*unused*/, EdgeTypeId edge_type, VertexId vertex_id, EdgeRef edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::ADD_OUT_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, std::move(vertex_id), edge}) {}

  Delta(RemoveInEdgeTag /*unused*/, EdgeTypeId edge_type, VertexId vertex_id, EdgeRef edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::REMOVE_IN_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, std::move(vertex_id), edge}) {}

  Delta(RemoveOutEdgeTag /*unused*/, EdgeTypeId edge_type, VertexId vertex_id, EdgeRef edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::REMOVE_OUT_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, std::move(vertex_id), edge}) {}

  Delta(const Delta &) = delete;
  Delta(Delta &&) = delete;
  Delta &operator=(const Delta &) = delete;
  Delta &operator=(Delta &&) = delete;

  ~Delta() {
    switch (action) {
      case Action::DELETE_OBJECT:
      case Action::RECREATE_OBJECT:
      case Action::ADD_LABEL:
      case Action::REMOVE_LABEL:
      case Action::ADD_IN_EDGE:
      case Action::ADD_OUT_EDGE:
      case Action::REMOVE_IN_EDGE:
      case Action::REMOVE_OUT_EDGE:
        break;
      case Action::SET_PROPERTY:
        property.value.~PropertyValue();
        break;
    }
  }

  Action action;

  // TODO: optimize with in-place copy
  std::atomic<uint64_t> *timestamp;
  uint64_t command_id;
  PreviousPtr prev;
  std::atomic<Delta *> next{nullptr};

  union {
    LabelId label;
    struct {
      PropertyId key;
      PropertyValue value;
    } property;
    struct {
      EdgeTypeId edge_type;
      VertexId vertex_id;
      EdgeRef edge;
    } vertex_edge;
  };
};

static_assert(alignof(Delta) >= 8, "The Delta should be aligned to at least 8!");

}  // namespace memgraph::storage::v3
