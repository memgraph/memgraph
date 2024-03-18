// Copyright 2024 Memgraph Ltd.
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

#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
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
    case PreviousPtr::Type::NULLPTR:
      return b.type == PreviousPtr::Type::NULLPTR;
  }
}

inline bool operator!=(const PreviousPtr::Pointer &a, const PreviousPtr::Pointer &b) { return !(a == b); }

struct opt_str {
  opt_str(std::optional<std::string> const &other) : str_{other ? new_cstr(*other) : nullptr} {}

  ~opt_str() { delete[] str_; }

  auto as_opt_str() const -> std::optional<std::string> {
    if (!str_) return std::nullopt;
    return std::optional<std::string>{std::in_place, str_};
  }

 private:
  static auto new_cstr(std::string const &str) -> char const * {
    auto *mem = new char[str.length() + 1];
    strcpy(mem, str.c_str());
    return mem;
  }

  char const *str_ = nullptr;
};

struct Delta {
  enum class Action : std::uint8_t {
    /// Use for Vertex and Edge
    /// Used for disk storage for modifying MVCC logic and storing old key. Storing old key is necessary for
    /// deleting old-data (compaction).
    DELETE_DESERIALIZED_OBJECT,
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
  Delta(DeleteDeserializedObjectTag /*tag*/, uint64_t ts, std::optional<std::string> old_disk_key)
      : timestamp(new std::atomic<uint64_t>(ts)), command_id(0), old_disk_key{.value = old_disk_key} {}

  Delta(DeleteObjectTag /*tag*/, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), action(Action::DELETE_OBJECT) {}

  Delta(RecreateObjectTag /*tag*/, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), action(Action::RECREATE_OBJECT) {}

  Delta(AddLabelTag /*tag*/, LabelId label, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), label{.action = Action::ADD_LABEL, .value = label} {}

  Delta(RemoveLabelTag /*tag*/, LabelId label, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : timestamp(timestamp), command_id(command_id), label{.action = Action::REMOVE_LABEL, .value = label} {}

  Delta(SetPropertyTag /*tag*/, PropertyId key, PropertyValue value, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : timestamp(timestamp),
        command_id(command_id),
        property{
            .action = Action::SET_PROPERTY, .key = key, .value = std::make_unique<PropertyValue>(std::move(value))} {}

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
      case Action::DELETE_DESERIALIZED_OBJECT:
        std::destroy_at(&old_disk_key.value);
        delete timestamp;
        timestamp = nullptr;
        break;
      case Action::SET_PROPERTY:
        property.value.reset();
        break;
    }
  }

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
      std::unique_ptr<storage::PropertyValue> value;
    } property;
    struct {
      Action action;
      EdgeTypeId edge_type;
      Vertex *vertex;
      EdgeRef edge;
    } vertex_edge;
  };
};

static_assert(alignof(Delta) >= 8, "The Delta should be aligned to at least 8!");

}  // namespace memgraph::storage
