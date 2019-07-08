#pragma once

#include <atomic>

#include "storage/v2/property_value.hpp"

namespace storage {

// Forward declarations because we only store pointers here.
struct Vertex;
struct Edge;

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
  struct AddLabelTag {};
  struct RemoveLabelTag {};
  struct SetPropertyTag {};

  // Used only for Vertex
  struct AddInEdgeTag {};
  struct AddOutEdgeTag {};
  struct RemoveInEdgeTag {};
  struct RemoveOutEdgeTag {};

  Delta(DeleteObjectTag, std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::DELETE_OBJECT),
        timestamp(timestamp),
        command_id(command_id) {}

  Delta(RecreateObjectTag, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : action(Action::RECREATE_OBJECT),
        timestamp(timestamp),
        command_id(command_id) {}

  Delta(AddLabelTag, uint64_t label, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : action(Action::ADD_LABEL),
        timestamp(timestamp),
        command_id(command_id),
        label(label) {}

  Delta(RemoveLabelTag, uint64_t label, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : action(Action::REMOVE_LABEL),
        timestamp(timestamp),
        command_id(command_id),
        label(label) {}

  Delta(SetPropertyTag, uint64_t key, const PropertyValue &value,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::SET_PROPERTY),
        timestamp(timestamp),
        command_id(command_id),
        property({key, value}) {}

  Delta(AddInEdgeTag, uint64_t edge_type, Vertex *vertex, Edge *edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::ADD_IN_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, vertex, edge}) {}

  Delta(AddOutEdgeTag, uint64_t edge_type, Vertex *vertex, Edge *edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::ADD_OUT_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, vertex, edge}) {}

  Delta(RemoveInEdgeTag, uint64_t edge_type, Vertex *vertex, Edge *edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::REMOVE_IN_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, vertex, edge}) {}

  Delta(RemoveOutEdgeTag, uint64_t edge_type, Vertex *vertex, Edge *edge,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(Action::REMOVE_OUT_EDGE),
        timestamp(timestamp),
        command_id(command_id),
        vertex_edge({edge_type, vertex, edge}) {}

  Delta(Delta &&other) noexcept
      : action(other.action),
        timestamp(other.timestamp),
        command_id(other.command_id),
        prev(other.prev),
        next(other.next.load()) {
    switch (other.action) {
      case Action::DELETE_OBJECT:
      case Action::RECREATE_OBJECT:
        break;
      case Action::ADD_LABEL:
      case Action::REMOVE_LABEL:
        label = other.label;
        break;
      case Action::SET_PROPERTY:
        property.key = other.property.key;
        new (&property.value) PropertyValue(std::move(other.property.value));
        break;
      case Action::ADD_IN_EDGE:
      case Action::ADD_OUT_EDGE:
      case Action::REMOVE_IN_EDGE:
      case Action::REMOVE_OUT_EDGE:
        vertex_edge = other.vertex_edge;
        break;
    }

    // reset the action of other
    other.DestroyValue();
    other.action = Action::DELETE_OBJECT;
  }

  Delta(const Delta &) = delete;
  Delta &operator=(const Delta &) = delete;
  Delta &operator=(Delta &&other) = delete;

  ~Delta() { DestroyValue(); }

  Action action;

  // TODO: optimize with in-place copy
  std::atomic<uint64_t> *timestamp;
  uint64_t command_id;
  Delta *prev{nullptr};
  std::atomic<Delta *> next{nullptr};

  union {
    uint64_t label;
    struct {
      uint64_t key;
      storage::PropertyValue value;
    } property;
    struct {
      uint64_t edge_type;
      Vertex *vertex;
      Edge *edge;
    } vertex_edge;
  };

 private:
  void DestroyValue() {
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
};

}  // namespace storage
