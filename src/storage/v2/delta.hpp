#pragma once

#include <atomic>

#include "storage/v2/property_value.hpp"

namespace storage {

struct Delta {
  enum class Action {
    DELETE_OBJECT,
    RECREATE_OBJECT,
    ADD_LABEL,
    REMOVE_LABEL,
    SET_PROPERTY,
  };

  Delta(Action action, uint64_t key, const PropertyValue &value,
        std::atomic<uint64_t> *timestamp, uint64_t command_id)
      : action(action),
        key(key),
        value(value),
        timestamp(timestamp),
        command_id(command_id),
        prev(nullptr),
        next(nullptr) {}

  Delta(Delta &&other) noexcept
      : action(other.action),
        key(other.key),
        value(std::move(other.value)),
        timestamp(other.timestamp),
        command_id(other.command_id),
        prev(other.prev),
        next(other.next.load()) {}

  Delta(const Delta &) = delete;
  Delta &operator=(const Delta &) = delete;
  Delta &operator=(Delta &&other) = delete;

  ~Delta() {}

  Action action;

  uint64_t key;         // Used as the label id or the property id.
  PropertyValue value;  // Used as the property value (only for SET_PROPERTY).

  // TODO: optimize with in-place copy
  std::atomic<uint64_t> *timestamp;
  uint64_t command_id;
  Delta *prev;
  std::atomic<Delta *> next;
};

}  // namespace storage
