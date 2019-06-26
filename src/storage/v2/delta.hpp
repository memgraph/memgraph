#pragma once

#include <atomic>

namespace storage {

struct Delta {
  enum class Action {
    DELETE_OBJECT,
    ADD_LABEL,
    REMOVE_LABEL,
  };

  Delta(Action action, uint64_t value, std::atomic<uint64_t> *timestamp,
        uint64_t command_id)
      : action(action),
        value(value),
        timestamp(timestamp),
        command_id(command_id),
        next(nullptr) {}

  Delta(const Delta &) = delete;
  Delta &operator=(const Delta &) = delete;

  Delta(Delta &&other) noexcept
      : action(other.action),
        value(other.value),
        timestamp(other.timestamp),
        command_id(other.command_id),
        next(other.next.load()) {}

  Delta &operator=(Delta &&other) noexcept {
    if (this == &other) return *this;

    action = other.action;
    value = other.value;
    timestamp = other.timestamp;
    command_id = other.command_id;
    next = other.next.load();

    return *this;
  }

  ~Delta() {}

  Action action;
  uint64_t value;

  // TODO: optimize with in-place copy
  std::atomic<uint64_t> *timestamp;
  uint64_t command_id;
  std::atomic<Delta *> next;
};

}  // namespace storage
