#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>

#include <glog/logging.h>

#include "utils/atomic.hpp"

namespace storage {

using IdT = uint16_t;

const IdT IdMask = std::numeric_limits<IdT>::max() >> 1;
const IdT IdNotMask = ~IdMask;

// In case of a new location Mask value has to be updated.
//
// |-------------|--------------|
// |---location--|------id------|
// |-Memory|Disk-|-----2^15-----|
enum class Location : IdT { Memory = 0x8000, Disk = 0x0000 };

class Label final {
 public:
  Label() = default;
  explicit Label(const IdT id, const Location location = Location::Memory)
      : id_((id & IdMask) | static_cast<IdT>(location)) {
    // TODO(ipaljak): A better way would be to throw an exception
    // and send a message to the user that a new Id can't be created.
    // By doing that, database instance will continue to work and user
    // has a chance to make an appropriate action.
    // CHECK isn't user friendly at all because it will immediately
    // terminate the whole process.
    // TODO implement throw and error handling
    CHECK(id <= IdMask) << "Number of used ids overflowed!";
  }

  IdT Id() const { return static_cast<IdT>(id_ & IdMask); }
  storage::Location Location() const {
    return static_cast<storage::Location>(id_ & IdNotMask);
  }

  friend bool operator==(const Label &a, const Label &b) {
    return a.Id() == b.Id();
  }
  friend bool operator!=(const Label &a, const Label &b) {
    return a.Id() != b.Id();
  }
  friend bool operator<(const Label &a, const Label &b) {
    return a.Id() < b.Id();
  }
  friend bool operator>(const Label &a, const Label &b) {
    return a.Id() > b.Id();
  }
  friend bool operator<=(const Label &a, const Label &b) {
    return a.Id() <= b.Id();
  }
  friend bool operator>=(const Label &a, const Label &b) {
    return a.Id() >= b.Id();
  }

  IdT id_{0};
};

class EdgeType final {
 public:
  EdgeType() = default;
  explicit EdgeType(const IdT id, const Location location = Location::Memory)
      : id_((id & IdMask) | static_cast<IdT>(location)) {
    // TODO(ipaljak): A better way would be to throw an exception
    // and send a message to the user that a new Id can't be created.
    // By doing that, database instance will continue to work and user
    // has a chance to make an appropriate action.
    // CHECK isn't user friendly at all because it will immediately
    // terminate the whole process.
    // TODO implement throw and error handling
    CHECK(id <= IdMask) << "Number of used ids overflowed!";
  }

  IdT Id() const { return static_cast<IdT>(id_ & IdMask); }
  storage::Location Location() const {
    return static_cast<storage::Location>(id_ & IdNotMask);
  }

  friend bool operator==(const EdgeType &a, const EdgeType &b) {
    return a.Id() == b.Id();
  }
  friend bool operator!=(const EdgeType &a, const EdgeType &b) {
    return a.Id() != b.Id();
  }
  friend bool operator<(const EdgeType &a, const EdgeType &b) {
    return a.Id() < b.Id();
  }
  friend bool operator>(const EdgeType &a, const EdgeType &b) {
    return a.Id() > b.Id();
  }
  friend bool operator<=(const EdgeType &a, const EdgeType &b) {
    return a.Id() <= b.Id();
  }
  friend bool operator>=(const EdgeType &a, const EdgeType &b) {
    return a.Id() >= b.Id();
  }

  IdT id_{0};
};

class Property final {
 public:
  Property() = default;
  explicit Property(const IdT id, const Location location = Location::Memory)
      : id_((id & IdMask) | static_cast<IdT>(location)) {
    // TODO(ipaljak): A better way would be to throw an exception
    // and send a message to the user that a new Id can't be created.
    // By doing that, database instance will continue to work and user
    // has a chance to make an appropriate action.
    // CHECK isn't user friendly at all because it will immediately
    // terminate the whole process.
    // TODO implement throw and error handling
    CHECK(id <= IdMask) << "Number of used ids overflowed!";
  }

  IdT Id() const { return static_cast<IdT>(id_ & IdMask); }
  storage::Location Location() const {
    return static_cast<storage::Location>(id_ & IdNotMask);
  }

  friend bool operator==(const Property &a, const Property &b) {
    return a.Id() == b.Id();
  }
  friend bool operator!=(const Property &a, const Property &b) {
    return a.Id() != b.Id();
  }
  friend bool operator<(const Property &a, const Property &b) {
    return a.Id() < b.Id();
  }
  friend bool operator>(const Property &a, const Property &b) {
    return a.Id() > b.Id();
  }
  friend bool operator<=(const Property &a, const Property &b) {
    return a.Id() <= b.Id();
  }
  friend bool operator>=(const Property &a, const Property &b) {
    return a.Id() >= b.Id();
  }

  IdT id_{0};
};

/** Global ID of a record in the database. */
using Gid = uint64_t;

/** Threadsafe generation of new global IDs. */
class GidGenerator {
 public:
  /**
   * Returns a globally unique identifier.
   *
   * @param requested_gid - The desired gid. If given, it will be returned and
   * this generator's state updated accordingly.
   */
  Gid Next(std::optional<Gid> requested_gid = std::nullopt) {
    if (requested_gid) {
      utils::EnsureAtomicGe(next_local_id_, *requested_gid + 1);
      return *requested_gid;
    } else {
      return next_local_id_++;
    }
  }

 private:
  std::atomic<uint64_t> next_local_id_{0};
};

}  // namespace storage

namespace std {
template <>
struct hash<storage::Label> {
  size_t operator()(const storage::Label &k) const {
    return hash<storage::IdT>()(k.Id());
  }
};

template <>
struct hash<storage::EdgeType> {
  size_t operator()(const storage::EdgeType &k) const {
    return hash<storage::IdT>()(k.Id());
  }
};

template <>
struct hash<storage::Property> {
  size_t operator()(const storage::Property &k) const {
    return hash<storage::IdT>()(k.Id());
  }
};
}  // namespace std
