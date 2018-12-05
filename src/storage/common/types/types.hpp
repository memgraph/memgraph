#pragma once

#include <cstdint>
#include <functional>
#include <limits>

#include <glog/logging.h>

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
  Location Location() const {
    return static_cast<enum Location>(id_ & IdNotMask);
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
  Location Location() const {
    return static_cast<enum Location>(id_ & IdNotMask);
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
  Location Location() const {
    return static_cast<enum Location>(id_ & IdNotMask);
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
