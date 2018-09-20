#pragma once

#include <cstdint>
#include <functional>
#include <limits>

#include "glog/logging.h"

#include "storage/serialization.capnp.h"
#include "utils/total_ordering.hpp"

namespace storage {

// In case of a new location Mask value has to be updated.
enum class Location : uint16_t { Memory = 0x8000, Disk = 0x0000 };

/**
 * |-------------|--------------|
 * |---location--|------id------|
 * |-Memory|Disk-|-----2^15-----|
 */
template <typename TSpecificType>
class Common : public utils::TotalOrdering<TSpecificType> {
 public:
  using IdT = uint16_t;

  Common() = default;
  explicit Common(const IdT id, const Location location = Location::Memory)
      : id_((id & Mask) | static_cast<uint16_t>(location)) {
    // TODO(ipaljak): A better way would be to throw an exception
    // and send a message to the user that a new Id can't be created.
    // By doing that, database instance will continue to work and user
    // has a chance to make an appropriate action.
    // CHECK isn't user friendly at all because it will immediately
    // terminate the whole process.
    // TODO implement throw and error handling
    CHECK(id <= Mask) << "Number of used ids overflowed!";
  }

  friend bool operator==(const TSpecificType &a, const TSpecificType &b) {
    return a.Id() == b.Id();
  }
  friend bool operator<(const TSpecificType &a, const TSpecificType &b) {
    return a.Id() < b.Id();
  }

  IdT Id() const { return static_cast<IdT>(id_ & Mask); }
  Location Location() const {
    return static_cast<enum Location>(id_ & NotMask);
  }

  struct Hash {
    std::hash<IdT> hash{};
    size_t operator()(const TSpecificType &t) const { return hash(t.id_); }
  };

  IdT id_{0};
 protected:
  ~Common() {}

 private:
  static constexpr IdT Mask = std::numeric_limits<IdT>::max() >> 1;
  static constexpr IdT NotMask = ~Mask;
};

template <class Type>
void Save(const Common<Type> &common, capnp::Common::Builder *builder) {
  builder->setStorage(common.id_);
}

template <class Type>
void Load(Common<Type> *common, const capnp::Common::Reader &reader) {
  common->id_ = reader.getStorage();
}

class Label final : public Common<Label> {
  using Common::Common;
};

class EdgeType final : public Common<EdgeType> {
  using Common::Common;
};

class Property final : public Common<Property> {
  using Common::Common;
};

}  // namespace storage

namespace std {

template <>
struct hash<storage::Label> : public storage::Common<storage::Label>::Hash {};
template <>
struct hash<storage::EdgeType>
    : public storage::Common<storage::EdgeType>::Hash {};
template <>
struct hash<storage::Property>
    : public storage::Common<storage::Property>::Hash {};

}  // namespace std
