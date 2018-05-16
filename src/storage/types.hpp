#pragma once

#include <cstdint>
#include <functional>
#include <limits>

#include "boost/serialization/base_object.hpp"
#include "glog/logging.h"
#include "types.capnp.h"

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
  virtual ~Common() {}

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

  virtual void Save(capnp::Common::Builder *builder) const {
    builder->setStorage(id_);
  }

  virtual void Load(const capnp::Common::Reader &reader) {
    id_ = reader.getStorage();
  }

 private:
  static constexpr IdT Mask = std::numeric_limits<IdT>::max() >> 1;
  static constexpr IdT NotMask = ~Mask;

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &id_;
  }

  IdT id_{0};
};

class Label : public Common<Label> {
  using Common::Common;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Common<Label>>(*this);
  }
};

class EdgeType : public Common<EdgeType> {
  using Common::Common;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Common<EdgeType>>(*this);
  }
};

class Property : public Common<Property> {
  using Common::Common;

 private:
  friend class boost::serialization::access;
  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Common<Property>>(*this);
  }
};
};  // namespace storage

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
