#pragma once

#include <string>

#include "boost/serialization/base_object.hpp"

#include "utils/total_ordering.hpp"

namespace GraphDbTypes {

template <typename TSpecificType>
class Common : public TotalOrdering<TSpecificType> {
 public:
  using StorageT = uint16_t;

  Common() {}
  explicit Common(const StorageT storage) : storage_(storage) {}

  friend bool operator==(const TSpecificType &a, const TSpecificType &b) {
    return a.storage_ == b.storage_;
  }
  friend bool operator<(const TSpecificType &a, const TSpecificType &b) {
    return a.storage_ < b.storage_;
  }

  StorageT storage() const { return storage_; }

  struct Hash {
    std::hash<StorageT> hash{};
    size_t operator()(const TSpecificType &t) const { return hash(t.storage_); }
  };

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &storage_;
  }

  StorageT storage_{0};
};

class Label : public Common<Label> {
  using Common::Common;

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Common<Label>>(*this);
  }
};

class EdgeType : public Common<EdgeType> {
  using Common::Common;

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Common<EdgeType>>(*this);
  }
};

class Property : public Common<Property> {
  using Common::Common;

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Common<Property>>(*this);
  }
};

};  // namespace GraphDbTypes

namespace std {

template <>
struct hash<GraphDbTypes::Label>
    : public GraphDbTypes::Common<GraphDbTypes::Label>::Hash {};
template <>
struct hash<GraphDbTypes::EdgeType>
    : public GraphDbTypes::Common<GraphDbTypes::EdgeType>::Hash {};
template <>
struct hash<GraphDbTypes::Property>
    : public GraphDbTypes::Common<GraphDbTypes::Property>::Hash {};

}  // namespace std
