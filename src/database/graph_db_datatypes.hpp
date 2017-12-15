#pragma once

#include <string>

#include "utils/total_ordering.hpp"

namespace GraphDbTypes {

template <typename TSpecificType>
class Common : TotalOrdering<TSpecificType> {
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

  struct Hash {
    std::hash<StorageT> hash{};
    size_t operator()(const TSpecificType &t) const { return hash(t.storage_); }
  };

  /** Required for cereal serialization. */
  template <class Archive>
  void serialize(Archive &archive) {
    archive(storage_);
  }

 private:
  StorageT storage_{0};
};

class Label : public Common<Label> {
  using Common::Common;
};

class EdgeType : public Common<EdgeType> {
  using Common::Common;
};

class Property : public Common<Property> {
  using Common::Common;
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
};  // namespace std
