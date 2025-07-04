// Copyright 2025 Memgraph Ltd.
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

#include <compare>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

#include <boost/functional/hash.hpp>
#include "utils/cast.hpp"

namespace memgraph::storage {

#define STORAGE_DEFINE_ID_TYPE(name, type_store, type_conv)                               \
  class name final {                                                                      \
   private:                                                                               \
    explicit name(type_store id) : id_{id} {}                                             \
                                                                                          \
   public:                                                                                \
    /* Default constructor to allow serialization or preallocation. */                    \
    name() = default;                                                                     \
                                                                                          \
    static name FromUint(type_store id) { return name{id}; }                              \
    static name FromInt(type_conv id) { return name{utils::MemcpyCast<type_store>(id)}; } \
    type_store AsUint() const { return id_; }                                             \
    type_conv AsInt() const { return utils::MemcpyCast<type_conv>(id_); }                 \
    static name FromString(std::string_view id);                                          \
    std::string ToString() const;                                                         \
    friend bool operator==(const name &, const name &) = default;                         \
    friend bool operator<(const name &, const name &) = default;                          \
    friend std::strong_ordering operator<=>(const name &, const name &) = default;        \
    friend std::ostream &operator<<(std::ostream &os, const name &id) {                   \
      os << id.ToString();                                                                \
      return os;                                                                          \
    }                                                                                     \
                                                                                          \
   private:                                                                               \
    type_store id_;                                                                       \
  };                                                                                      \
  static_assert(std::is_trivially_copyable_v<name>, "storage::" #name " must be trivially copyable!");

STORAGE_DEFINE_ID_TYPE(Gid, uint64_t, int64_t);
STORAGE_DEFINE_ID_TYPE(LabelId, uint32_t, int32_t);
STORAGE_DEFINE_ID_TYPE(PropertyId, uint32_t, int32_t);
STORAGE_DEFINE_ID_TYPE(EdgeTypeId, uint32_t, int32_t);

#undef STORAGE_DEFINE_ID_TYPE

struct LabelPropKey {
  LabelPropKey(LabelId const &label, PropertyId const &property) : label_(label), property_(property) {}
  friend auto operator<=>(LabelPropKey const &, LabelPropKey const &) = default;

  auto label() const -> LabelId { return label_; }
  auto property() const -> PropertyId { return property_; }

 private:
  LabelId label_;
  PropertyId property_;
};

struct EdgeTypePropKey {
  EdgeTypePropKey(EdgeTypeId const &edge_type, PropertyId const &property)
      : edge_type_(edge_type), property_(property) {}
  friend auto operator<=>(EdgeTypePropKey const &, EdgeTypePropKey const &) = default;

  auto edge_type() const -> EdgeTypeId { return edge_type_; }
  auto property() const -> PropertyId { return property_; }

 private:
  EdgeTypeId edge_type_;
  PropertyId property_;
};

}  // namespace memgraph::storage

namespace std {

template <>
struct hash<memgraph::storage::Gid> {
  size_t operator()(const memgraph::storage::Gid &id) const noexcept { return id.AsUint(); }
};

template <>
struct hash<memgraph::storage::LabelId> {
  size_t operator()(const memgraph::storage::LabelId &id) const noexcept { return id.AsUint(); }
};

template <>
struct hash<memgraph::storage::PropertyId> {
  size_t operator()(const memgraph::storage::PropertyId &id) const noexcept { return id.AsUint(); }
};

template <>
struct hash<memgraph::storage::EdgeTypeId> {
  size_t operator()(const memgraph::storage::EdgeTypeId &id) const noexcept { return id.AsUint(); }
};

template <>
struct hash<memgraph::storage::LabelPropKey> {
  size_t operator()(const memgraph::storage::LabelPropKey &lpk) const noexcept {
    std::size_t seed = 0;
    boost::hash_combine(seed, lpk.label().AsUint());
    boost::hash_combine(seed, lpk.property().AsUint());
    return seed;
  }
};

template <>
struct hash<memgraph::storage::EdgeTypePropKey> {
  size_t operator()(const memgraph::storage::EdgeTypePropKey &etpk) const noexcept {
    std::size_t seed = 0;
    boost::hash_combine(seed, etpk.edge_type().AsUint());
    boost::hash_combine(seed, etpk.property().AsUint());
    return seed;
  }
};

}  // namespace std
