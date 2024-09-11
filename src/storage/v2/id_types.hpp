// Copyright 2024 Memgraph Ltd.
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

#include <charconv>
#include <compare>
#include <functional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utils/exceptions.hpp>

#include <boost/functional/hash.hpp>
#include "utils/cast.hpp"
#include "utils/string.hpp"

namespace memgraph::storage {

#define STORAGE_DEFINE_ID_TYPE(name, type_store, type_conv, parse)                                      \
  class name final {                                                                                    \
   private:                                                                                             \
    explicit name(type_store id) : id_{id} {}                                                           \
                                                                                                        \
   public:                                                                                              \
    /* Default constructor to allow serialization or preallocation. */                                  \
    name() = default;                                                                                   \
                                                                                                        \
    static name FromUint(type_store id) { return name{id}; }                                            \
    static name FromInt(type_conv id) { return name{utils::MemcpyCast<type_store>(id)}; }               \
    type_store AsUint() const { return id_; }                                                           \
    type_conv AsInt() const { return utils::MemcpyCast<type_conv>(id_); }                               \
    static name FromString(std::string_view id) { return name{parse(id)}; }                             \
    std::string ToString() const { return std::to_string(id_); }                                        \
    friend bool operator==(const name &first, const name &second) { return first.id_ == second.id_; }   \
    friend bool operator<(const name &first, const name &second) { return first.id_ < second.id_; }     \
    friend auto operator<=>(const name &first, const name &second) { return first.id_ <=> second.id_; } \
                                                                                                        \
   private:                                                                                             \
    type_store id_;                                                                                     \
  };                                                                                                    \
  static_assert(std::is_trivially_copyable_v<name>, "storage::" #name " must be trivially copyable!");

STORAGE_DEFINE_ID_TYPE(Gid, uint64_t, int64_t, utils::ParseStringToUint64);
STORAGE_DEFINE_ID_TYPE(LabelId, uint32_t, int32_t, utils::ParseStringToUint32);
STORAGE_DEFINE_ID_TYPE(PropertyId, uint32_t, int32_t, utils::ParseStringToUint32);
STORAGE_DEFINE_ID_TYPE(EdgeTypeId, uint32_t, int32_t, utils::ParseStringToUint32);

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

}  // namespace std
