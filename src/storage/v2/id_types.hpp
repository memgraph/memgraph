// Copyright 2023 Memgraph Ltd.
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
#include <functional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utils/exceptions.hpp>

#include "utils/cast.hpp"
#include "utils/string.hpp"

namespace memgraph::storage {

#define STORAGE_DEFINE_ID_TYPE(name)                                                                          \
  class name final {                                                                                          \
   private:                                                                                                   \
    explicit name(uint64_t id) : id_(id) {}                                                                   \
                                                                                                              \
   public:                                                                                                    \
    /* Default constructor to allow serialization or preallocation. */                                        \
    name() = default;                                                                                         \
                                                                                                              \
    static name FromUint(uint64_t id) { return name{id}; }                                                    \
    static name FromInt(int64_t id) { return name{utils::MemcpyCast<uint64_t>(id)}; }                         \
    uint64_t AsUint() const { return id_; }                                                                   \
    int64_t AsInt() const { return utils::MemcpyCast<int64_t>(id_); }                                         \
    static name FromString(std::string_view id) { return name{utils::ParseStringToUint64(id)}; }              \
    std::string ToString() const { return std::to_string(id_); }                                              \
                                                                                                              \
   private:                                                                                                   \
    uint64_t id_;                                                                                             \
  };                                                                                                          \
  static_assert(std::is_trivially_copyable_v<name>, "storage::" #name " must be trivially copyable!");        \
  inline bool operator==(const name &first, const name &second) { return first.AsUint() == second.AsUint(); } \
  inline bool operator!=(const name &first, const name &second) { return first.AsUint() != second.AsUint(); } \
  inline bool operator<(const name &first, const name &second) { return first.AsUint() < second.AsUint(); }   \
  inline bool operator>(const name &first, const name &second) { return first.AsUint() > second.AsUint(); }   \
  inline bool operator<=(const name &first, const name &second) { return first.AsUint() <= second.AsUint(); } \
  inline bool operator>=(const name &first, const name &second) { return first.AsUint() >= second.AsUint(); }

STORAGE_DEFINE_ID_TYPE(Gid);
STORAGE_DEFINE_ID_TYPE(LabelId);
STORAGE_DEFINE_ID_TYPE(PropertyId);
STORAGE_DEFINE_ID_TYPE(EdgeTypeId);

#undef STORAGE_DEFINE_ID_TYPE

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

}  // namespace std
