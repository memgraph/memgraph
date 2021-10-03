// Copyright 2021 Memgraph Ltd.
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

#include <functional>
#include <type_traits>

#include "utils/cast.hpp"

namespace storage {

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
                                                                                                              \
   private:                                                                                                   \
    uint64_t id_;                                                                                             \
  };                                                                                                          \
  static_assert(std::is_trivially_copyable<name>::value, "storage::" #name " must be trivially copyable!");   \
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

}  // namespace storage

namespace std {

template <>
struct hash<storage::Gid> {
  size_t operator()(const storage::Gid &id) const noexcept { return id.AsUint(); }
};

template <>
struct hash<storage::LabelId> {
  size_t operator()(const storage::LabelId &id) const noexcept { return id.AsUint(); }
};

template <>
struct hash<storage::PropertyId> {
  size_t operator()(const storage::PropertyId &id) const noexcept { return id.AsUint(); }
};

template <>
struct hash<storage::EdgeTypeId> {
  size_t operator()(const storage::EdgeTypeId &id) const noexcept { return id.AsUint(); }
};

}  // namespace std
