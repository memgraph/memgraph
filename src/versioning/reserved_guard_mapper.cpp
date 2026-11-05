// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "versioning/reserved_guard_mapper.hpp"

#include <utility>

#include "storage/v2/id_types.hpp"
#include "versioning/stub_state.hpp"

namespace memgraph::versioning {

namespace {

constexpr uint64_t kReservedBit = storage::kReservedIdBit;
constexpr uint64_t kDeleteId = stub::kDeleteId;

// Ids are stored as 32 bits; anything wider cannot be a marker.
constexpr bool IsMarkerId(uint64_t id) noexcept { return id <= UINT32_MAX && (id & kReservedBit) != 0; }

std::string Escape(std::string_view real_name) {
  if (!real_name.starts_with(ReservedGuardMapper::kReservedPrefix)) return std::string{real_name};
  return std::string{ReservedGuardMapper::kEscapePrefix}.append(real_name);
}

// The inverse of Escape on its image; any other name is already real.
std::string_view Unescape(std::string_view name) {
  if (name.starts_with(ReservedGuardMapper::kEscapePrefix))
    name.remove_prefix(ReservedGuardMapper::kEscapePrefix.size());
  return name;
}

}  // namespace

ReservedGuardMapper::ReservedGuardMapper(std::shared_ptr<storage::NameIdMapper> inner) : inner_{std::move(inner)} {
  DMG_ASSERT(inner_ != nullptr, "the guard wraps main's mapper");
}

uint64_t ReservedGuardMapper::NameToId(std::string_view name) {
  if (name == kDeletedName) return kDeleteId;
  if (name.starts_with(kRemovedPrefix)) {
    name.remove_prefix(kRemovedPrefix.size());
    return inner_->NameToId(Unescape(name)) | kReservedBit;
  }
  return inner_->NameToId(Unescape(name));
}

std::optional<uint64_t> ReservedGuardMapper::NameToIdIfExists(std::string_view name) const {
  if (name == kDeletedName) return kDeleteId;
  if (name.starts_with(kRemovedPrefix)) {
    name.remove_prefix(kRemovedPrefix.size());
    auto const id = inner_->NameToIdIfExists(Unescape(name));
    if (!id) return std::nullopt;
    return *id | kReservedBit;
  }
  return inner_->NameToIdIfExists(Unescape(name));
}

// The decorator never maps ids itself, so the base class's id -> name list holds exactly the interned names. It is
// append-only (see NameIdMapper::IdToName): references into it stay valid.
template <typename MakeName>
const std::string &ReservedGuardMapper::Intern(uint64_t id, MakeName &&make_name) {
  if (auto const found = MaybeIdToName(id)) return found->get();
  id_to_name_.access().insert({id, make_name()});
  return MaybeIdToName(id)->get();
}

const std::string &ReservedGuardMapper::IdToName(uint64_t id) {
  if (!IsMarkerId(id)) {
    const std::string &name = inner_->IdToName(id);
    if (!name.starts_with(kReservedPrefix)) return name;
    return Intern(id, [&] { return Escape(name); });
  }
  return Intern(id, [&] {
    if (id == kDeleteId) return std::string{kDeletedName};
    return std::string{kRemovedPrefix}.append(Escape(inner_->IdToName(id & ~kReservedBit)));
  });
}

}  // namespace memgraph::versioning
