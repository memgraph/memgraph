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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "storage/v2/name_id_mapper.hpp"

namespace memgraph::versioning {

// The name mapper of a branch's diff engine. The engine's stock code (its WAL encoder, for one) turns the stub
// markers' reserved ids (see stub_state.hpp) into names, and the ids a restart assigns to main's names differ from
// run to run, so a marker is named after what it marks:
//
//   Mark(x)      <-> "__mg_removed_" + x
//   kDeleteLabel <-> "__mg_deleted"
//
// Any real name that starts with "__mg_" is written "__mg_lit_" + name, so that the mapping is a bijection and a user
// name can never read back as a marker. Real ids and names otherwise pass through to the wrapped mapper.
//
// Translation is unconditional: a marker that leaks to an output shows as its name instead of aborting.
//
// NameToId reads every "__mg_"-prefixed name as the encoded form, so text a user typed must be resolved on the
// wrapped mapper, not on this one.
class ReservedGuardMapper final : public storage::NameIdMapper {
 public:
  static constexpr std::string_view kReservedPrefix = "__mg_";
  static constexpr std::string_view kEscapePrefix = "__mg_lit_";
  static constexpr std::string_view kRemovedPrefix = "__mg_removed_";
  static constexpr std::string_view kDeletedName = "__mg_deleted";

  explicit ReservedGuardMapper(std::shared_ptr<storage::NameIdMapper> inner);

  /// @throw std::bad_alloc if unable to insert a new mapping
  uint64_t NameToId(std::string_view name) override;

  std::optional<uint64_t> NameToIdIfExists(std::string_view name) const override;

  // Names are interned: the returned reference stays valid, and distinct from any other, for the mapper's lifetime.
  const std::string &IdToName(uint64_t id) override;

  // The wrapped mapper is main's and is shared: a branch must never reset it.
  void Clear() override {}

 private:
  template <typename MakeName>
  const std::string &Intern(uint64_t id, MakeName &&make_name);

  std::shared_ptr<storage::NameIdMapper> inner_;
};

}  // namespace memgraph::versioning
