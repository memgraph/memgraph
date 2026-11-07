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

#include <algorithm>
#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <utility>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/ordered_property_paths.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/schema_info_types.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "versioning/stub_state.hpp"

// How a branch reads a fork object: what the stub holds, merged over the object as of the fork (read at View::OLD).
//
// The stub is read straight through the accessor, a key at a time, never decoded as a whole. Under READ_COMMITTED the
// diff engine's stock reads are not atomic across keys (a read whose delta walk straddles a commit sees a prefix of
// that commit's writes, and two reads can straddle one), so every decision here rests on ONE stock read, or on reads
// whose torn outcomes are all the old or the new answer:
//  - a property is decided by its value first (one read); only when that is absent does one multi-key read of the
//    value and its removal marker decide. The writer (ApplyProperties) turns the marker on before it drops the value
//    and off after it sets it, so a torn stub may hold both and the value wins.
//  - a label is decided by its own id, then its marker. This relies on the writer never recording what the fork already
//    implies (no added label the fork has, no marker for a label the fork lacks): a label then moves only between
//    "unchanged" and "added", or "unchanged" and "removed", and a torn pair of reads is still the old or the new
//    answer.
//  - the whole label or property set is one read.
namespace memgraph::versioning::stub {

namespace detail {

// A property and its removal marker as the stub itself holds them; a key the stub does not hold reads as null.
struct Decided {
  storage::PropertyValue value;
  storage::PropertyValue marker;
};

// One multi-key read of `key` and its marker, whatever order the store needs them asked in.
template <typename Stub>
storage::Result<Decided> ReadDecided(const Stub &stub, storage::PropertyId key, storage::View view) {
  auto const ordered = storage::OrderedPropertyPaths::FromPropertyIds(std::array{key, Mark(key)});
  std::array<storage::PropertyValue, 2> values;
  if (auto read = stub.ReadPropertyValues(ordered, values, view); !read) return std::unexpected(read.error());
  auto const first_is_key = ordered.paths()[0] == storage::PropertyPath{key};
  return Decided{.value = std::move(values[first_is_key ? 0 : 1]), .marker = std::move(values[first_is_key ? 1 : 0])};
}

}  // namespace detail

// nullopt: the stub does not decide, defer to the fork vertex. The marker is asked first: a stub may hold a fork label
// as a copy (it keeps a mirrored index key complete) and then the marker of its removal, and the marker wins.
inline storage::Result<std::optional<bool>> DecidesLabel(const storage::VertexAccessor &stub, storage::LabelId label,
                                                         storage::View view) {
  auto removed = stub.HasLabel(Mark(label), view);
  if (!removed) return std::unexpected(removed.error());
  if (*removed) return std::optional<bool>{false};
  auto added = stub.HasLabel(label, view);
  if (!added) return std::unexpected(added.error());
  if (*added) return std::optional<bool>{true};
  return std::optional<bool>{};
}

// `stub_labels` is the stub's label set (never holding the delete label, which the caller maps to a deleted object):
// markers hide fork labels, plain labels are the branch's additions. A plain label whose marker is also there is a copy
// of a fork label that was removed since, so it is hidden too.
inline storage::VertexKey MergeLabels(const storage::VertexKey &stub_labels, storage::VertexKey fork_labels) {
  auto const hidden = [&](storage::LabelId label) { return std::ranges::contains(stub_labels, Mark(label)); };
  fork_labels.erase(std::remove_if(fork_labels.begin(), fork_labels.end(), hidden), fork_labels.end());
  for (auto const label : stub_labels) {
    if (!storage::IsReservedId(label) && !hidden(label) && !std::ranges::contains(fork_labels, label)) {
      fork_labels.push_back(label);
    }
  }
  return fork_labels;
}

// `stub_properties` is the stub's property map (never holding the edge delete property): marked keys are removed from
// the fork's, then the stub's own values win.
template <typename Fork>
storage::Result<std::map<storage::PropertyId, storage::PropertyValue>> MergeProperties(
    const std::map<storage::PropertyId, storage::PropertyValue> &stub_properties, const Fork &fork) {
  return fork.Properties(storage::View::OLD).transform([&](auto merged) {
    for (auto const &entry : stub_properties) {
      if (storage::IsReservedId(entry.first)) merged.erase(Strip(entry.first));
    }
    for (auto const &[pid, value] : stub_properties) {
      if (!storage::IsReservedId(pid)) merged.insert_or_assign(pid, value);
    }
    return merged;
  });
}

// The property as the branch sees it: the stub's value, else null when the stub removed it, else the fork's.
template <typename Stub, typename Fork>
storage::Result<storage::PropertyValue> MergeGetProperty(const Stub &stub, const Fork &fork, storage::PropertyId key,
                                                         storage::View view) {
  auto own = stub.GetProperty(key, view);
  if (!own) return own;
  if (!own->IsNull()) return own;
  auto decided = detail::ReadDecided(stub, key, view);
  if (!decided) return std::unexpected(decided.error());
  if (!decided->value.IsNull()) return std::move(decided->value);
  if (!decided->marker.IsNull()) return storage::PropertyValue{};
  return fork.GetProperty(key, storage::View::OLD);
}

template <typename Stub, typename Fork>
storage::Result<uint64_t> MergeGetPropertySize(const Stub &stub, const Fork &fork, storage::PropertyId key,
                                               storage::View view) {
  auto own = stub.GetPropertySize(key, view);
  if (!own) return own;
  if (*own != 0) return own;
  auto decided = detail::ReadDecided(stub, key, view);
  if (!decided) return std::unexpected(decided.error());
  if (!decided->value.IsNull()) {
    storage::PropertyStore store;
    store.SetProperty(key, decided->value);
    return uint64_t{store.PropertySize(key)};
  }
  if (!decided->marker.IsNull()) return uint64_t{0};
  return fork.GetPropertySize(key, storage::View::OLD);
}

}  // namespace memgraph::versioning::stub
