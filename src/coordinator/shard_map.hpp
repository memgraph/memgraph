// Copyright 2022 Memgraph Ltd.
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

#include <map>
#include <vector>

#include "coordinator/hybrid_logical_clock.hpp"
#include "io/address.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"

namespace memgraph::coordinator {

using memgraph::io::Address;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::PropertyId;

enum class Status : uint8_t {
  CONSENSUS_PARTICIPANT,
  INITIALIZING,
  // TODO(tyler) this will possibly have more states,
  // depending on the reconfiguration protocol that we
  // implement.
};

struct AddressAndStatus {
  memgraph::io::Address address;
  Status status;

  friend bool operator<(const AddressAndStatus &lhs, const AddressAndStatus &rhs) { return lhs.address < rhs.address; }
};

using CompoundKey = std::vector<memgraph::storage::v3::PropertyValue>;
using Shard = std::vector<AddressAndStatus>;
using Shards = std::map<CompoundKey, Shard>;
using LabelName = std::string;
using PropertyName = std::string;
using PropertyMap = std::map<PropertyName, PropertyId>;

struct ShardMap {
  Hlc shard_map_version;
  uint64_t max_property_id;
  std::map<PropertyName, PropertyId> properties;
  uint64_t max_label_id;
  std::map<LabelName, LabelId> labels;
  std::map<LabelId, Shards> shards;

  auto FindShardToInsert(const LabelName &name, CompoundKey &key) {
    MG_ASSERT(labels.contains(name));
    const auto id = labels.find(name)->second;
    auto &shards_ref = shards[id];
    auto it =
        std::find_if(shards_ref.rbegin(), shards_ref.rend(), [&key](const auto &shard) { return shard.first <= key; });
    MG_ASSERT(it != shards_ref.rbegin());
    return it;
  }

  // TODO(gabor) later we will want to update the wallclock time with
  // the given Io<impl>'s time as well
  Hlc IncrementShardMapVersion() noexcept {
    ++shard_map_version.logical_id;
    return shard_map_version;
  }

  Hlc GetHlc() const noexcept { return shard_map_version; }

  bool SplitShard(Hlc previous_shard_map_version, LabelId label_id, CompoundKey key) {
    if (previous_shard_map_version != shard_map_version) {
      return false;
    }

    if (!shards.contains(label_id)) {
      return false;
    }

    auto &shards_in_map = shards.at(label_id);
    MG_ASSERT(!shards_in_map.contains(key));

    // Finding the Shard that the new CompoundKey should map to.
    Shard shard_to_map_to;
    CompoundKey prev_key = ((*shards_in_map.begin()).first);

    for (auto iter = std::next(shards_in_map.begin()); iter != shards_in_map.end(); ++iter) {
      const auto &current_key = (*iter).first;
      if (key > prev_key && key < current_key) {
        shard_to_map_to = shards_in_map[prev_key];
      }

      prev_key = (*iter).first;
    }

    // Apply the split
    shards_in_map[key] = shard_to_map_to;

    return true;
  }

  bool InitializeNewLabel(std::string label_name, Hlc last_shard_map_version) {
    if (shard_map_version != last_shard_map_version) {
      return false;
    }

    if (labels.contains(label_name)) {
      return false;
    }

    const LabelId label_id = LabelId::FromUint(++max_label_id);

    labels.emplace(label_name, label_id);
    shards.emplace(label_id, Shards{});

    IncrementShardMapVersion();

    return true;
  }

  void AddServer(Address server_address) {
    // Find a random place for the server to plug in
  }

  LabelId GetLabelId(const std::string &label) const { return labels.at(label); }

  Shards GetShardsForRange(LabelName label_name, CompoundKey start_key, CompoundKey end_key) {
    MG_ASSERT(start_key <= end_key);
    MG_ASSERT(labels.contains(label_name));

    LabelId label_id = labels.at(label_name);

    const auto &shard_for_label = shards.at(label_id);

    auto it = std::prev(shard_for_label.upper_bound(start_key));
    const auto end_it = shard_for_label.upper_bound(end_key);

    Shards shards{};

    for (; it != end_it; it++) {
      shards.emplace(it->first, it->second);
    }

    return shards;
  }

  Shard GetShardForKey(LabelName label_name, CompoundKey key) {
    MG_ASSERT(labels.contains(label_name));

    LabelId label_id = labels.at(label_name);

    const auto &shard_for_label = shards.at(label_id);

    return std::prev(shard_for_label.upper_bound(key))->second;
  }

  Shard GetShardForKey(LabelId label_id, CompoundKey key) {
    MG_ASSERT(shards.contains(label_id));

    const auto &shard_for_label = shards.at(label_id);

    return std::prev(shard_for_label.upper_bound(key))->second;
  }

  PropertyMap AllocatePropertyIds(std::vector<PropertyName> &new_properties) {
    PropertyMap ret{};

    bool mutated = false;

    for (const auto &property_name : new_properties) {
      if (properties.contains(property_name)) {
        auto property_id = properties.at(property_name);
        ret.emplace(property_name, property_id);
      } else {
        mutated = true;

        const PropertyId property_id = PropertyId::FromUint(++max_property_id);

        ret.emplace(property_name, property_id);

        properties.emplace(property_name, property_id);
      }
    }

    if (mutated) {
      IncrementShardMapVersion();
    }

    return ret;
  }

  std::optional<PropertyId> GetPropertyId(std::string &property_name) {
    if (properties.contains(property_name)) {
      return properties.at(property_name);
    } else {
      return std::nullopt;
    }
  }
};

}  // namespace memgraph::coordinator
