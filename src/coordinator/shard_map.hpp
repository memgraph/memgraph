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
#include "storage/v3/property_value.hpp"

namespace memgraph::coordinator {

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
};

using memgraph::io::Address;

using CompoundKey = std::vector<memgraph::storage::v3::PropertyValue>;
using Shard = std::vector<AddressAndStatus>;
using Shards = std::map<CompoundKey, Shard>;

// use string for intermachine communication and NameIdMapper within the machine
using Label = std::string;

struct ShardMap {
  Hlc shard_map_version;
  std::map<Label, Shards> shards;

  // TODO(gabor) later we will want to update the wallclock time with
  // the given Io<impl>'s time as well. This function should just be
  // replaced with operator== since it is already overloaded for Hlc
  // objects.
  bool CompareShardMapVersions(Hlc one, Hlc two) { return one.logical_id == two.logical_id; }

 public:
  // TODO(gabor) later we will want to update the wallclock time with
  // the given Io<impl>'s time as well
  Hlc UpdateShardMapVersion() noexcept {
    ++shard_map_version.logical_id;
    return shard_map_version;
  }

  Hlc GetHlc() const noexcept { return shard_map_version; }

  bool SplitShard(Hlc previous_shard_map_version, Label label, CompoundKey key) {
    if (CompareShardMapVersions(previous_shard_map_version, shard_map_version)) {
      MG_ASSERT(shards.contains(label));
      auto &shards_in_map = shards[label];
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

    return false;
  }

  void AddServer(Address server_address) {
    // Find a random place for the server to plug in
  }

  std::map<Label, Shards> &GetShards() noexcept { return shards; }

  Shards GetShardsForRange(Label label, CompoundKey start, CompoundKey end);

  Shard GetShardForKey(Label label, CompoundKey key) { return shards.at(label).at(key); }
};

}  // namespace memgraph::coordinator
