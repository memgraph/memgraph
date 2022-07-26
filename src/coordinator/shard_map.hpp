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

#include "io/address.hpp"

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

using CompoundKey = std::vector<memgraph::storage::PropertyValue>;
using Shard = std::vector<AddressAndStatus>;
using Shards = std::map<CompoundKey, Shard>;

// use string for intermachine communication and NameIdMapper within the machine
using Label = std::string;

struct ShardMap {
  uint64_t shard_map_version;
  std::map<Label, Shards> shards;

 public:
  Shards GetShardsForRange(Label label, CompoundKey start, CompoundKey end);

  Shard GetShardForKey(Label label, CompoundKey key);
};

}  // namespace memgraph::coordinator
