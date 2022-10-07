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

namespace memgraph::tests::simulation {

static constexpr auto kMinimumShards = 1;
static constexpr auto kMaximumShards = kMinimumShards + 5;

static constexpr auto kMinimumServers = 3;
static constexpr auto kMaximumServers = kMinimumServers + 5;

static constexpr auto kMinimumReplicationFactor = 1;
static constexpr auto kMaximumReplicationFactor = kMinimumReplicationFactor + 2;

}  // namespace memgraph::tests::simulation
