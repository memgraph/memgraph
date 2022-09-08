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

#include <chrono>
#include <deque>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "common/types.hpp"
#include "coordinator/coordinator_client.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/rsm_client.hpp"
#include "io/rsm/shard_rsm.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/result.hpp"

using memgraph::coordinator::AddressAndStatus;
// using memgraph::coordinator::CompoundKey;
using memgraph::coordinator::Coordinator;
using memgraph::coordinator::CoordinatorClient;
using memgraph::coordinator::CoordinatorRsm;
using memgraph::coordinator::HlcRequest;
using memgraph::coordinator::HlcResponse;
// using memgraph::coordinator::Shard;
using memgraph::coordinator::ShardMap;
// using memgraph::coordinator::Shards;
using memgraph::coordinator::Status;
using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
// using memgraph::io::ResponseFuture;
using memgraph::io::Time;
using memgraph::io::TimedOut;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::RsmClient;
using memgraph::io::rsm::StorageReadRequest;
using memgraph::io::rsm::StorageReadResponse;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::SchemaProperty;
using memgraph::utils::BasicResult;

using ShardClient =
    RsmClient<SimulatorTransport, StorageWriteRequest, StorageWriteResponse, ScanVerticesRequest, ScanVerticesResponse>;

int main() {}
