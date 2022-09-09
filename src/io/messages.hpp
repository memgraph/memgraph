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

#include <variant>

#include <coordinator/coordinator.hpp>
#include <io/rsm/raft.hpp>
#include <io/rsm/shard_rsm.hpp>
#include "utils/concepts.hpp"

namespace memgraph::io::messages {

using memgraph::coordinator::CoordinatorReadRequests;
using memgraph::coordinator::CoordinatorWriteRequests;
using memgraph::coordinator::CoordinatorWriteResponses;
using memgraph::coordinator::HeartbeatResponse;

using memgraph::io::rsm::StorageReadRequest;
using memgraph::io::rsm::StorageReadResponse;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;

using memgraph::io::rsm::AppendRequest;
using memgraph::io::rsm::AppendResponse;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::VoteRequest;
using memgraph::io::rsm::VoteResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;

// TODO(tyler) make these real types instead of unique placeholders
struct QEM {};

template <typename M>
concept CoordinatorMessageC =
    memgraph::utils::SameAsAnyOf<M, ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>,
                                 AppendResponse, WriteRequest<CoordinatorWriteRequests>, VoteRequest, VoteResponse>;

using CoordinatorMessages =
    std::variant<ReadRequest<CoordinatorReadRequests>, AppendRequest<CoordinatorWriteRequests>, AppendResponse,
                 WriteRequest<CoordinatorWriteRequests>, VoteRequest, VoteResponse>;

template <typename M>
concept ShardMessageC =
    memgraph::utils::SameAsAnyOf<M, ReadRequest<StorageReadRequest>, AppendRequest<StorageWriteRequest>, AppendResponse,
                                 WriteRequest<StorageWriteRequest>, VoteRequest, VoteResponse>;

using ShardMessages = std::variant<ReadRequest<StorageReadRequest>, AppendRequest<StorageWriteRequest>, AppendResponse,
                                   WriteRequest<StorageWriteRequest>, VoteRequest, VoteResponse>;

template <typename M>
concept QueryEngineMessageC = memgraph::utils::SameAsAnyOf<M, CoordinatorWriteResponses>;

using QueryEngineMessages = std::variant<QEM>;

template <typename M>
concept ShardManagerMessageC = memgraph::utils::SameAsAnyOf<M, CoordinatorWriteResponses>;

using ShardManagerMessages = std::variant<CoordinatorWriteResponses>;

}  // namespace memgraph::io::messages
