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

#include <boost/asio/thread_pool.hpp>
#include <boost/uuid/uuid.hpp>

#include "io/rsm/raft.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/shard_manager.hpp"

namespace memgraph::storage::v3 {

using memgraph::io::rsm::Raft;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::msgs::ReadRequests;
using memgraph::msgs::ReadResponses;
using memgraph::msgs::WriteRequests;
using memgraph::msgs::WriteResponses;

template <typename IoImpl>
using ShardRaft = Raft<IoImpl, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

template <class IoImpl>
class ShardScheduler {
  std::map<boost::uuids::uuid, ShardRaft<IoImpl>> rsm_map_;
  io::Io<IoImpl> io_;

 public:
  ShardScheduler(io::Io<IoImpl> io) : io_(io) {}
};

}  // namespace memgraph::storage::v3
