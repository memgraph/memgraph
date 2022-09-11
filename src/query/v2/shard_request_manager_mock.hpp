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

#include "coordinator/coordinator_client.hpp"

namespace requests {

template <typename TTransport>
class ShardRequestManagerMock {
  using CoordinatorClient = memgraph::coordinator::CoordinatorClient<TTransport>;
  using IoImpl = memgraph::io::Io<TTransport>;

  CoordinatorClient coord_cli_;
  IoImpl io_;

 public:
  ShardRequestManagerMock(CoordinatorClient coord_cli, memgraph::io::Io<TTransport> &&io)
      : coord_cli_(std::move(coord_cli)), io_(std::move(io)) {}

  void StartTransaction() { throw std::runtime_error("StartTransaction not yet implemented!"); }

  void RequestExpandOne() {}
};

}  // namespace requests
