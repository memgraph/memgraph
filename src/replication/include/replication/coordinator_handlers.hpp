// Copyright 2024 Memgraph Ltd.
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

#include "replication/coordinator_server.hpp"
#include "replication/state.hpp"
#include "slk/serialization.hpp"

// TODO: (andi) How to organize this code better, what to do with namespaces...? Do we need to bind this to storage
// Check replication_handlers.hpp for example, for forward declarations etc.
// I would avoid binding to storage since this is higher level logic than in replication

#ifdef MG_ENTERPRISE

namespace memgraph::replication {

class CoordinatorHandlers {
 public:
  static void Register(const ReplicationState &repl_state, CoordinatorServer &server);

 private:
  static void FailoverHandler(const ReplicationState &repl_state, slk::Reader *req_reader, slk::Builder *res_builder);
};

}  // namespace memgraph::replication

#endif
