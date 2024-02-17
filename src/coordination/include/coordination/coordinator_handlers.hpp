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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_server.hpp"
#include "replication_handler/replication_handler.hpp"
#include "slk/streams.hpp"

namespace memgraph::dbms {

class DbmsHandler;

class CoordinatorHandlers {
 public:
  static void Register(memgraph::coordination::CoordinatorServer &server,
                       replication::ReplicationHandler &replication_handler);

 private:
  static void PromoteReplicaToMainHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                          slk::Builder *res_builder);
  static void DemoteMainToReplicaHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                         slk::Builder *res_builder);
  static void SwapMainUUIDHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                  slk::Builder *res_builder);

  static void UnregisterReplicaHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                       slk::Builder *res_builder);
  static void EnableWritingOnMainHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                         slk::Builder *res_builder);

  static void GetInstanceUUIDHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                     slk::Builder *res_builder);
};

}  // namespace memgraph::dbms

#endif
