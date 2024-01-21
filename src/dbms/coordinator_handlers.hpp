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

#include "auth/auth.hpp"
#include "slk/serialization.hpp"

namespace memgraph::dbms {

class DbmsHandler;

class CoordinatorHandlers {
 public:
  static void Register(DbmsHandler &dbms_handler, auth::SynchedAuth &auth);

 private:
  static void PromoteReplicaToMainHandler(DbmsHandler &dbms_handler, slk::Reader *req_reader,
                                          slk::Builder *res_builder);
  static void DemoteMainToReplicaHandler(DbmsHandler &dbms_handler, auth::SynchedAuth &auth, slk::Reader *req_reader,
                                         slk::Builder *res_builder);
};

}  // namespace memgraph::dbms

#endif
