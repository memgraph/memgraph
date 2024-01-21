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

#include "dbms/auth/replication_handlers.hpp"
#include "auth/auth.hpp"
#include "replication/messages.hpp"

namespace memgraph::dbms {

namespace auth_replication {

void UpdateAuthDataHandler(DbmsHandler &dbms_handler, auth::SynchedAuth &auth, slk::Reader *req_reader,
                           slk::Builder *res_builder) {
  replication::UpdateAuthDataReq req;
  memgraph::slk::Load(&req, req_reader);

  using memgraph::replication::UpdateAuthDataRes;
  UpdateAuthDataRes res(false);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != dbms_handler.LastCommitedTS()) {
    spdlog::debug("UpdateAuthDataHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  dbms_handler.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // Update
    if (req.user) auth->SaveUser(*req.user);
    if (req.role) auth->SaveRole(*req.role);
    // Success
    dbms_handler.SetLastCommitedTS(req.new_group_timestamp);
    res = UpdateAuthDataRes(true);
    spdlog::debug("UpdateAuthDataHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
  } catch (const auth::AuthException & /* not used */) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

void DropAuthDataHandler(DbmsHandler &dbms_handler, auth::SynchedAuth &auth, slk::Reader *req_reader,
                         slk::Builder *res_builder) {
  replication::DropAuthDataReq req;
  memgraph::slk::Load(&req, req_reader);

  using memgraph::replication::DropAuthDataRes;
  DropAuthDataRes res(false);

  // Note: No need to check epoch, recovery mechanism is done by a full uptodate snapshot
  //       of the set of databases. Hence no history exists to maintain regarding epoch change.
  //       If MAIN has changed we need to check this new group_timestamp is consistent with
  //       what we have so far.

  if (req.expected_group_timestamp != dbms_handler.LastCommitedTS()) {
    spdlog::debug("DropAuthDataHandler: bad expected timestamp {},{}", req.expected_group_timestamp,
                  dbms_handler.LastCommitedTS());
    memgraph::slk::Save(res, res_builder);
    return;
  }

  try {
    // Remove
    switch (req.type) {
      case replication::DropAuthDataReq::DataType::USER:
        auth->RemoveUser(req.name);
        break;
      case replication::DropAuthDataReq::DataType::ROLE:
        auth->RemoveRole(req.name);
        break;
    }
    // Success
    dbms_handler.SetLastCommitedTS(req.new_group_timestamp);
    res = DropAuthDataRes(true);
    spdlog::debug("DropAuthDataHandler: SUCCESS updated LCTS to {}", req.new_group_timestamp);
  } catch (const auth::AuthException & /* not used */) {
    // Failure
  }

  memgraph::slk::Save(res, res_builder);
}

}  // namespace auth_replication

}  // namespace memgraph::dbms
