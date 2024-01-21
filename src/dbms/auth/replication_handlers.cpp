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

void UpdateAuthDataHandler(auth::SynchedAuth &auth, slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::UpdateAuthDataReq req;
  memgraph::slk::Load(&req, req_reader);

  if (req.user) auth->SaveUser(*req.user);
  if (req.role) auth->SaveRole(*req.role);

  replication::UpdateAuthDataRes res(true);
  memgraph::slk::Save(res, res_builder);
}

void DropAuthDataHandler(auth::SynchedAuth &auth, slk::Reader *req_reader, slk::Builder *res_builder) {
  replication::DropAuthDataReq req;
  memgraph::slk::Load(&req, req_reader);

  switch (req.type) {
    case replication::DropAuthDataReq::DataType::USER:
      auth->RemoveUser(req.name);
      break;
    case replication::DropAuthDataReq::DataType::ROLE:
      auth->RemoveRole(req.name);
      break;
  }

  replication::DropAuthDataRes res(true);
  memgraph::slk::Save(res, res_builder);
}

}  // namespace auth_replication

}  // namespace memgraph::dbms
