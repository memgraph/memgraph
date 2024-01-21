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

#include "auth/auth.hpp"
#include "slk/streams.hpp"

namespace memgraph::dbms {
namespace auth_replication {

// #ifdef MG_ENTERPRISE // TODO Is it???
void UpdateAuthDataHandler(auth::SynchedAuth &auth, slk::Reader *req_reader, slk::Builder *res_builder);
void DropAuthDataHandler(auth::SynchedAuth &auth, slk::Reader *req_reader, slk::Builder *res_builder);
// #endif

}  // namespace auth_replication

}  // namespace memgraph::dbms
