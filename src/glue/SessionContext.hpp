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

#include "communication/v2/server.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::query {
struct InterpreterContext;
}

#if MG_ENTERPRISE
namespace memgraph::audit {
class Log;
}
#endif

namespace memgraph::utils {
class WritePrioritizedRWLock;
}
namespace memgraph::auth {
class Auth;
using SynchedAuth = memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock>;
}  // namespace memgraph::auth

namespace memgraph::glue {
struct Context {
  memgraph::communication::v2::ServerEndpoint endpoint;
  memgraph::query::InterpreterContext *ic;
  memgraph::auth::SynchedAuth *auth;
#if MG_ENTERPRISE
  memgraph::audit::Log *audit_log;
#endif
};
}  // namespace memgraph::glue
