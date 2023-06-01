// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
// TODO: Check if comment above is ok
#pragma once

#include "auth/auth.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"
#include "utils/synchronized.hpp"

#if MG_ENTERPRISE
#include "audit/log.hpp"
#endif

namespace memgraph::dbms {

/// Encapsulates Dbms and Interpreter that are passed through the network server
/// and worker to the session.
struct SessionData {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.
#if MG_ENTERPRISE

  SessionData(memgraph::storage::Storage *db, memgraph::query::InterpreterContext *interpreter_context,
              memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
              memgraph::audit::Log *audit_log)
      : db(db), interpreter_context(interpreter_context), auth(auth), audit_log(audit_log) {}
  memgraph::storage::Storage *db;
  memgraph::query::InterpreterContext *interpreter_context;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth;
  memgraph::audit::Log *audit_log;

#else

  SessionData(memgraph::storage::Storage *db, memgraph::query::InterpreterContext *interpreter_context,
              memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth)
      : db(db), interpreter_context(interpreter_context), auth(auth) {}
  memgraph::storage::Storage *db;
  memgraph::query::InterpreterContext *interpreter_context;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth;

#endif
  // NOTE: run_id should be const but that complicates code a lot.
  std::optional<std::string> run_id;
};

}  // namespace memgraph::dbms
