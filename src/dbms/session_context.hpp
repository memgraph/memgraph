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

#pragma once

#include "auth/auth.hpp"
#include "auth_handler.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"
#include "utils/synchronized.hpp"

#if MG_ENTERPRISE
#include "audit/log.hpp"
#endif
namespace memgraph::dbms {

/**
 * @brief Structure encapsulating storage and interpreter context.
 *
 * @note Each session contains a copy.
 */
struct SessionContext {
  // Explicit constructor here to ensure that pointers to all objects are
  // supplied.

  SessionContext(std::shared_ptr<memgraph::storage::Storage> db,
                 std::shared_ptr<memgraph::query::InterpreterContext> interpreter_context, std::string run,
                 std::shared_ptr<AuthContextHandler::AuthContext> auth_context
#ifdef MG_ENTERPRISE
                 ,
                 memgraph::audit::Log *audit_log
#endif
                 )
      : db(db),
        interpreter_context(interpreter_context),
        run_id(run),
        auth_context(auth_context),
        auth(&auth_context->auth)
#ifdef MG_ENTERPRISE
        ,
        audit_log(audit_log)
#endif
  {
  }

  std::shared_ptr<memgraph::storage::Storage> db;
  std::shared_ptr<memgraph::query::InterpreterContext> interpreter_context;
  const std::string run_id;

  std::shared_ptr<AuthContextHandler::AuthContext> auth_context;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *const auth;

#ifdef MG_ENTERPRISE
  memgraph::audit::Log *const audit_log;
#endif
};

}  // namespace memgraph::dbms
