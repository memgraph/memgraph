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

#include <unordered_set>

#include <gflags/gflags.h>

#include "dbms/database.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/context.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/stripped.hpp"
#include "query/interpret/frame.hpp"
#include "query/metadata.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/read_write_type_checker.hpp"
#include "query/stream.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "utils/event_counter.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"
#include "utils/settings.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/thread_pool.hpp"
#include "utils/timer.hpp"
#include "utils/tsc.hpp"

namespace memgraph::dbms {
class NewSessionHandler;
}  // namespace memgraph::dbms
namespace memgraph::metrics {
extern const Event FailedQuery;
}  // namespace memgraph::metrics

namespace memgraph::query {

inline constexpr size_t kExecutionMemoryBlockSize = 1UL * 1024UL * 1024UL;
inline constexpr size_t kExecutionPoolMaxBlockSize = 1024UL;  // 2 ^ 10

class Interpreter;
class AuthQueryHandler;

/**
 * Holds data shared between multiple `Interpreter` instances (which might be
 * running concurrently).
 *
 */
/// TODO: andi decouple in a separate file why here?

struct InterpreterContext {
  InterpreterContext(InterpreterConfig interpreter_config, memgraph::dbms::NewSessionHandler *db_handler,
                     AuthQueryHandler *ah = nullptr, AuthChecker *ac = nullptr);

  memgraph::dbms::NewSessionHandler *db_handler;

  // Internal
  const InterpreterConfig config;
  std::atomic<bool> is_shutting_down{false};  // TODO: Do we even need this, since there is a global one also
  utils::SkipList<QueryCacheEntry> ast_cache;

  // GLOBAL
  AuthQueryHandler *auth;
  AuthChecker *auth_checker;

  // Used to check active transactions
  // TODO: Have a way to read the current database
  utils::Synchronized<std::unordered_set<Interpreter *>, utils::SpinLock> interpreters;
};

}  // namespace memgraph::query
