// Copyright 2026 Memgraph Ltd.
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
#include "utils/logging.hpp"
#include "utils/priorities.hpp"
#include "utils/priority_thread_pool.hpp"
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
using SynchedAuth = utils::Synchronized<auth::Auth, utils::WritePrioritizedRWLock>;
}  // namespace memgraph::auth

namespace memgraph::glue {
struct Context {
  communication::v2::ServerEndpoint endpoint;
  query::InterpreterContext *ic;
  auth::SynchedAuth *auth;
#if MG_ENTERPRISE
  audit::Log *audit_log;
#endif
  utils::PriorityThreadPool *worker_pool_;

  using TaskID = utils::PriorityThreadPool::TaskID;

  auto AddTask(auto &&task, utils::Priority priority, bool productive = true) {
    MG_ASSERT(worker_pool_, "Trying to add task to a non-existent worker pool");
    return worker_pool_->ScheduledAddTask(std::forward<decltype(task)>(task), priority, productive);
  }

  void ParkAdmission(auto &&task, TaskID id, std::chrono::steady_clock::time_point deadline,
                     utils::WaitResource resource, uint64_t observed_epoch) {
    MG_ASSERT(worker_pool_, "Trying to park admission on a non-existent worker pool");
    worker_pool_->ParkAdmission(std::forward<decltype(task)>(task), id, deadline, resource, observed_epoch);
  }

  // 0 when no pool is configured.
  uint64_t WakeEpoch(utils::WaitResource resource) const noexcept {
    return worker_pool_ ? worker_pool_->WakeEpoch(resource) : 0;
  }

  // False when no pool is configured.
  bool IsDrainingAdmissions() const noexcept { return worker_pool_ && worker_pool_->IsDrainingAdmissions(); }

  // 1 when no pool is configured (park on first contention).
  uint32_t RescheduleCap() const noexcept { return worker_pool_ ? worker_pool_->AdmissionRescheduleCap() : 1u; }
};
}  // namespace memgraph::glue
