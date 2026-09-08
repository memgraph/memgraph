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

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "storage/v2/pipeline_budget.hpp"

namespace memgraph::storage {

// Test-only instrumentation for the commit path (lock-free-read-snapshot and pipelined-commit experiments).
// A test installs a CommitProbe on the storage; the commit path invokes each hook at the
// corresponding phase boundary so the test can block it on a latch and run another
// transaction at a precise instant. In production the storage's probe pointer is null and
// every call site is a single predictable null-check (near-zero cost). Hooks are optional.
struct CommitProbe {
  std::function<void()> after_mint;         // T minted, engine_lock released, before durability
  std::function<void()> during_durability;  // inside the (unlocked) WAL+replication window
  std::function<void()> before_publish;     // about to reacquire engine_lock to publish
  std::function<void()> after_publish;      // visibility store + watermark advanced

  // Pipelined commit. Ticketed execution only.
  std::function<void()> after_ticket;               // once per transaction: after the serializer release (eligible
                                                    // writers) or after gate entry with the serializer retained
  std::function<void()> before_validate;            // gate entered, about to validate unique constraints
  std::function<void()> before_append;              // pipeline: WAL initialized and streams open, before scheduling
  std::function<void()> after_append;               // pipeline: buffered transaction appended
  std::function<void()> after_finalize_wal;         // pipeline: WAL finalized, WAL promise not yet satisfied
  std::function<void()> after_prepare_record;       // legacy: complete commit=false record, WAL promise satisfied
  std::function<void()> after_schedule_ship;        // right after ScheduleEncodeAndShip, before any frame
  std::function<void()> between_frames;             // legacy: after the start frame, before the end frame
  std::function<void()> before_legacy_fallback;     // pipeline: S2 charges released, gate not yet entered
  std::function<void()> before_legacy_materialize;  // legacy materialization on a ticketed execution

  // Test-only injection state.
  BudgetRefuse budget_refuse;
  std::atomic<uint64_t> minted_ticket{0};  // stored by CommitWithTicket right after registration, before after_mint
  std::atomic<bool> abort_throws_once{false};
  std::atomic<bool> finalize_wal_throws_once{false};
  std::atomic<bool> decision_schedule_throws_once{false};
  // Each instrumented fault site appends one line here before consuming its one-shot flag.
  std::string fault_attempt_marker_path;
};

// Safe invoker: no-op if the hook is unset. Call sites use InvokeProbe(probe, &CommitProbe::after_mint).
inline void InvokeProbe(CommitProbe *probe, std::function<void()> CommitProbe::*hook) {
  if (probe != nullptr && (probe->*hook)) (probe->*hook)();
}

// Test-only replication instrumentation, owned by a storage (null in production). Main-side callbacks are read
// through the storage the replication object retains; replica-side callbacks are installed on the replica's
// storage so identity is implicit. Every observation callback is by contract nonthrowing: it records into
// test-owned atomics and performs only bounded synchronization, because OnScopeExit invokes callbacks directly
// and an observer throwing during unwind would terminate an otherwise abortable case.
struct ReplicationTestHooks {
  // Main side: receive the replica name and the durability timestamp.
  std::function<void(std::string const &, uint64_t)> before_wal_result_wait;  // task about to wait on the WAL gate
  std::function<void(std::string const &, uint64_t)> on_task_done;            // a fused task's lambda finished
  // Invoked by a scope guard placed right after the command-owning object in each path; owner_scope is "legacy"
  // or "pipeline".
  std::function<void(std::string_view owner_scope, uint64_t)> on_commands_released;
  std::function<void()> after_async_abort_cleanup;  // ticketed ASYNC arm: stream reset, client MAYBE_BEHIND
  std::function<void(std::string const &)> before_reconcile_quiesce;  // UpdateReplicaState, before QuiesceCommits
  std::function<bool(std::string const &, uint64_t)> throw_before_enqueue_for;  // ScheduleEncodeAndShip
  std::function<bool(std::string const &, uint64_t)> throw_on_open_for;         // TransactionReplication ctor
  // Replica side.
  std::function<bool(uint64_t)> refuse_next_prepare;             // after the prepared accessor is cached: vote no
  std::function<bool(uint64_t)> refuse_next_abort_decision;      // after the abort is applied: answer with failure
  std::function<void(uint64_t, bool)> on_abort_decision_result;  // main side: AbortTwoPcOrTerminate outcome
  std::function<void(uint64_t)> on_prepared;                     // after the prepare response is sent
  std::function<void(uint64_t)> on_abort_applied;                // after the abort decision is applied
};

}  // namespace memgraph::storage
