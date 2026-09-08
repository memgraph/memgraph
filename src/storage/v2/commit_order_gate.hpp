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

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>

namespace memgraph::storage {

/// Orders main-side committers by commit timestamp. Issue() is noexcept (the ticket owns its list node) and is
/// called under engine_lock_ right after the mint, so tickets are issued in mint order. Enter() blocks until every
/// earlier ticket has retired. Retire() wakes the next. WaitIdle() blocks until no ticket is pending; it is only
/// called with commit_mutex_ held so no new ticket can be issued meanwhile. All members are mutable so a const
/// InMemoryStorage caller (recovery-step selection) can quiesce.
class CommitOrderGate {
 public:
  struct Node {
    uint64_t ticket{0};
    Node *next{nullptr};
  };

  /// Links `node` into the pending list. Tickets arrive in mint order, so the node is appended.
  void Issue(Node &node) noexcept;

  /// Blocks until `ticket` is the oldest pending ticket.
  void Enter(uint64_t ticket);

  /// Unlinks `node` and wakes every waiter so the next oldest ticket can enter.
  void Retire(Node &node) noexcept;

  /// Blocks until no ticket is pending.
  void WaitIdle();

  auto Pending() const -> size_t;

 private:
  auto HeadTicket() const -> uint64_t;

  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  mutable Node *head_{nullptr};
  mutable Node *tail_{nullptr};
  mutable size_t pending_{0};
};

/// Owning ticket, noncopyable and nonmovable. States: registered -> entered -> published | aborted, plus an
/// independent `irreversible` mark set at the first committing file write (or at the start of publication on a
/// WAL-disabled path). MarkPublished/MarkAborted record the outcome; Retire() is a separate, explicit call made only
/// after the whole durability/outcome continuation has finished, by CommitWithTicket only. The destructor terminates
/// (release builds too) whenever Retire() has not run, published or aborted included; it never aborts implicitly and
/// never retires silently. RecordState brackets a WAL record: BeginRecord before the start frame, EndRecord after the
/// end frame.
class CommitTicket {
 public:
  enum class RecordState : uint8_t { not_started, incomplete, complete };

  /// Registers with the gate.
  CommitTicket(CommitOrderGate &gate, uint64_t ticket) noexcept;
  ~CommitTicket();

  CommitTicket(CommitTicket const &) = delete;
  CommitTicket &operator=(CommitTicket const &) = delete;
  CommitTicket(CommitTicket &&) = delete;
  CommitTicket &operator=(CommitTicket &&) = delete;

  /// Idempotent.
  void Enter();

  void MarkIrreversible() noexcept { irreversible_ = true; }

  void BeginRecord() noexcept { record_state_ = RecordState::incomplete; }

  void EndRecord() noexcept { record_state_ = RecordState::complete; }

  void MarkPublished() noexcept { outcome_ = Outcome::published; }

  void MarkAborted() noexcept { outcome_ = Outcome::aborted; }

  /// Requires a terminal mark; exactly once; wakes the next ticket.
  void Retire() noexcept;

  auto ticket() const noexcept -> uint64_t { return node_.ticket; }

  auto entered() const noexcept -> bool { return entered_; }

  auto terminal() const noexcept -> bool { return outcome_ != Outcome::none; }

  auto published() const noexcept -> bool { return outcome_ == Outcome::published; }

  auto irreversible() const noexcept -> bool { return irreversible_; }

  auto record_state() const noexcept -> RecordState { return record_state_; }

  auto incomplete_record() const noexcept -> bool { return record_state_ == RecordState::incomplete; }

  auto retired() const noexcept -> bool { return retired_; }

 private:
  enum class Outcome : uint8_t { none, published, aborted };

  CommitOrderGate &gate_;
  CommitOrderGate::Node node_;
  bool entered_{false};
  bool irreversible_{false};
  bool retired_{false};
  RecordState record_state_{RecordState::not_started};
  Outcome outcome_{Outcome::none};
};

}  // namespace memgraph::storage
