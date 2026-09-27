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
#include <mutex>
#include <optional>

namespace memgraph::communication::v2 {

/**
 * Decides which of two threads closes a session whose termination has been requested.
 *
 * A session's socket has one owner at a time. While a read is armed the owner is the thread
 * servicing completions; from the moment a read completes until the next one is armed the owner
 * is whichever thread executes the request and writes the reply. Termination can be asked for
 * from a third thread at any point, including part-way through that handover, and the session
 * must then be closed exactly once: closing it twice tears down a socket someone still holds,
 * closing it never leaves a session that ignores the request for as long as it lives.
 *
 * Asking to arm and asking to claim a termination are therefore one decision each, taken under
 * one lock, so neither can see the other half-done and conclude the other side will do the
 * closing. Whoever loses the race is told to close: an arm that finds a request outstanding
 * refuses, and a claim that finds no read armed defers to the arm that must follow.
 */
class ReadArmState {
 public:
  ReadArmState() = default;

  ReadArmState(const ReadArmState &) = delete;
  ReadArmState &operator=(const ReadArmState &) = delete;
  ReadArmState(ReadArmState &&) = delete;
  ReadArmState &operator=(ReadArmState &&) = delete;
  ~ReadArmState() = default;

  /// Proof that its holder has the arming decision, and with it the socket, until it goes away.
  class [[nodiscard]] ArmTicket {
   public:
    ArmTicket(ArmTicket &&) noexcept = default;
    ArmTicket &operator=(ArmTicket &&) noexcept = default;
    ArmTicket(const ArmTicket &) = delete;
    ArmTicket &operator=(const ArmTicket &) = delete;
    ~ArmTicket() = default;

   private:
    friend class ReadArmState;

    explicit ArmTicket(std::unique_lock<std::mutex> lock) noexcept : lock_{std::move(lock)} {}

    std::unique_lock<std::mutex> lock_;
  };

  /// Called by whichever thread has finished with the socket and wants the next read armed.
  /// Nothing means a termination is outstanding and the caller must close the session instead.
  /// Hold the returned ticket across the arming itself, so no claim can land inside it.
  std::optional<ArmTicket> TryArm() {
    auto lock = std::unique_lock{mutex_};
    if (terminate_requested_.load(std::memory_order_acquire)) {
      return std::nullopt;
    }
    armed_.store(true, std::memory_order_release);
    return ArmTicket{std::move(lock)};
  }

  /// The armed read has completed, so the socket passes to whoever executes the request. Left
  /// off the lock deliberately: it happens once per request, and a claim that observes it either
  /// way reaches a correct answer, since a claim seeing the read still armed closes the session
  /// and one seeing it finished defers to the arm that follows.
  void NoteReadFinished() noexcept { armed_.store(false, std::memory_order_release); }

  /// Callable from any thread. A request is never withdrawn, so an arm that refuses once refuses
  /// for the rest of the session's life.
  void RequestTermination() noexcept { terminate_requested_.store(true, std::memory_order_release); }

  bool TerminationRequested() const noexcept { return terminate_requested_.load(std::memory_order_acquire); }

  /// Whether the caller is the one that must close the session. False means a read is not armed,
  /// so another thread holds the socket and the arm it must perform will do the closing.
  bool ClaimForTermination() {
    auto lock = std::lock_guard{mutex_};
    if (!armed_.load(std::memory_order_acquire)) {
      return false;
    }
    armed_.store(false, std::memory_order_release);
    return true;
  }

 private:
  std::mutex mutex_;
  std::atomic_bool armed_{false};
  std::atomic_bool terminate_requested_{false};
};

}  // namespace memgraph::communication::v2
