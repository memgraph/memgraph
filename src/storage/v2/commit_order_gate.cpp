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

#include "storage/v2/commit_order_gate.hpp"

#include "utils/logging.hpp"

namespace memgraph::storage {

void CommitOrderGate::Issue(Node &node) noexcept {
  auto guard = std::lock_guard{mutex_};
  node.next = nullptr;
  if (tail_ == nullptr) {
    head_ = &node;
  } else {
    // Tickets are issued under engine_lock_ in mint order, so the newest always goes last.
    DMG_ASSERT(tail_->ticket < node.ticket, "Commit tickets must be issued in mint order");
    tail_->next = &node;
  }
  tail_ = &node;
  ++pending_;
}

void CommitOrderGate::Enter(uint64_t ticket) {
  auto guard = std::unique_lock{mutex_};
  cv_.wait(guard, [this, ticket] { return head_ != nullptr && head_->ticket == ticket; });
}

void CommitOrderGate::Retire(Node &node) noexcept {
  {
    auto guard = std::lock_guard{mutex_};
    Node *prev = nullptr;
    auto *current = head_;
    while (current != nullptr && current != &node) {
      prev = current;
      current = current->next;
    }
    MG_ASSERT(current != nullptr, "Retiring a commit ticket that is not pending");
    if (prev == nullptr) {
      head_ = node.next;
    } else {
      prev->next = node.next;
    }
    if (tail_ == &node) tail_ = prev;
    node.next = nullptr;
    --pending_;
  }
  cv_.notify_all();
}

void CommitOrderGate::WaitIdle() {
  auto guard = std::unique_lock{mutex_};
  cv_.wait(guard, [this] { return head_ == nullptr; });
}

auto CommitOrderGate::Pending() const -> size_t {
  auto guard = std::lock_guard{mutex_};
  return pending_;
}

CommitTicket::CommitTicket(CommitOrderGate &gate, uint64_t ticket) noexcept : gate_{gate} {
  node_.ticket = ticket;
  gate_.Issue(node_);
}

CommitTicket::~CommitTicket() {
  // Retirement is the only way out: an unretired ticket would leave its node linked into the gate after this
  // object is gone, and retiring here would let a successor validate against an unresolved transaction.
  MG_ASSERT(retired_, "Commit ticket {} destroyed without being retired", node_.ticket);
}

void CommitTicket::Enter() {
  if (entered_) return;
  gate_.Enter(node_.ticket);
  entered_ = true;
}

void CommitTicket::Retire() noexcept {
  MG_ASSERT(terminal(), "Commit ticket {} retired without a terminal outcome", node_.ticket);
  MG_ASSERT(!retired_, "Commit ticket {} retired twice", node_.ticket);
  retired_ = true;
  gate_.Retire(node_);
}

}  // namespace memgraph::storage
