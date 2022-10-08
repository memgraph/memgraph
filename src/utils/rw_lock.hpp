// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

namespace memgraph::utils {

class RWLock {
 public:
  enum class Priority { READ, WRITE };

  explicit RWLock(Priority priority) {
    // TODO(gitbuda): Implement RWLock::RWLock(Priority)
  }

  RWLock(const RWLock &) = delete;
  RWLock &operator=(const RWLock &) = delete;
  RWLock(RWLock &&) = delete;
  RWLock &operator=(RWLock &&) = delete;
  ~RWLock() = default;

  void lock() {
    // TODO(gitbuda): Implement RWLock::lock
  }

  bool try_lock() {
    // TODO(gitbuda): Implement RWLock::try_lock
    return false;
  }

  void unlock() {
    // TODO(gitbuda): Implement RWLock::unlock
  }

  void lock_shared() {
    // TODO(gitbuda): Implement RWLock::lock_shared
  }

  bool try_lock_shared() {
    // TODO(gitbuda): Implement RWLock::try_lock_shared
    return false;
  }

  void unlock_shared() {
    // TODO(gitbuda): Implement RWLock::try_lock_shared
  }
};

class WritePrioritizedRWLock final : public RWLock {
 public:
  WritePrioritizedRWLock() : RWLock{Priority::WRITE} {};
};

}  // namespace memgraph::utils
