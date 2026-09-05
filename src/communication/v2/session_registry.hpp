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

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace memgraph::communication::v2 {

// Type-erased handle so the registry does not have to be templated on
// Session<TSession, TSessionContext>. communication::v2::Session derives from this.
struct TerminableSession {
  virtual ~TerminableSession() = default;

  // Ask the session to close its connection. Returns immediately; the close is executed on the
  // session's own strand, and the session then tears itself down.
  virtual void RequestTermination() = 0;
};

// Process-wide uuid -> session lookup used by admin commands that terminate a Bolt connection
// by uuid. Holding only a weak_ptr means the registry never extends a session's lifetime.
class SessionRegistry {
 public:
  static SessionRegistry &Instance();

  SessionRegistry(SessionRegistry const &) = delete;
  SessionRegistry &operator=(SessionRegistry const &) = delete;
  SessionRegistry(SessionRegistry &&) = delete;
  SessionRegistry &operator=(SessionRegistry &&) = delete;

  void Register(std::string uuid, std::weak_ptr<TerminableSession> session);
  void Deregister(std::string_view uuid, TerminableSession const *self);

  // Unlocks before returning the promoted shared_ptr: the caller may drop the last ref, running
  // ~Session -> Deregister, which re-locks mutex_ and would self-deadlock if still held.
  [[nodiscard]] std::shared_ptr<TerminableSession> Find(std::string_view uuid) const;

  // Test-only: the map size is not part of any production code path.
  [[nodiscard]] std::size_t Size() const;

 private:
  SessionRegistry() = default;

  // A mutex, not utils::SpinLock: Register/Deregister allocate/free a hash-map node under the
  // lock, so spinning through a stalled allocator would waste CPU; churn is only per connect.
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::weak_ptr<TerminableSession>> sessions_;
};

}  // namespace memgraph::communication::v2
