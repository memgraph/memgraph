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

#include "communication/v2/session_registry.hpp"

#include <utility>

namespace memgraph::communication::v2 {

SessionRegistry &SessionRegistry::Instance() {
  static SessionRegistry instance;
  return instance;
}

void SessionRegistry::Register(std::string uuid, std::weak_ptr<TerminableSession> session) {
  std::lock_guard lock(mutex_);
  sessions_.insert_or_assign(std::move(uuid), std::move(session));
}

void SessionRegistry::Deregister(std::string_view uuid, TerminableSession const *self) {
  std::lock_guard lock(mutex_);
  auto it = sessions_.find(std::string(uuid));
  if (it == sessions_.end()) return;
  // Erase iff the entry is stale or still owned by the caller; a uuid collision must not let one
  // session's destructor erase another session's live entry.
  auto owner = it->second.lock();
  if (owner == nullptr || owner.get() == self) {
    sessions_.erase(it);
  }
}

std::shared_ptr<TerminableSession> SessionRegistry::Find(std::string_view uuid) const {
  std::shared_ptr<TerminableSession> result;
  {
    std::lock_guard lock(mutex_);
    auto it = sessions_.find(std::string(uuid));
    if (it != sessions_.end()) {
      result = it->second.lock();
    }
  }
  return result;
}

std::size_t SessionRegistry::Size() const {
  std::lock_guard lock(mutex_);
  return sessions_.size();
}

}  // namespace memgraph::communication::v2
