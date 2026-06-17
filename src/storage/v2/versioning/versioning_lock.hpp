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

#include <mutex>

namespace memgraph::storage {

// Process-wide guard serializing every versioning RocksDB open (the .registry and the per-version
// delta stores). RocksDB keeps an exclusive directory lock for the lifetime of each open DB and
// rejects a second open of the same path within the same process ("lock hold by current process").
// VersionRegistry and VersionDeltaStore each hold this lock for their entire lifetime, so two opens
// of the same store can never overlap across threads (concurrent queries, a background Lab poll
// racing a write, etc.).
//
// It is *recursive* because a single thread routinely holds the registry open while also opening a
// version's delta store (the replay and REVERT paths), which would self-deadlock on a plain mutex.
// The cost is that versioning operations are mutually exclusive process-wide, which is acceptable
// under the current single-writer overlay model.
std::recursive_mutex &VersioningStoreMutex();

}  // namespace memgraph::storage
