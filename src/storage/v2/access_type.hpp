// Copyright 2025 Memgraph Ltd.
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

#include <cstdint>

namespace memgraph::storage {

enum StorageAccessType : uint8_t {
  NO_ACCESS = 0,  // Modifies nothing in the storage
  UNIQUE = 1,     // An operation that requires mutral exclusive access to the storage
  WRITE = 2,      // Writes to the data of storage
  READ = 3,       // Either reads the data of storage, or a metadata operation that doesn't require unique access
  READ_ONLY = 4,  // Ensures writers have gone
};

}  // namespace memgraph::storage
