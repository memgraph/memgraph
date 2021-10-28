// Copyright 2021 Memgraph Ltd.
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

namespace utils {

// This is the default Linux page size found on all architectures. The proper
// way to check for this constant is to call `sysconf(_SC_PAGESIZE)`, but we
// can't use that as a `constexpr`.
const uint64_t kLinuxPageSize = 4096;

}  // namespace utils
