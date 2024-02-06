// Copyright 2024 Memgraph Ltd.
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

namespace memgraph::rpc {

using Version = uint64_t;

// versioning of RPC was/will be introduced in 2.13
// We start the versioning with a strange number, to radically reduce the
// probability of accidental match/conformance with pre 2.13 versions
constexpr auto v1 = Version{2023'10'30'0'2'13};

// TypeId has been changed, they were not stable
// Added stable numbering for replication types to be in
// 2000-2999 range. We shouldn't need to version bump again
// for any TypeIds that get added.
constexpr auto v2 = Version{2023'12'07'0'2'14};

constexpr auto current_version = v2;

}  // namespace memgraph::rpc
