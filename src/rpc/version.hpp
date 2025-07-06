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

// To each RPC main uuid was added
constexpr auto v3 = Version{2024'02'02'0'2'14};

// The order of metadata and data deltas has been changed
// It is now possible that both can be sent in one commit
// this is due to auto index creation
constexpr auto v4 = Version{2024'07'02'0'2'18};

// Moved coordinator section in TypeId.
constexpr auto v5 = Version{2025'05'29'0'3'3};

constexpr auto current_version = v5;

}  // namespace memgraph::rpc
