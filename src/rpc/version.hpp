// Copyright 2023 Memgraph Ltd.
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

constexpr auto current_version = v1;

}  // namespace memgraph::rpc
