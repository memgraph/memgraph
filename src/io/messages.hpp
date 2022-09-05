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

#pragma once

#include <variant>

namespace memgraph::io::messages {

struct Heartbeat {};

// TODO(tyler) make these real types instead of unique placeholders
struct QEM {};
struct CM {};
struct SM {};
struct SMM {};
struct MMM {};

using QueryEngineMessages = std::variant<QEM>;
using CoordinatorMessages = std::variant<CM>;
using ShardMessages = std::variant<SM>;
using ShardManagerMessages = std::variant<SMM>;
using MachineManagerMessages = std::variant<MMM>;

using UberMessage =
    std::variant<CoordinatorMessages, ShardMessages, ShardManagerMessages, MachineManagerMessages, QueryEngineMessages>;

}  // namespace memgraph::io::messages
