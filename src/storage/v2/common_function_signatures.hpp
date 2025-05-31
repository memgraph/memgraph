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

#include <functional>

namespace memgraph::storage {

using CheckCancelFunction = std::function<bool()>;

// Main purpose is the we need to invalidate plans at query level
// the wrapper allows that to happen in storage level via a callback
// which executed the index publish function while the plan cache is locked
using PublishIndexCallback = std::function<void(std::function<void()>)>;

// default for when callback not provided
constexpr auto invoke_input = [](auto &&func) { func(); };

}  // namespace memgraph::storage
