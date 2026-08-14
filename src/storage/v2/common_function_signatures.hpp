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

#include <cstdint>
#include <exception>
#include <functional>

namespace memgraph::storage {

using CheckCancelFunction = std::function<bool()>;
constexpr auto neverCancel = []() { return false; };

// Invoked by long full-scan operations (snapshot load, index population, constraint validation, storage teardown) once
// per item processed. Lets a caller that owes liveness to somebody else -- an RPC handler under a peer timeout -- see
// that the work is still advancing. Empty means nobody is watching, which is the common case.
using ProgressCallback = std::function<void()>;

// Thrown when a CheckCancelFunction asks a full-scan schema operation (index population, constraint validation) to
// stop. Caught by whoever registered the schema object so it can be deregistered before the error surfaces.
struct PopulateCancel : std::exception {};

// default for when callback not provided
constexpr auto always_invalidate_plan_cache = []<typename... Args>(Args &&...) { return true; };

}  // namespace memgraph::storage
