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

#include "utils/fips.hpp"

#include <atomic>
#include <utility>

namespace memgraph::utils {

namespace {

/// Function-local static rather than a namespace-scope global: the status is
/// read from several translation units, and a global with a dynamic
/// initialiser could be read before its own initialisation ran.
///
/// `atomic<shared_ptr>` (as used for the SSL contexts in
/// `communication::ServerContext`) rather than a mutex: writes happen once at
/// startup, reads happen on every password hash and TLS context build.
auto Instance() -> std::atomic<std::shared_ptr<FipsStatus const>> & {
  static std::atomic<std::shared_ptr<FipsStatus const>> instance{std::make_shared<FipsStatus const>()};
  return instance;
}

}  // namespace

void SetFipsStatus(FipsStatus status) {
  Instance().store(std::make_shared<FipsStatus const>(std::move(status)), std::memory_order_release);
}

auto GetFipsStatus() -> std::shared_ptr<FipsStatus const> { return Instance().load(std::memory_order_acquire); }

bool FipsEnabled() { return GetFipsStatus()->enabled; }

}  // namespace memgraph::utils
