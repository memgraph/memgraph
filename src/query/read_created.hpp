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

#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::query {

/// Reads a record at `view`. A record this command created is not there at View::OLD until the command advances, so
/// that read falls back to View::NEW. `seen`, when given, receives the view the read used, for later reads of it.
template <class TRead>
auto ReadSeeingCreated(storage::View view, TRead &&read, storage::View *seen = nullptr) {
  auto result = read(view);
  if (view == storage::View::OLD && result == std::unexpected{storage::Error::NONEXISTENT_OBJECT}) {
    view = storage::View::NEW;
    result = read(view);
  }
  if (seen) *seen = view;
  return result;
}

}  // namespace memgraph::query
