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

#include "query/interpret/frame.hpp"
#include "query/frame_change.hpp"

namespace memgraph::query {

auto FrameWriter::ResetTrackingValue(const Symbol &symbol) -> void {
  if (frame_change_collector_) {
    frame_change_collector_->ResetTrackingValue(symbol);
  }
}

auto FrameWriter::Write(const Symbol &symbol, TypedValue value) -> TypedValue & {
  auto &inserted_value = frame_.elems_[symbol.position()] = std::move(value);
  ResetTrackingValue(symbol);
  return inserted_value;
}

auto FrameWriter::WriteAt(const Symbol &symbol, TypedValue value) -> TypedValue & {
  auto &inserted_value = frame_.elems_.at(symbol.position()) = std::move(value);
  ResetTrackingValue(symbol);
  return inserted_value;
}

void FrameWriter::ClearList(const Symbol &symbol) { frame_.elems_[symbol.position()].ValueList().clear(); }
}  // namespace memgraph::query
