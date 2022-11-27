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

///
/// Physical Single Frame Pull Architecture Implementation
///

#include "utils/logging.hpp"

#include "query/v2/physical/mock/context.hpp"
#include "query/v2/physical/mock/frame.hpp"

namespace memgraph::query::v2::physical {

class Cursor {
 public:
  using TFrame = mock::Frame;
  using TExecutionContext = mock::ExecutionContext;
  using TCursorPtr = std::unique_ptr<Cursor>;

  explicit Cursor(TCursorPtr &&input) : input_(std::move(input)) {}
  Cursor() = delete;
  Cursor(const Cursor &) = delete;
  Cursor(Cursor &&) noexcept = delete;
  Cursor &operator=(const Cursor &) = delete;
  Cursor &operator=(Cursor &&) noexcept = delete;
  virtual ~Cursor() {}

  virtual bool Pull(TFrame &, TExecutionContext &) = 0;
  void Reset() {}
  void Shutdown() {}

 protected:
  TCursorPtr input_;
};

class OnceCursor : public Cursor {
 public:
  using Cursor::TCursorPtr;
  using Cursor::TExecutionContext;
  using Cursor::TFrame;

  explicit OnceCursor(TCursorPtr &&input) : Cursor(std::move(input)) {}

  bool Pull(TFrame &, TExecutionContext &) override {
    SPDLOG_INFO("Once Cursor Pull");
    return false;
  }
};

class ScanAllCursor : public Cursor {
 public:
  using Cursor::TCursorPtr;
  using Cursor::TExecutionContext;
  using Cursor::TFrame;

  explicit ScanAllCursor(TCursorPtr &&input) : Cursor(std::move(input)) {}

  bool Pull(TFrame &frame, TExecutionContext &ctx) override {
    SPDLOG_INFO("Scan All Cursor Pull");
    return input_->Pull(frame, ctx);
  }
};

class ProduceCursor : public Cursor {
 public:
  using Cursor::TCursorPtr;
  using Cursor::TExecutionContext;
  using Cursor::TFrame;

  explicit ProduceCursor(TCursorPtr &&input) : Cursor(std::move(input)) {}

  bool Pull(TFrame &frame, TExecutionContext &ctx) override {
    SPDLOG_INFO("Produce Cursor Pull");
    return input_->Pull(frame, ctx);
  }
};

}  // namespace memgraph::query::v2::physical
