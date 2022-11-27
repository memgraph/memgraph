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

#include "query/v2/physical/mock/frame.hpp"

namespace memgraph::query::v2::physical {

/// Base class for iteration cursors of @c LogicalOperator classes.
///
/// Each @c LogicalOperator must produce a concrete @c Cursor, which provides
/// the iteration mechanism.
class Cursor {
 public:
  using TFrame = mock::Frame;
  using TExecutionContext =

      /// Run an iteration of a @c LogicalOperator.
      ///
      /// Since operators may be chained, the iteration may pull results from
      /// multiple operators.
      ///
      /// @param Frame May be read from or written to while performing the
      ///     iteration.
      /// @param ExecutionContext Used to get the position of symbols in frame and
      ///     other information.
      ///
      /// @throws QueryRuntimeException if something went wrong with execution
      virtual bool Pull(TFrame &, ExecutionContext &) = 0;

  /// Resets the Cursor to its initial state.
  void Reset() {}

  /// Perform cleanup which may throw an exception
  void Shutdown() {}

  virtual ~Cursor() {}
};
