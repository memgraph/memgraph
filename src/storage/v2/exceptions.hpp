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

#include "utils/exceptions.hpp"

namespace memgraph::storage {

// The operation is not valid for the arguments it was given or the state the
// engine is in: an index that does not exist, a vector whose dimension does not
// match the one the index was created with, a write that the active mode
// forbids. Repeating it unchanged fails the same way, which is what lets a
// caller distinguish these from a failure worth trying again.
//
// Errors the engine detects about itself do not belong here.
class InvalidOperationException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(InvalidOperationException)
};

class VectorSearchException : public InvalidOperationException {
  using InvalidOperationException::InvalidOperationException;
  SPECIALIZE_GET_EXCEPTION_NAME(VectorSearchException)
};

class TextSearchException : public InvalidOperationException {
  using InvalidOperationException::InvalidOperationException;
  SPECIALIZE_GET_EXCEPTION_NAME(TextSearchException)
};

// A write gave a property a value of a type a type constraint forbids. Carries the constraint it
// broke already rendered, because the names behind the ids are the storage's to resolve.
class TypeConstraintViolationException : public InvalidOperationException {
  using InvalidOperationException::InvalidOperationException;
  SPECIALIZE_GET_EXCEPTION_NAME(TypeConstraintViolationException)
};

class WriteVertexOperationInEdgeImportModeException : public InvalidOperationException {
 public:
  WriteVertexOperationInEdgeImportModeException()
      : InvalidOperationException(
            "Write operations on nodes are forbidden while the edge import mode is active. To disable the edge import "
            "mode, run the EDGE IMPORT MODE INACTIVE; query.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(WriteVertexOperationInEdgeImportModeException)
};

#ifdef MG_ENTERPRISE
class WriteInHistoricalAccessorException : public InvalidOperationException {
 public:
  WriteInHistoricalAccessorException()
      : InvalidOperationException("Write operations are forbidden on a read-only historical snapshot.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(WriteInHistoricalAccessorException)
};

// Text and vector indexes hold only current values, so they cannot answer a historical read.
inline constexpr auto kTextSearchOnHistorical = "Text search is not supported on a historical snapshot.";
inline constexpr auto kVectorSearchOnHistorical = "Vector search is not supported on a historical snapshot.";
#endif

}  // namespace memgraph::storage
