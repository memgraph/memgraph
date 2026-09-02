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

// Conditions the request caused and the sender can correct: an index that does
// not exist, a vector whose dimension does not match the one the index was
// created with, a write that the active mode forbids. Repeating the same
// request fails the same way, so a session translating one of these must not
// present it as worth retrying.
//
// Errors the engine detects about itself do not belong here.
class ClientFixableException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(ClientFixableException)
};

class VectorSearchException : public ClientFixableException {
  using ClientFixableException::ClientFixableException;
  SPECIALIZE_GET_EXCEPTION_NAME(VectorSearchException)
};

class TextSearchException : public ClientFixableException {
  using ClientFixableException::ClientFixableException;
  SPECIALIZE_GET_EXCEPTION_NAME(TextSearchException)
};

class WriteVertexOperationInEdgeImportModeException : public ClientFixableException {
 public:
  WriteVertexOperationInEdgeImportModeException()
      : ClientFixableException(
            "Write operations on nodes are forbidden while the edge import mode is active. To disable the edge import "
            "mode, run the EDGE IMPORT MODE INACTIVE; query.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(WriteVertexOperationInEdgeImportModeException)
};

}  // namespace memgraph::storage
