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

#include <chrono>
#include <cstdint>
#include <memory>

#include "communication/session.hpp"
#include "rpc/messages.hpp"

/**
 * @brief Protocol
 *
 * Has classes and functions that implement the server side of our
 * RPC protocol.
 *
 * Message layout: MessageSize message_size,
 *                 message_size bytes serialized_message
 */
namespace memgraph::rpc {

// Forward declaration of class Server
class Server;

/**
 * This class is thrown when the Session wants to indicate that a fatal error
 * occurred during execution.
 */
class SessionException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/**
 * Distributed Protocol Session
 *
 * This class is responsible for handling a single client connection.
 */
class Session {
 public:
  Session(Server *server, const io::network::Endpoint &endpoint, communication::InputStream *input_stream,
          communication::OutputStream *output_stream);

  /**
   * Executes the protocol after data has been read into the stream.
   * Goes through the protocol states in order to execute commands from the
   * client.
   */
  void Execute();

 private:
  Server *server_;
  io::network::Endpoint endpoint_;
  communication::InputStream *input_stream_;
  communication::OutputStream *output_stream_;
};

}  // namespace memgraph::rpc
