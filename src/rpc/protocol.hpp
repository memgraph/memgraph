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

#include "communication/session.hpp"

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
  SPECIALIZE_GET_EXCEPTION_NAME(SessionException)
};

/**
 * This class represents a communication link from the server's side. It is something between a fair-loss link
 * and perfect link (see Introduction to Reliable and Secure Distributed Programming book). It's not a perfect link
 * because we don't guarantee that a message sent once will be always eventually delivered to the process.
 * Fair-loss property:
 *   If a client sends infinitely often a message m to a correct (non-Byzantine) process q,
 *   this class will deliver the message to the server_ an infinite numer of times.
 * No creation:
 *   If we deliver the message m to the server_ it means the message m was previously sent by some process p. We also
 *   rely on TCP for that.
 * No duplication:
 *   For this property, we rely on TCP protocol. It says that a message m won't be delivered to the server_ more than
 * once. This class is responsible for handling a single client connection.
 */
class RpcMessageDeliverer {
 public:
  RpcMessageDeliverer(Server *server, io::network::Endpoint const & /*endpoint*/,
                      communication::InputStream *input_stream, communication::OutputStream *output_stream);

  void Execute() const;

 private:
  Server *server_;
  communication::InputStream *input_stream_;
  communication::OutputStream *output_stream_;
};

}  // namespace memgraph::rpc
