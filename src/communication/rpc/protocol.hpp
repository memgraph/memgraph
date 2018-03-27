#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "communication/rpc/messages.hpp"
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
namespace communication::rpc {

// Forward declaration of class Server
class Server;

/**
 * This class is thrown when the Session wants to indicate that a fatal error
 * occured during execution.
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
  Session(Server &server, communication::InputStream &input_stream,
          communication::OutputStream &output_stream);

  /**
   * Executes the protocol after data has been read into the stream.
   * Goes through the protocol states in order to execute commands from the
   * client.
   */
  void Execute();

 private:
  Server &server_;
  communication::InputStream &input_stream_;
  communication::OutputStream &output_stream_;
};

}  // namespace communication::rpc
