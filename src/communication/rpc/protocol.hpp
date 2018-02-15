#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "communication/rpc/buffer.hpp"
#include "communication/rpc/messages.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"

/**
 * @brief Protocol
 *
 * Has classes and functions that implement the server side of our
 * RPC protocol.
 *
 * Handshake layout: MessageSize service_size, service_size characters service
 *
 * Message layout: uint32_t message_id, MessageSize message_size,
 *                 message_size bytes serialized_message
 */
namespace communication::rpc {

using Endpoint = io::network::Endpoint;
using Socket = io::network::Socket;
using StreamBuffer = io::network::StreamBuffer;

// Forward declaration of class System
class System;

/**
 * Distributed Protocol Session
 *
 * This class is responsible for handling a single client connection.
 */
class Session {
 public:
  Session(Socket &&socket, System &system);

  int Id() const { return socket_->fd(); }

  /**
   * Returns the protocol alive state
   */
  bool Alive() const;

  /**
   * Executes the protocol after data has been read into the buffer.
   * Goes through the protocol states in order to execute commands from the
   * client.
   */
  void Execute();

  /**
   * Allocates data from the internal buffer.
   * Used in the underlying network stack to asynchronously read data
   * from the client.
   * @returns a StreamBuffer to the allocated internal data buffer
   */
  StreamBuffer Allocate();

  /**
   * Notifies the internal buffer of written data.
   * Used in the underlying network stack to notify the internal buffer
   * how many bytes of data have been written.
   * @param len how many data was written to the buffer
   */
  void Written(size_t len);

  bool TimedOut() { return false; }

  /**
   * Closes the session (client socket).
   */
  void Close();

  Socket &socket() { return *socket_; }

  void RefreshLastEventTime(
      const std::chrono::time_point<std::chrono::steady_clock>
          &last_event_time) {
    last_event_time_ = last_event_time;
  }

 private:
  std::shared_ptr<Socket> socket_;
  std::chrono::time_point<std::chrono::steady_clock> last_event_time_ =
      std::chrono::steady_clock::now();
  System &system_;

  std::string service_name_;
  bool handshake_done_{false};

  bool alive_{true};
  Buffer buffer_;
};

/**
 * Distributed Protocol Server Send Message
 */
void SendMessage(Socket &socket, uint32_t message_id,
                 std::unique_ptr<Message> &message);

}  // namespace communication::rpc
