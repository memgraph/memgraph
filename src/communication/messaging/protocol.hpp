#pragma once

#include <chrono>
#include <cstdint>

#include "communication/bolt/v1/decoder/buffer.hpp"
#include "communication/messaging/local.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"

/**
 * @brief Protocol
 *
 * Has classes and functions that implement server and client sides of our
 * messaging protocol.
 *
 * Message layout: SizeT channel_size, channel_size characters channel,
 *                 SizeT message_size, message_size bytes serialized_message
 */
namespace communication::messaging {

class Message;

using Endpoint = io::network::Endpoint;
using Socket = io::network::Socket;
using StreamBuffer = io::network::StreamBuffer;

// This buffer should be larger than the largest serialized message.
const int64_t kMaxMessageSize = 262144;
using Buffer = bolt::Buffer<kMaxMessageSize>;
using SizeT = uint16_t;

/**
 * Distributed Protocol Data
 */
struct SessionData {
  LocalSystem system;
};

/**
 * Distributed Protocol Session
 *
 * This class is responsible for handling a single client connection.
 */
class Session {
 public:
  Session(Socket &&socket, SessionData &data);

  int Id() const { return socket_.fd(); }

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

  Socket socket_;
  LocalSystem &system_;

  std::chrono::time_point<std::chrono::steady_clock> last_event_time_;

 private:
  SizeT GetLength(int offset = 0);
  std::string GetStringAndShift(SizeT len);

  bool alive_{true};
  Buffer buffer_;
};

/**
 * Distributed Protocol Send Message
 */
void SendMessage(const Endpoint &endpoint, const std::string &channel,
                 std::unique_ptr<Message> message);
}  // namespace communication::messaging
