#pragma once

#include <chrono>

#include "communication/bolt/v1/decoder/buffer.hpp"
#include "communication/reactor/reactor_local.hpp"
#include "io/network/epoll.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"

/**
 * @brief Protocol
 *
 * Has classes and functions that implement server and client sides of our
 * distributed protocol.
 *
 * The protocol consists of two stages.
 * The first stage is a handshake stage when the client sends to the server
 * reactor and channel names which it wants to communicate with.
 * The second stage is sending messages.
 *
 * HANDSHAKE
 *
 * Client sends:
 *    len_reactor_name(SizeT) len_channel_name(SizeT) reactor_name channel_name
 * Server responds:
 *    0x80
 *
 * MESSAGES
 *
 * Client sends:
 *    len_message(SizeT) cereal_encoded_binary_message
 *
 * Currently the server is implemented to handle more than one message after
 * the initial handshake, but the client can only send one message.
 * TODO: no reason to do any sort of handshake at all.
 */
namespace communication::reactor {

class Message;

using Endpoint = io::network::NetworkEndpoint;
using Socket = io::network::Socket;
using StreamBuffer = io::network::StreamBuffer;

// this buffer should be larger than the largest serialized message
using Buffer = bolt::Buffer<262144>;
using SizeT = uint16_t;

/**
 * Distributed Protocol Data
 */
struct SessionData {
  System system;
};

/**
 * Distributed Protocol Session
 *
 * This class is responsible for handling a single client connection.
 *
 * @tparam Socket type of socket (could be a network socket or test socket)
 */
class Session {
 private:
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

  io::network::Epoll::Event event_;
  Socket socket_;
  System &system_;

  std::chrono::time_point<std::chrono::steady_clock> last_event_time_;

 private:
  SizeT GetLength(int offset = 0);
  std::string GetStringAndShift(SizeT len);
  // Should be renamed to SendHandshake.
  bool SendSuccess(bool success);

  bool alive_{true};
  bool handshake_done_{false};

  std::string reactor_{""};
  std::string channel_{""};

  Buffer buffer_;
};

/**
 * Distributed Protocol Send Message
 *
 * This function sends a message to the specified server.
 * If message is a nullptr then it only checks whether the remote reactor
 * and channel exist, else it returns the complete message send success.
 */
void SendMessage(std::string address, uint16_t port, std::string reactor,
                 std::string channel, std::unique_ptr<Message> message);
}
