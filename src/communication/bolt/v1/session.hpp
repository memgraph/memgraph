#pragma once

#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

#include "dbms/dbms.hpp"
#include "query/engine.hpp"

#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/error.hpp"
#include "communication/bolt/v1/states/executor.hpp"
#include "communication/bolt/v1/states/handshake.hpp"
#include "communication/bolt/v1/states/init.hpp"

#include "communication/bolt/v1/encoder/encoder.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"

#include "logging/loggable.hpp"

namespace communication::bolt {

/**
 * Bolt Session
 *
 * This class is responsible for handling a single client connection.
 *
 * @tparam Socket type of socket (could be a network socket or test socket)
 */
template <typename Socket>
class Session : public Loggable {
 public:
  using Decoder = BoltDecoder;
  using OutputStream = ResultStream<Encoder<ChunkedBuffer<Socket>>>;

  Session(Socket &&socket, Dbms &dbms, QueryEngine<OutputStream> &query_engine)
      : Loggable("communication::bolt::Session"),
        socket_(std::move(socket)),
        dbms_(dbms),
        query_engine_(query_engine),
        encoder_buffer_(socket_),
        encoder_(encoder_buffer_),
        output_stream_(encoder_) {
    event_.data.ptr = this;
    // start with a handshake state
    state_ = HANDSHAKE;
  }

  /**
   * @return is the session in a valid state
   */
  bool Alive() const { return state_ != NULLSTATE; }

  /**
   * @return the socket id
   */
  int Id() const { return socket_.id(); }

  /**
   * Reads the data from a client and goes through the bolt states in
   * order to execute command from the client.
   *
   * @param data pointer on bytes received from a client
   * @param len  length of data received from a client
   */
  void Execute(const byte *data, size_t len) {
    // mark the end of the message
    auto end = data + len;

    while (true) {
      auto size = end - data;

      if (LIKELY(connected_)) {
        logger.debug("Decoding chunk of size {}", size);
        if (!decoder_.decode(data, size)) return;
      } else {
        logger.debug("Decoding handshake of size {}", size);
        decoder_.handshake(data, size);
      }

      switch (state_) {
        case HANDSHAKE:
          state_ = StateHandshakeRun<Session<Socket>>(*this);
          break;
        case INIT:
          state_ = StateInitRun<Session<Socket>>(*this);
          break;
        case EXECUTOR:
          state_ = StateExecutorRun<Session<Socket>>(*this);
          break;
        case ERROR:
          state_ = StateErrorRun<Session<Socket>>(*this);
          break;
        case NULLSTATE:
          break;
      }

      decoder_.reset();
    }
  }

  /**
   * Closes the session (client socket).
   */
  void Close() {
    logger.debug("Closing session");
    this->socket_.Close();
  }

  GraphDbAccessor ActiveDb() { return dbms_.active(); }

  Socket socket_;
  Dbms &dbms_;
  QueryEngine<OutputStream> &query_engine_;
  ChunkedBuffer<Socket> encoder_buffer_;
  Encoder<ChunkedBuffer<Socket>> encoder_;
  OutputStream output_stream_;
  Decoder decoder_;
  io::network::Epoll::Event event_;
  bool connected_{false};
  State state_;
};
}
