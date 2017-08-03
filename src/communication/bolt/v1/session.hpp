#pragma once

#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

#include "database/dbms.hpp"
#include "query/engine.hpp"
#include "transactions/transaction.hpp"

#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/error.hpp"
#include "communication/bolt/v1/states/handshake.hpp"
#include "communication/bolt/v1/states/idle_result.hpp"
#include "communication/bolt/v1/states/init.hpp"

#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/encoder.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"

#include "io/network/stream_buffer.hpp"

namespace communication::bolt {

/**
 * Bolt SessionData
 *
 * This class is responsible for holding references to Dbms and QueryEngine
 * that are passed through the network server and worker to the session.
 *
 * @tparam OutputStream type of output stream (could be a bolt output stream or
 *                      a test output stream)
 */
template <typename OutputStream>
struct SessionData {
  Dbms dbms;
  QueryEngine<OutputStream> query_engine;
};

/**
 * Bolt Session
 *
 * This class is responsible for handling a single client connection.
 *
 * @tparam Socket type of socket (could be a network socket or test socket)
 */
template <typename Socket>
class Session {
 private:
  using OutputStream = ResultStream<Encoder<ChunkedEncoderBuffer<Socket>>>;
  using StreamBuffer = io::network::StreamBuffer;

 public:
  Session(Socket &&socket, SessionData<OutputStream> &data)
      : socket_(std::move(socket)), dbms_(data.dbms), query_engine_(data.query_engine) {
    event_.data.ptr = this;
  }

  ~Session() {
    debug_assert(!db_accessor_,
                 "Transaction should have already be closed in Close");
  }

  /**
   * @return is the session in a valid state
   */
  bool Alive() const { return state_ != State::Close; }

  /**
   * @return the socket id
   */
  int Id() const { return socket_.id(); }

  /**
   * Executes the session after data has been read into the buffer.
   * Goes through the bolt states in order to execute commands from the client.
   */
  void Execute() {
    // while there is data in the buffers
    while (buffer_.size() > 0 || decoder_buffer_.Size() > 0) {
      if (LIKELY(connected_)) {
        DLOG(INFO) << fmt::format("Decoding chunk of size {}", buffer_.size());
        auto chunk_state = decoder_buffer_.GetChunk();
        if (chunk_state == ChunkState::Partial) {
          DLOG(WARNING) << "Chunk isn't complete!";
          return;
        } else if (chunk_state == ChunkState::Invalid) {
          DLOG(WARNING) << "Chunk is invalid!";
          ClientFailureInvalidData();
          return;
        }
        // if chunk_state == ChunkState::Whole then we continue with
        // execution of the select below
      } else if (buffer_.size() < HANDSHAKE_SIZE) {
        DLOG(WARNING) << fmt::format("Received partial handshake of size {}",
                                     buffer_.size());
        return;
      } else {
        DLOG(WARNING) << fmt::format("Decoding handshake of size {}",
                                     buffer_.size());
      }

      switch (state_) {
        case State::Handshake:
          state_ = StateHandshakeRun(*this);
          break;
        case State::Init:
          state_ = StateInitRun(*this);
          break;
        case State::Idle:
        case State::Result:
        case State::WaitForRollback:
          state_ = StateExecutingRun(*this, state_);
          break;
        case State::ErrorIdle:
        case State::ErrorWaitForRollback:
          state_ = StateErrorRun(*this, state_);
          break;
        case State::Close:
          // This state is handled below
          break;
      }

      // State::Close is handled here because we always want to check for
      // it after the above select. If any of the states above return a
      // State::Close then the connection should be terminated immediately.
      if (state_ == State::Close) {
        ClientFailureInvalidData();
        return;
      }

      DLOG(INFO) << fmt::format("Buffer size: {}", buffer_.size());
      DLOG(INFO) << fmt::format("Decoder buffer size: {}",
                                decoder_buffer_.Size());
    }
  }

  /**
   * Allocates data from the internal buffer.
   * Used in the underlying network stack to asynchronously read data
   * from the client.
   * @returns a StreamBuffer to the allocated internal data buffer
   */
  StreamBuffer Allocate() { return buffer_.Allocate(); }

  /**
   * Notifies the internal buffer of written data.
   * Used in the underlying network stack to notify the internal buffer
   * how many bytes of data have been written.
   * @param len how many data was written to the buffer
   */
  void Written(size_t len) { buffer_.Written(len); }

  /**
   * Closes the session (client socket).
   */
  void Close() {
    DLOG(INFO) << "Closing session";
    if (db_accessor_) {
      Abort();
    }
    this->socket_.Close();
  }

  /**
   * Commits associated transaction.
   */
  void Commit() {
    debug_assert(db_accessor_, "Commit called and there is no transaction");
    db_accessor_->commit();
    db_accessor_ = nullptr;
  }

  /**
   * Aborts associated transaction.
   */
  void Abort() {
    debug_assert(db_accessor_, "Abort called and there is no transaction");
    db_accessor_->abort();
    db_accessor_ = nullptr;
  }

  GraphDbAccessor ActiveDb() { return dbms_.active(); }

  // TODO: Rethink if there is a way to hide some members. At the momemnt all of
  // them are public.
  Socket socket_;
  Dbms &dbms_;
  QueryEngine<OutputStream> &query_engine_;

  ChunkedEncoderBuffer<Socket> encoder_buffer_{socket_};
  Encoder<ChunkedEncoderBuffer<Socket>> encoder_{encoder_buffer_};
  OutputStream output_stream_{encoder_};

  Buffer<> buffer_;
  ChunkedDecoderBuffer decoder_buffer_{buffer_};
  Decoder<ChunkedDecoderBuffer> decoder_{decoder_buffer_};

  io::network::Epoll::Event event_;
  bool connected_{false};
  State state_{State::Handshake};
  // GraphDbAccessor of active transaction in the session, can be null if there
  // is no associated transaction.
  std::unique_ptr<GraphDbAccessor> db_accessor_;

 private:
  void ClientFailureInvalidData() {
    // set the state to Close
    state_ = State::Close;
    // don't care about the return status because this is always
    // called when we are about to close the connection to the client
    encoder_buffer_.Clear();
    encoder_.MessageFailure({{"code", "Memgraph.InvalidData"},
                             {"message", "The client has sent invalid data!"}});
    // close the connection
    Close();
  }
};
}
