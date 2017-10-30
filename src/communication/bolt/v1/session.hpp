#pragma once

#include "glog/logging.h"

#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"

#include "query/interpreter.hpp"
#include "transactions/transaction.hpp"

#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/encoder.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/error.hpp"
#include "communication/bolt/v1/states/executing.hpp"
#include "communication/bolt/v1/states/handshake.hpp"
#include "communication/bolt/v1/states/init.hpp"

DECLARE_int32(session_inactivity_timeout);

namespace communication::bolt {

/**
 * Bolt SessionData
 *
 * This class is responsible for holding references to Dbms and Interpreter
 * that are passed through the network server and worker to the session.
 */
struct SessionData {
  GraphDb db;
  query::Interpreter interpreter;
};

/**
 * Bolt Session
 *
 * This class is responsible for handling a single client connection.
 *
 * @tparam TSocket type of socket (could be a network socket or test socket)
 */
template <typename TSocket>
class Session {
 public:
  // Wrapper around socket that checks if session has timed out on write
  // failures, used in encoder buffer.
  class TimeoutSocket {
   public:
    explicit TimeoutSocket(Session &session) : session_(session) {}

    bool Write(const uint8_t *data, size_t len) {
      return session_.socket_.Write(data, len,
                                    [this] { return !session_.TimedOut(); });
    }

   private:
    Session &session_;
  };

  using ResultStreamT =
      ResultStream<Encoder<ChunkedEncoderBuffer<TimeoutSocket>>>;
  using StreamBuffer = io::network::StreamBuffer;

  Session(TSocket &&socket, SessionData &data)
      : socket_(std::move(socket)),
        db_(data.db),
        interpreter_(data.interpreter) {}

  ~Session() {
    DCHECK(!db_accessor_)
        << "Transaction should have already be closed in Close";
  }

  /**
   * @return the socket id
   */
  int Id() const { return socket_.fd(); }

  /**
   * Executes the session after data has been read into the buffer.
   * Goes through the bolt states in order to execute commands from the client.
   */
  void Execute() {
    if (UNLIKELY(!handshake_done_)) {
      if (buffer_.size() < HANDSHAKE_SIZE) {
        DLOG(WARNING) << fmt::format("Received partial handshake of size {}",
                                     buffer_.size());
        return;
      }
      DLOG(WARNING) << fmt::format("Decoding handshake of size {}",
                                   buffer_.size());
      state_ = StateHandshakeRun(*this);
      if (UNLIKELY(state_ == State::Close)) {
        ClientFailureInvalidData();
        return;
      }
      handshake_done_ = true;
    }

    ChunkState chunk_state;
    while ((chunk_state = decoder_buffer_.GetChunk()) != ChunkState::Partial) {
      if (chunk_state == ChunkState::Whole) {
        // The chunk is whole, we need to read one more chunk
        // (the 0x00 0x00 end marker).
        continue;
      }

      switch (state_) {
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
        default:
          // State::Handshake is handled above
          // State::Close is handled below
          break;
      }

      // State::Close is handled here because we always want to check for
      // it after the above select. If any of the states above return a
      // State::Close then the connection should be terminated immediately.
      if (UNLIKELY(state_ == State::Close)) {
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
   * Returns true if session has timed out. Session times out if there was no
   * activity in FLAGS_sessions_inactivity_timeout seconds or if there is a
   * active transaction with shoul_abort flag set to true.
   */
  bool TimedOut() const {
    return db_accessor_
               ? db_accessor_->should_abort()
               : last_event_time_ + std::chrono::seconds(
                                        FLAGS_session_inactivity_timeout) <
                     std::chrono::steady_clock::now();
  }

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
    DCHECK(db_accessor_) << "Commit called and there is no transaction";
    db_accessor_->Commit();
    db_accessor_ = nullptr;
  }

  /**
   * Aborts associated transaction.
   */
  void Abort() {
    DCHECK(db_accessor_) << "Abort called and there is no transaction";
    db_accessor_->Abort();
    db_accessor_ = nullptr;
  }

  // TODO: Rethink if there is a way to hide some members. At the momement all
  // of them are public.
  TSocket socket_;
  GraphDb &db_;
  query::Interpreter &interpreter_;

  TimeoutSocket timeout_socket_{*this};
  ChunkedEncoderBuffer<TimeoutSocket> encoder_buffer_{timeout_socket_};
  Encoder<ChunkedEncoderBuffer<TimeoutSocket>> encoder_{encoder_buffer_};
  ResultStreamT output_stream_{encoder_};

  Buffer<> buffer_;
  ChunkedDecoderBuffer decoder_buffer_{buffer_};
  Decoder<ChunkedDecoderBuffer> decoder_{decoder_buffer_};

  bool handshake_done_{false};
  State state_{State::Handshake};
  // GraphDbAccessor of active transaction in the session, can be null if
  // there is no associated transaction.
  std::unique_ptr<GraphDbAccessor> db_accessor_;
  // Time of the last event.
  std::chrono::time_point<std::chrono::steady_clock> last_event_time_ =
      std::chrono::steady_clock::now();

 private:
  void ClientFailureInvalidData() {
    // Set the state to Close.
    state_ = State::Close;
    // We don't care about the return status because this is called when we
    // are about to close the connection to the client.
    encoder_buffer_.Clear();
    encoder_.MessageFailure({{"code", "Memgraph.InvalidData"},
                             {"message", "The client has sent invalid data!"}});
    // Close the connection.
    Close();
  }
};
}  // namespace communication::bolt
