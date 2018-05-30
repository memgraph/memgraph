#pragma once

#include <thread>

#include "glog/logging.h"

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
#include "communication/buffer.hpp"
#include "database/graph_db.hpp"
#include "query/interpreter.hpp"
#include "transactions/transaction.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

/** Encapsulates Dbms and Interpreter that are passed through the network server
 * and worker to the session. */
struct SessionData {
  database::MasterBase &db;
  query::Interpreter interpreter{db};
};

/**
 * Bolt Session Exception
 *
 * Used to indicate that something went wrong during the session execution.
 */
class SessionException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * Bolt Session
 *
 * This class is responsible for handling a single client connection.
 *
 * @tparam TInputStream type of input stream that will be used
 * @tparam TOutputStream type of output stream that will be used
 */
template <typename TInputStream, typename TOutputStream>
class Session {
 public:
  using ResultStreamT =
      ResultStream<Encoder<ChunkedEncoderBuffer<TOutputStream>>>;

  Session(SessionData &data, TInputStream &input_stream,
          TOutputStream &output_stream)
      : db_(data.db),
        interpreter_(data.interpreter),
        input_stream_(input_stream),
        output_stream_(output_stream) {}

  ~Session() {
    if (db_accessor_) {
      Abort();
    }
  }

  /**
   * Executes the session after data has been read into the buffer.
   * Goes through the bolt states in order to execute commands from the client.
   */
  void Execute() {
    if (UNLIKELY(!handshake_done_)) {
      if (input_stream_.size() < HANDSHAKE_SIZE) {
        DLOG(WARNING) << fmt::format("Received partial handshake of size {}",
                                     input_stream_.size());
        return;
      }
      DLOG(WARNING) << fmt::format("Decoding handshake of size {}",
                                   input_stream_.size());
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

      DLOG(INFO) << fmt::format("Input stream size: {}", input_stream_.size());
      DLOG(INFO) << fmt::format("Decoder buffer size: {}",
                                decoder_buffer_.Size());
    }
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
  database::MasterBase &db_;
  query::Interpreter &interpreter_;
  TInputStream &input_stream_;
  TOutputStream &output_stream_;

  ChunkedEncoderBuffer<TOutputStream> encoder_buffer_{output_stream_};
  Encoder<ChunkedEncoderBuffer<TOutputStream>> encoder_{encoder_buffer_};
  ResultStreamT result_stream_{encoder_};

  ChunkedDecoderBuffer<TInputStream> decoder_buffer_{input_stream_};
  Decoder<ChunkedDecoderBuffer<TInputStream>> decoder_{decoder_buffer_};

  bool handshake_done_{false};
  State state_{State::Handshake};
  // GraphDbAccessor of active transaction in the session, can be null if
  // there is no associated transaction.
  std::unique_ptr<database::GraphDbAccessor> db_accessor_;

 private:
  void ClientFailureInvalidData() {
    // Set the state to Close.
    state_ = State::Close;
    // We don't care about the return status because this is called when we
    // are about to close the connection to the client.
    encoder_buffer_.Clear();
    encoder_.MessageFailure({{"code", "Memgraph.ExecutionException"},
                             {"message",
                              "Something went wrong while executing the query! "
                              "Check the server logs for more details."}});
    // Throw an exception to indicate that something went wrong with execution
    // of the session to trigger session cleanup and socket close.
    throw SessionException("Something went wrong during session execution!");
  }
};
}  // namespace communication::bolt
