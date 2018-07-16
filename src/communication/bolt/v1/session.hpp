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
#include "utils/exceptions.hpp"

namespace communication::bolt {

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

  Session(TInputStream &input_stream, TOutputStream &output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  virtual ~Session() {}

  /**
   * Put results in the `result_stream` by processing the given `query` with
   * `params`.
   */
  virtual void PullAll(const std::string &query,
                       const std::map<std::string, DecodedValue> &params,
                       ResultStreamT *result_stream) = 0;

  /** Aborts currently running query. */
  virtual void Abort() = 0;

  void PullAll(const std::string &query,
               const std::map<std::string, DecodedValue> &params) {
    return PullAll(query, params, &result_stream_);
  }

  /**
   * Executes the session after data has been read into the buffer.
   * Goes through the bolt states in order to execute commands from the client.
   */
  void Execute() {
    if (UNLIKELY(!handshake_done_)) {
      // Resize the input buffer to ensure that a whole chunk can fit into it.
      // This can be done only once because the buffer holds its size.
      input_stream_.Resize(WHOLE_CHUNK_SIZE);

      // Receive the handshake.
      if (input_stream_.size() < HANDSHAKE_SIZE) {
        DLOG(WARNING) << fmt::format("Received partial handshake of size {}",
                                     input_stream_.size());
        return;
      }
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
          state_ = StateExecutingRun(*this, state_);
          break;
        case State::Error:
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
    }
  }

  // TODO: Rethink if there is a way to hide some members. At the momement all
  // of them are public.
  TInputStream &input_stream_;
  TOutputStream &output_stream_;

  ChunkedEncoderBuffer<TOutputStream> encoder_buffer_{output_stream_};
  Encoder<ChunkedEncoderBuffer<TOutputStream>> encoder_{encoder_buffer_};
  ResultStreamT result_stream_{encoder_};

  ChunkedDecoderBuffer<TInputStream> decoder_buffer_{input_stream_};
  Decoder<ChunkedDecoderBuffer<TInputStream>> decoder_{decoder_buffer_};

  bool handshake_done_{false};
  State state_{State::Handshake};

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
