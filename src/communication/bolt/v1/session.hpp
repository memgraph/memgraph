// Copyright 2023 Memgraph Ltd.
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

#include <optional>
#include <thread>

#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"
#include "communication/bolt/v1/encoder/encoder.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/error.hpp"
#include "communication/bolt/v1/states/executing.hpp"
#include "communication/bolt/v1/states/handshake.hpp"
#include "communication/bolt/v1/states/init.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

namespace memgraph::communication::bolt {

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
  using TEncoder = Encoder<ChunkedEncoderBuffer<TOutputStream>>;

  Session(TInputStream *input_stream, TOutputStream *output_stream)
      : input_stream_(*input_stream), output_stream_(*output_stream) {}

  virtual ~Session() {}

  /**
   * Process the given `query` with `params`.
   * @return A pair which contains list of headers and qid which is set only
   * if an explicit transaction was started.
   */
  virtual std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, Value> &params,
      const std::map<std::string, memgraph::communication::bolt::Value> &metadata) = 0;

  /**
   * Put results of the processed query in the `encoder`.
   *
   * @param n If set, defines amount of rows to be pulled from the result,
   * otherwise all the rows are pulled.
   * @param q If set, defines from which query to pull the results,
   * otherwise the last query is used.
   */
  virtual std::map<std::string, Value> Pull(TEncoder *encoder, std::optional<int> n, std::optional<int> qid) = 0;

  /**
   * Discard results of the processed query.
   *
   * @param n If set, defines amount of rows to be discarded from the result,
   * otherwise all the rows are discarded.
   * @param q If set, defines from which query to discard the results,
   * otherwise the last query is used.
   */
  virtual std::map<std::string, Value> Discard(std::optional<int> n, std::optional<int> qid) = 0;

  virtual void BeginTransaction(const std::map<std::string, memgraph::communication::bolt::Value> &) = 0;
  virtual void CommitTransaction() = 0;
  virtual void RollbackTransaction() = 0;

  /** Aborts currently running query. */
  virtual void Abort() = 0;

  /** Return `true` if the user was successfully authenticated. */
  virtual bool Authenticate(const std::string &username, const std::string &password) = 0;

  /** Return the name of the server that should be used for the Bolt INIT
   * message. */
  virtual std::optional<std::string> GetServerNameForInit() = 0;

  /**
   * Executes the session after data has been read into the buffer.
   * Goes through the bolt states in order to execute commands from the client.
   */
  void Execute() {
    if (UNLIKELY(!handshake_done_)) {
      // Resize the input buffer to ensure that a whole chunk can fit into it.
      // This can be done only once because the buffer holds its size.
      input_stream_.Resize(kChunkWholeSize);

      // Receive the handshake.
      if (input_stream_.size() < kHandshakeSize) {
        spdlog::trace("Received partial handshake of size {}", input_stream_.size());
        return;
      }
      state_ = StateHandshakeRun(*this);
      if (UNLIKELY(state_ == State::Close)) {
        ClientFailureInvalidData();
        return;
      }
      handshake_done_ = true;
      // Update the decoder's Bolt version (v5 has changed the underlying structure)
      decoder_.UpdateVersion(version_.major);
      encoder_.UpdateVersion(version_.major);
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
  TEncoder encoder_{encoder_buffer_};

  ChunkedDecoderBuffer<TInputStream> decoder_buffer_{input_stream_};
  Decoder<ChunkedDecoderBuffer<TInputStream>> decoder_{decoder_buffer_};

  bool handshake_done_{false};
  State state_{State::Handshake};

  struct Version {
    uint8_t major;
    uint8_t minor;
  };

  Version version_;

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

}  // namespace memgraph::communication::bolt
