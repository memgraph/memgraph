// Copyright 2025 Memgraph Ltd.
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

#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"
#include "communication/bolt/v1/encoder/encoder.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/error.hpp"
#include "communication/bolt/v1/states/executing.hpp"
#include "communication/bolt/v1/states/handlers.hpp"
#include "communication/bolt/v1/states/handshake.hpp"
#include "communication/bolt/v1/states/init.hpp"
#include "communication/metrics.hpp"
#include "utils/exceptions.hpp"
#include "utils/timestamp.hpp"
#include "utils/uuid.hpp"

namespace memgraph::communication::bolt {
/**
 * Bolt Session Exception
 *
 * Used to indicate that something went wrong during the session execution.
 */
class SessionException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(SessionException)
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

  /**
   * @brief Construct a new Session object
   *
   * @param input_stream stream to read from
   * @param output_stream stream to write to
   * @param impl a default high-level implementation to use (has to be defined)
   */
  Session(TInputStream *input_stream, TOutputStream *output_stream)
      : input_stream_(*input_stream),
        output_stream_(*output_stream),
        session_uuid_(utils::GenerateUUID()),
        login_timestamp_(utils::Timestamp::Now().ToString(kTimestampFormat)) {}

  ~Session() = default;

  Session(const Session &) = delete;
  Session &operator=(const Session &) = delete;
  Session(Session &&) noexcept = delete;
  Session &operator=(Session &&) noexcept = delete;

  /**
   * Executes the session after data has been read into the buffer.
   * Goes through the bolt states in order to execute commands from the client.
   */
  template <typename TImpl>
  bool Execute_(TImpl &impl) {
    if (state_ == State::Handshake) [[unlikely]] {
      // Resize the input buffer to ensure that a whole chunk can fit into it.
      // This can be done only once because the buffer holds its size.
      input_stream_.Resize(kChunkWholeSize);

      // Receive the handshake.
      if (input_stream_.size() < kHandshakeSize) {
        spdlog::trace("Received partial handshake of size {}", input_stream_.size());
        return false;  // no more data
      }
      state_ = StateHandshakeRun(impl);
      if (state_ == State::Close) [[unlikely]] {
        ClientFailureInvalidData();
        return false;  // no more data
      }
      // Update the decoder's Bolt version (v5 has changed the undelying structure)
      decoder_.UpdateVersion(version_.major);
      encoder_.UpdateVersion(version_.major);
      // Fallthrough as there could be more data to process
    }

    // Re-entering while in the Parsed state. Query has been parsed, execution has yielded to check the priority, we are
    // here now (with the correct priority), so continue with Prepare.
    // Phase 1: parse and deduce priority
    // Phase 2: actually prepare interpreter for the query
    if (state_ == State::Parsed) {
      state_ = HandlePrepare(impl);
      if (state_ == State::Close) [[unlikely]] {
        ClientFailureInvalidData();
      }
      // We are here, so the query will have the correct priority; just fall down to execute any other requests
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
          state_ = StateInitRun(impl);
          break;
        case State::Idle:
        case State::Result:
          at_least_one_run_ = true;
          state_ = StateExecutingRun(impl, state_);
          break;
        case State::Error:
          state_ = StateErrorRun(impl, state_);
          break;
        default:
          // State::Handshake is handled above
          // State::Parsed is handled below
          // State::Close is handled below
          break;
      }

      if (state_ == State::Parsed) {
        // First time seeing this query;
        // Parsing the query has the highest priority as we don't know what's incoming
        // Once the query has been parsed, break, check task priority and reschedule if needed.
        // After Parsed, we do a Prepare (state::Result) and the Pull/Discard (state::Result)
        // Try to not break from Prepare till the end of the execution as this will lead to worse performance.
        // Last pull will set the state to State::Idle
        return true;  // more data to process
      }

      if (state_ == State::Close) [[unlikely]] {
        // State::Close is handled here because we always want to check for
        // it after the above select. If any of the states above return a
        // State::Close then the connection should be terminated immediately.
        ClientFailureInvalidData();
      }
    }
    return false;  // no more data
  }

  void HandleError() {
    if (!at_least_one_run_) {
      spdlog::info("Sudden connection loss. Make sure the client supports Memgraph.");
    }
  }

  // TODO: Rethink if there is a way to hide some members. At the momement all of them are public.
  TInputStream &input_stream_;
  TOutputStream &output_stream_;

  ChunkedEncoderBuffer<TOutputStream> encoder_buffer_{output_stream_};
  TEncoder encoder_{encoder_buffer_};

  ChunkedDecoderBuffer<TInputStream> decoder_buffer_{input_stream_};
  Decoder<ChunkedDecoderBuffer<TInputStream>> decoder_{decoder_buffer_};

  State state_{State::Handshake};
  bool at_least_one_run_{false};

  struct Version {
    uint8_t major;
    uint8_t minor;
  };

  Version version_;
  std::vector<std::string> client_supported_bolt_versions_;
  std::optional<BoltMetrics::Metrics> metrics_;

  std::string UUID() const { return session_uuid_; }
  std::string GetLoginTimestamp() const { return login_timestamp_; }

 protected:
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

 private:
  const std::string kTimestampFormat = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:06d}";
  const std::string session_uuid_;  //!< unique identifier of the session (auto generated)
  const std::string login_timestamp_;
};

}  // namespace memgraph::communication::bolt
