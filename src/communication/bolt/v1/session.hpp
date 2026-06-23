// Copyright 2026 Memgraph Ltd.
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

#include <concepts>
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
#include "utils/session_context.hpp"
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
    requires requires(TImpl &impl) {
      { impl.GetLogContext() } -> std::same_as<memgraph::logging::SessionLogContext *>;
    }
  ExecuteResult Execute_(TImpl &impl) {
    // nullptr is the explicit no-op opt-out (test fakes, pre-auth).
    memgraph::logging::ScopedSessionLog log_guard(impl.GetLogContext());
    if (state_ == State::Handshake) [[unlikely]] {
      // Resize the input buffer to ensure that a whole chunk can fit into it.
      // This can be done only once because the buffer holds its size.
      input_stream_.Resize(kChunkWholeSize);

      // Receive the handshake.
      if (input_stream_.size() < kHandshakeSize) {
        spdlog::trace("Received partial handshake of size {}", input_stream_.size());
        return ExecuteResult::NoMoreData;
      }
      state_ = StateHandshakeRun(impl);
      if (state_ == State::Close) [[unlikely]] {
        ClientFailureInvalidData();
        return ExecuteResult::NoMoreData;
      }
      // Update the decoder's Bolt version (v5 has changed the undelying structure)
      decoder_.UpdateVersion(version_.major);
      encoder_.UpdateVersion(version_.major);
      // Fallthrough as there could be more data to process
    }

    // Batch B5: re-entry from a yielded PULL (COROUTINE_CURSORS ON only).
    // HandlePullDiscard returned State::Yielding; the DoWork resumable task
    // returned true (same-worker pinned reschedule) and called Execute_() again.
    //
    // The PULL Bolt chunk was already consumed from the decoder buffer before
    // the yield (StateExecutingRun read the header; HandlePullDiscardV4 read
    // the n/qid parameters).  We CANNOT call StateExecutingRun again — it would
    // attempt ReadMessageHeader on an empty buffer and return State::Close.
    //
    // Instead, bypass StateExecutingRun and call HandlePullDiscard<true> directly
    // with the n/qid we saved in pending_yield_n_ / pending_yield_qid_ when the
    // yield was first signalled.  This mirrors how the State::Parsed path calls
    // HandlePrepare() outside the GetChunk() loop.
    //
    // Flag-OFF: State::Yielding is never set → this block is dead code.
    if (state_ == State::Yielding) [[unlikely]] {
      state_ = details::HandlePullDiscard<true>(impl, pending_yield_n_, pending_yield_qid_);

      if (state_ == State::Yielding) [[unlikely]] {
        // Cursor yielded again; signal the resumable task to reschedule.
        // pending_yield_n_ / qid_ are refreshed by HandlePullDiscard itself.
        return ExecuteResult::Yielding;
      }
      if (state_ == State::Close) [[unlikely]] {
        ClientFailureInvalidData();
      }
      // Fell through to Idle (PULL exhausted) or another state — fall down
      // to the GetChunk() loop to handle any remaining client messages.
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
          // State::Yielding is handled above
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
        return ExecuteResult::MoreData;
      }

      // Batch B5: Yielding returned by HandlePullDiscard (flag-ON only).
      // Do NOT reset state_ here — Execute_ was called to process a Bolt
      // chunk and StateExecutingRun already set state_ = State::Yielding.
      // The DoWork resumable loop will call Execute_() again; the Yielding
      // guard at the top of Execute_ will reset state_ → State::Result and
      // return Yielding so the loop can reschedule.
      if (state_ == State::Yielding) [[unlikely]] {
        return ExecuteResult::Yielding;
      }

      if (state_ == State::Close) [[unlikely]] {
        // State::Close is handled here because we always want to check for
        // it after the above select. If any of the states above return a
        // State::Close then the connection should be terminated immediately.
        ClientFailureInvalidData();
      }
    }
    return ExecuteResult::NoMoreData;
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

  // Batch B5 — COROUTINE_CURSORS yield re-entry context.
  // When HandlePullDiscard signals State::Yielding it first stores the n/qid
  // parameters here so that Execute_() can re-enter HandlePullDiscard directly
  // (bypassing StateExecutingRun) without re-reading from the decoder buffer.
  // Under COROUTINE_CURSORS OFF these fields are never written, so the flag-OFF
  // path is byte-identical to the pre-B5 code.
  std::optional<int> pending_yield_n_{};
  std::optional<int> pending_yield_qid_{};

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
