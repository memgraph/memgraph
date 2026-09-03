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

// PendingCommitOutcome lives in state.hpp (so the v2 session driver can name it).

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
  bool Execute_(TImpl &impl) {
    // nullptr is the explicit no-op opt-out (test fakes, pre-auth).
    memgraph::logging::ScopedSessionLog log_guard(impl.GetLogContext());
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

      if (state_ == State::PendingCommit) {
        // PULL/DISCARD's Commit() step would block on commit_mutex_; the pool driver
        // (PostFinishPendingCommit) parks this worker under CommitLock and retries on wake.
        // Stop the dechunk loop so a pipelined message can't run before the commit finishes.
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

  // True while a PULL/DISCARD commit is parked waiting for commit_mutex_.
  bool HasPendingCommit() const { return pending_commit_; }

  // Whether the input buffer still has unprocessed bytes; used by PostFinishPendingCommit to
  // decide DoWork vs DoRead after the commit completes.
  bool HasBufferedData() const { return input_stream_.size() > 0; }

  // Record the PULL arguments so FinishPendingCommit_ can retry without re-decoding.
  // Called from HandlePullDiscard on a CommitWouldBlockException catch.
  void StashPendingCommit(std::optional<int> n, std::optional<int> qid) {
    pending_commit_ = true;
    pending_commit_n_ = n;
    pending_commit_qid_ = qid;
  }

  // Pool-side retry of a Commit() that threw CommitWouldBlockException.
  // Re-runs the exhausted HandlePullDiscard<PULL> path: the cursor is already drained so
  // Pull() streams 0 rows and only retries Commit().  On another TryLock miss, the specific
  // CommitWouldBlockException catch in HandlePullDiscard re-stashes and returns PendingCommit
  // — the pool driver re-parks.  On success, HandlePullDiscard sends the SUCCESS response and
  // sets state_ = Idle.  Any other exception becomes ClientError (the bolt error was already
  // sent by HandleFailure inside HandlePullDiscard).
  //
  // FLAG (re-Pull safety): commit_mutex_ is try-locked as the FIRST action in Interpreter::Commit()
  // (interpreter.cpp:11518), before any state mutation.  The cursor has already returned its last
  // result row and set maybe_res = COMMIT before the first CommitWouldBlockException; a second
  // Pull() on an exhausted PullPlan/PullPlanVector cursor returns immediately (0 rows, returns
  // the done signal) so Commit() is the only meaningful work.  The plan_execution_time in the
  // re-generated summary accumulates a near-zero second elapsed — negligible measurement noise.
  template <typename TImpl>
    requires requires(TImpl &impl) {
      { impl.GetLogContext() } -> std::same_as<memgraph::logging::SessionLogContext *>;
    }
  PendingCommitOutcome FinishPendingCommit_(TImpl &impl) {
    MG_ASSERT(pending_commit_, "FinishPendingCommit_ called without a stashed pending commit");
    memgraph::logging::ScopedSessionLog log_guard(impl.GetLogContext());

    const auto n = pending_commit_n_;
    const auto qid = pending_commit_qid_;

    // Re-invoke the Pull path.  HandlePullDiscard's specific CommitWouldBlockException catch
    // re-stashes n/qid and returns PendingCommit on another TryLock miss; any other exception
    // triggers HandleFailure (bolt Error → ClientError here).  state_ is written by either the
    // success return (Idle) or by HandleFailure (Error/Close) inside HandlePullDiscard.
    state_ = details::HandlePullDiscard</*is_pull=*/true>(impl, n, qid);

    switch (state_) {
      case State::PendingCommit:
        // CommitWouldBlockException thrown again; StashPendingCommit already re-set pending_commit_.
        return PendingCommitOutcome::Reschedule;
      case State::Idle:
      case State::Result:
        // Commit succeeded; HandlePullDiscard already sent SUCCESS to the client.
        pending_commit_ = false;
        return PendingCommitOutcome::Done;
      default:
        // State::Error or State::Close: HandleFailure sent the bolt error response.
        pending_commit_ = false;
        return PendingCommitOutcome::ClientError;
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
  // Set when HandlePullDiscard catches CommitWouldBlockException; cleared when the pool-side
  // retry completes (Done) or fails unrecoverably (ClientError).
  bool pending_commit_{false};

  // Arguments from the original PULL stashed so FinishPendingCommit_ can retry without re-decoding.
  std::optional<int> pending_commit_n_{};
  std::optional<int> pending_commit_qid_{};

  const std::string kTimestampFormat = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:06d}";
  const std::string session_uuid_;  //!< unique identifier of the session (auto generated)
  const std::string login_timestamp_;
};

}  // namespace memgraph::communication::bolt
