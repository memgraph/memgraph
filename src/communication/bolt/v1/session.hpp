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
#include "query/exceptions.hpp"
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

// Outcome of the strand-side inline-BEGIN fast path (TryInlineBegin_).
// NotBegin  - the buffered message is not an inline-eligible BEGIN; leave it for the normal pool path.
// Handled   - BEGIN ran inline and SUCCESS was sent; nothing left to do.
// ClientError - the message was malformed / send failed; state moved to Close.
// WouldBlock - the storage lock was contended; extras are stashed for FinishPendingBeginBlocking_ on the pool.
enum class InlineBeginResult : uint8_t { NotBegin, Handled, ClientError, WouldBlock };

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

      if (state_ == State::Close) [[unlikely]] {
        // State::Close is handled here because we always want to check for
        // it after the above select. If any of the states above return a
        // State::Close then the connection should be terminated immediately.
        ClientFailureInvalidData();
      }
    }
    return false;  // no more data
  }

  bool HasBufferedData() const { return input_stream_.size() > 0; }

  // Strand-side fast path: run an explicit BEGIN inline (on the ASIO strand) when the whole message has
  // arrived and the storage lock is uncontended, saving the thread-pool hop. See InlineBeginResult for the
  // per-outcome contract.
  template <typename TImpl>
    requires requires(TImpl &impl) {
      { impl.GetLogContext() } -> std::same_as<memgraph::logging::SessionLogContext *>;
    }
  InlineBeginResult TryInlineBegin_(TImpl &impl) {
    // Inline only for bolt v4+ and only from an idle (not-in-txn) state.
    if (version_.major < 4) return InlineBeginResult::NotBegin;
    if (state_ != State::Idle) return InlineBeginResult::NotBegin;

    // Non-destructive raw-buffer peek: GetChunk() is destructive, so it must not run until we are certain
    // this is a fully-arrived single-chunk BEGIN -- a non-BEGIN or partial message must stay intact for the pool path.
    const size_t avail = input_stream_.size();
    if (avail < 4) return InlineBeginResult::NotBegin;
    const uint8_t *raw = input_stream_.data();
    const size_t chunk_size = (static_cast<size_t>(raw[0]) << 8) | raw[1];
    // Require the whole single chunk + its 0x00 0x00 terminator to be present.
    if (avail < 2 + chunk_size + 2) return InlineBeginResult::NotBegin;
    if (raw[2 + chunk_size] != 0 || raw[2 + chunk_size + 1] != 0)
      return InlineBeginResult::NotBegin;  // multi-chunk -> normal path
    if (raw[2] != static_cast<uint8_t>(Marker::TinyStruct1)) return InlineBeginResult::NotBegin;
    if (raw[3] != static_cast<uint8_t>(Signature::Begin)) return InlineBeginResult::NotBegin;

    // Committed to inline handling of this BEGIN. Replicate the side effects the normal
    // Execute_/StateExecutingRun path performs, exactly once.
    memgraph::logging::ScopedSessionLog log_guard(impl.GetLogContext());
    memgraph::metrics::Metrics().global.bolt_messages->Increment();
    at_least_one_run_ = true;

    ChunkState cs;
    while ((cs = decoder_buffer_.GetChunk()) == ChunkState::Whole) { /* drain chunks */ }
    if (cs != ChunkState::Done) return InlineBeginResult::NotBegin;  // defensive; completeness was checked above

    Marker marker;
    Signature signature;
    if (!decoder_.ReadMessageHeader(&signature, &marker)) {
      state_ = State::Close;
      return InlineBeginResult::ClientError;
    }

    Value extra;
    if (!decoder_.ReadValue(&extra, Value::Type::Map)) {
      state_ = State::Close;
      return InlineBeginResult::ClientError;
    }

    // Off-strand for any BEGIN that needs a real reconfigure (first BEGIN, db-switch, impersonation):
    // Configure would take the dbms/auth locks, which can block. Hand the whole BEGIN to the pool.
    if (!impl.ConfigureWouldBeNoOp(extra.ValueMap())) {
      pending_begin_extra_ = std::move(extra);
      return InlineBeginResult::WouldBlock;  // fallback runs the FULL begin (Configure + BeginTransaction) on a worker
    }

    try {
      // Configure is verified a no-op above, so it is skipped here; the full begin runs on the pool otherwise.
      impl.BeginTransaction(extra.ValueMap(), /*try_only=*/true);  // may throw WouldBlockInlineException
      if (!encoder_.MessageSuccess({})) {
        state_ = State::Close;
        return InlineBeginResult::ClientError;
      }
      state_ = State::Idle;
      return InlineBeginResult::Handled;
    } catch (const memgraph::query::WouldBlockInlineException &) {
      // Clean bail: accessor acquired first, so interpreter txn state is untouched. Stash extras for the pool fallback.
      pending_begin_extra_ = std::move(extra);
      return InlineBeginResult::WouldBlock;
    } catch (const std::exception &e) {
      state_ = HandleFailure(impl, e);  // same failure handling as the normal HandleBegin path
      return InlineBeginResult::ClientError;
    }
  }

  // Pool-side completion of a BEGIN that bailed inline with WouldBlock. Runs the FULL begin (Configure +
  // BeginTransaction) on the worker so it covers BOTH bail causes: the gate bail (a real, lock-taking
  // reconfigure was needed) and the engine-lock contention bail. Configure re-running when it was actually a
  // no-op is harmless -- it early-outs again. bolt_messages/at_least_one_run_ were already counted by
  // TryInlineBegin_, so they are NOT touched here.
  template <typename TImpl>
    requires requires(TImpl &impl) {
      { impl.GetLogContext() } -> std::same_as<memgraph::logging::SessionLogContext *>;
    }
  void FinishPendingBeginBlocking_(TImpl &impl) {
    memgraph::logging::ScopedSessionLog log_guard(impl.GetLogContext());
    MG_ASSERT(pending_begin_extra_.has_value(), "FinishPendingBeginBlocking_ without a pending BEGIN");
    Value extra = std::move(*pending_begin_extra_);
    pending_begin_extra_.reset();
    try {
      impl.Configure(extra.ValueMap());                            // real reconfigure runs here, on the worker (blocking OK)
      impl.BeginTransaction(extra.ValueMap(), /*try_only=*/false);  // blocking Access on the worker
      if (!encoder_.MessageSuccess({})) {
        state_ = State::Close;
        return;
      }
      state_ = State::Idle;
    } catch (const std::exception &e) {
      state_ = HandleFailure(impl, e);
    }
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
  // Extras decoded by an inline BEGIN that bailed with WouldBlock, carried across the strand->pool handoff so
  // FinishPendingBeginBlocking_ can retry the Access without re-decoding.
  std::optional<Value> pending_begin_extra_{};

  const std::string kTimestampFormat = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:06d}";
  const std::string session_uuid_;  //!< unique identifier of the session (auto generated)
  const std::string login_timestamp_;
};

}  // namespace memgraph::communication::bolt
