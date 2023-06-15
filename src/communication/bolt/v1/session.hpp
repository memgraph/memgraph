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

#include <concepts>
#include <cstddef>
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
#include "dbms/constants.hpp"
#include "dbms/global.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"

namespace memgraph::communication::bolt {

template <typename T>
/**
 * @brief High level session interface concept.
 */
concept HLImpl = requires(T v) {
  { v.Interpret({}, {}, {}, {}) } -> std::same_as<std::pair<std::vector<std::string>, std::optional<int>>>;
  { v.Pull({}, {}, {}) } -> std::same_as<std::map<std::string, Value>>;
  { v.Discard({}, {}) } -> std::same_as<std::map<std::string, Value>>;
  { v.BeginTransaction({}) } -> std::same_as<void>;
  { v.CommitTransaction() } -> std::same_as<void>;
  { v.RollbackTransaction() } -> std::same_as<void>;
  { v.Abort() } -> std::same_as<void>;
  { v.Authenticate({}, {}) } -> std::same_as<bool>;
  { v.GetServerNameForInit() } -> std::same_as<std::optional<std::string>>;
};

/**
 * @brief Handles multiple high-level session instances.
 *
 * Each bolt::Session needs to have a high-level session instance (@ref SessionHL).
 * This hl session implements the core APIs (like: Interpret, Pull, etc.).
 * With multi-tenancy, one Bolt Session can support multiple hl sessions (one per used database).
 * Here we handle the multiple hl instances and their addition/deletion.
 *
 * @note about session concurency: Set and Del are called from the session context handler (singleton) and are
 * exclusive. GetImpl is called during normal operation, but it is safe, since the Set can be called only from the
 * session itself. Del can be called at any time, but the id_ (set during Set) protects the current_ ref.
 *
 * @tparam T high-level implementation's type
 */
template <typename T>
class MultiSessionHandler {
 public:
  /**
   * @brief Construct a new Multi Session Handler object
   *
   * @param id unique identifier of the instance (usually the database name)
   * @param p unique_ptr to the high-level session instance (the handler needs to have at least one at any time)
   */
  MultiSessionHandler(const std::string &id, std::unique_ptr<T> &&p)
      : id_(id), all_({std::make_pair(id, std::move(p))}), current_(all_[id]) {}
  ~MultiSessionHandler() = default;

  MultiSessionHandler(const MultiSessionHandler &) = delete;
  MultiSessionHandler &operator=(const MultiSessionHandler &) = delete;
  MultiSessionHandler(MultiSessionHandler &&) noexcept = delete;
  MultiSessionHandler &operator=(MultiSessionHandler &&) noexcept = delete;

  /**
   * @brief Set the current high-level implementation object
   *
   * @tparam TArgs types of arguments to pass to T's constructor
   * @param id unique identifier of the instance (usually the database name)
   * @param args arguments forwarded to the high-level implementation constructor
   * @return true on success
   */
  template <typename... TArgs>
  dbms::SetForResult SetImpl(std::string id, TArgs &&...args) {
    if (id.empty()) {
      return dbms::SetForResult::FAIL;
    }
    if (id == id_) {
      return dbms::SetForResult::ALREADY_SET;
    }
    auto &ptr = all_[id];
    if (ptr == nullptr) {
      ptr = std::make_shared<T>(std::forward<TArgs>(args)...);
    }
    current_ = ptr;
    id_ = id;
    return dbms::SetForResult::SUCCESS;
  }

  /**
   * @brief Delete a high-level implementation object
   *
   * @param id unique identifier of the instance (usually the database name)
   * @return true on success
   * @return false if using the instance
   */
  bool DelImpl(std::string id) {
    if (id == id_) return false;
    if (auto itr = all_.find(id); itr != all_.end()) {
      all_.erase(itr);
    }
    return true;
  }

  /**
   * @brief Get the current unique id identifying the high-level implementation.
   *
   * @return std::string
   */
  std::string GetID() const { return id_; }

 protected:
  /**
   * @brief Get the current high-level implementation object
   *
   * @return std::shared_ptr<T>
   */
  std::shared_ptr<T> GetImpl() { return current_; }

 private:
  std::string id_;  //!< identifier of the current high-level implementation
  std::unordered_map<std::string, std::shared_ptr<T>>
      all_;  //!< map of all hl implementations used @note a single implementation can be used at a time, but multiple
             //!< are ready for fast switching in case of a query spanning multiple databases
  std::shared_ptr<T> current_;  //!< currently used hl implementation
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
template <typename TInputStream, typename TOutputStream, HLImpl TSession>
class Session : public MultiSessionHandler<TSession> {
 public:
  using TEncoder = Encoder<ChunkedEncoderBuffer<TOutputStream>>;
  using HLImplT = TSession;
  using MultiSessionHandler<TSession>::GetImpl;

  /**
   * @brief Construct a new Session object
   *
   * @param input_stream stream to read from
   * @param output_stream stream to write to
   * @param impl a default high-level implementation to use (has to be defined)
   */
  Session(TInputStream *input_stream, TOutputStream *output_stream, std::unique_ptr<TSession> &&impl)
      : MultiSessionHandler<TSession>(dbms::kDefaultDB, std::move(impl)),
        input_stream_(*input_stream),
        output_stream_(*output_stream),
        session_uuid_(utils::GenerateUUID()) {}

  /**
   * Process the given `query` with `params`.
   * @return A pair which contains list of headers and qid which is set only
   * if an explicit transaction was started.
   */
  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, Value> &params,
      const std::map<std::string, memgraph::communication::bolt::Value> &metadata) {
    return GetImpl()->Interpret(query, params, metadata, session_uuid_);
  }

  /**
   * Put results of the processed query in the `encoder`.
   *
   * @param n If set, defines amount of rows to be pulled from the result,
   * otherwise all the rows are pulled.
   * @param q If set, defines from which query to pull the results,
   * otherwise the last query is used.
   */
  std::map<std::string, Value> Pull(TEncoder *encoder, std::optional<int> n, std::optional<int> qid) {
    return GetImpl()->Pull(encoder, n, qid);
  }

  /**
   * Discard results of the processed query.
   *
   * @param n If set, defines amount of rows to be discarded from the result,
   * otherwise all the rows are discarded.
   * @param q If set, defines from which query to discard the results,
   * otherwise the last query is used.
   */
  std::map<std::string, Value> Discard(std::optional<int> n, std::optional<int> qid) {
    return GetImpl()->Discard(n, qid);
  }

  void BeginTransaction(const std::map<std::string, memgraph::communication::bolt::Value> &params) {
    return GetImpl()->BeginTransaction(params);
  }
  void CommitTransaction() { return GetImpl()->CommitTransaction(); }
  void RollbackTransaction() { return GetImpl()->RollbackTransaction(); }

  /** Aborts currently running query. */
  void Abort() { return GetImpl()->Abort(); }

  /** Return `true` if the user was successfully authenticated. */
  bool Authenticate(const std::string &username, const std::string &password) {
    return GetImpl()->Authenticate(username, password);
  }

  /** Return the name of the server that should be used for the Bolt INIT
   * message. */
  std::optional<std::string> GetServerNameForInit() { return GetImpl()->GetServerNameForInit(); }

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
      // Update the decoder's Bolt version (v5 has changed the undelying structure)
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

  std::string UUID() const { return session_uuid_; }

  // TODO: Rethink if there is a way to hide some members. At the momement all of them are public.
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

  const std::string session_uuid_;  //!< unique identifier of the session (auto generated)
};

}  // namespace memgraph::communication::bolt
