// Copyright 2024 Memgraph Ltd.
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

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"
#include "communication/bolt/v1/encoder/client_encoder.hpp"
#include "communication/client.hpp"
#include "communication/context.hpp"
#include "io/network/endpoint.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::communication::bolt {

class FailureResponseException : public utils::BasicException {
 public:
  FailureResponseException() : utils::BasicException{"Couldn't execute query!"} {}

  explicit FailureResponseException(const std::string &message) : utils::BasicException{message} {}

  FailureResponseException(std::string code, const std::string &message)
      : utils::BasicException{message}, code_{std::move(code)} {}

  const std::string &code() const { return code_; }
  SPECIALIZE_GET_EXCEPTION_NAME(FailureResponseException)

 private:
  std::string code_;
};

/// This exception is thrown whenever an error occurs during query execution
/// that isn't fatal (eg. mistyped query or some transient error occurred).
/// It should be handled by everyone who uses the client.
class ClientQueryException : public FailureResponseException {
 public:
  using FailureResponseException::FailureResponseException;
  SPECIALIZE_GET_EXCEPTION_NAME(ClientQueryException)
};

/// This exception is thrown whenever a fatal error occurs during query
/// execution and/or connecting to the server.
/// It should be handled by everyone who uses the client.
class ClientFatalException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(ClientFatalException)
};

// Internal exception used whenever a communication error occurs. You should
// only handle the `ClientFatalException`.
class ServerCommunicationException : public ClientFatalException {
 public:
  ServerCommunicationException() : ClientFatalException("Couldn't communicate with the server!") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ServerCommunicationException)
};

// Internal exception used whenever a malformed data error occurs. You should
// only handle the `ClientFatalException`.
class ServerMalformedDataException : public ClientFatalException {
 public:
  ServerMalformedDataException() : ClientFatalException("The server sent malformed data!") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ServerMalformedDataException)
};

/// Structure that is used to return results from an executed query.
struct QueryData {
  std::vector<std::string> fields;
  std::vector<std::vector<Value>> records;
  std::map<std::string, Value> metadata;
};

/// Bolt client.
/// It has methods used to connect to the server and execute queries against the
/// server. It supports both SSL and plaintext connections.
class Client final {
 public:
  explicit Client(communication::ClientContext &context);

  Client(const Client &) = delete;
  Client(Client &&) = delete;
  Client &operator=(const Client &) = delete;
  Client &operator=(Client &&) = delete;
  ~Client() = default;

  /// Method used to connect to the server. Before executing queries this method
  /// should be called to set-up the connection to the server. After the
  /// connection is set-up, multiple queries may be executed through a single
  /// established connection.
  /// @throws ClientFatalException when we couldn't connect to the server
  void Connect(const io::network::Endpoint &endpoint, const std::string &username, const std::string &password,
               const std::string &client_name = "memgraph-bolt");

  /// Function used to execute queries against the server. Before you can
  /// execute queries you must connect the client to the server.
  /// @throws ClientQueryException when there is some transient error while
  ///                              executing the query (eg. mistyped query,
  ///                              etc.)
  /// @throws ClientFatalException when we couldn't communicate with the server
  QueryData Execute(const std::string &query, const std::map<std::string, Value> &parameters);

  /// Close the active client connection.
  void Close();

  /// Can be used to reset the active client connection. Reset is automatically sent after receiving a failure message
  /// from the server, which result in throwing an FailureResponseException or any exception derived from it.
  void Reset();

  /// Can be used to send a route message.
  std::optional<std::map<std::string, Value>> Route(const std::map<std::string, Value> &routing,
                                                    const std::vector<Value> &bookmarks,
                                                    const std::optional<std::string> &db);

 private:
  using ClientEncoderX = ClientEncoder<ChunkedEncoderBuffer<communication::ClientOutputStream>>;

  template <typename TException = FailureResponseException>
  [[noreturn]] void HandleFailure(const std::map<std::string, Value> &response_map) {
    Reset();
    auto it = response_map.find("message");
    if (it != response_map.end()) {
      auto it_code = response_map.find("code");
      if (it_code != response_map.end()) {
        throw TException(it_code->second.ValueString(), it->second.ValueString());
      }
      throw TException("", it->second.ValueString());
    }
    throw TException();
  }

  bool GetMessage();
  bool ReadMessage(Signature &signature, Value &ret);
  bool ReadMessageData(Marker marker, Value &ret);

  // client
  communication::Client client_;
  communication::ClientInputStream input_stream_{client_};
  communication::ClientOutputStream output_stream_{client_};

  // decoder objects
  ChunkedDecoderBuffer<communication::ClientInputStream> decoder_buffer_{input_stream_};
  Decoder<ChunkedDecoderBuffer<communication::ClientInputStream>> decoder_{decoder_buffer_};

  // encoder objects
  ChunkedEncoderBuffer<communication::ClientOutputStream> encoder_buffer_{output_stream_};
  ClientEncoderX encoder_{encoder_buffer_};
};

}  // namespace memgraph::communication::bolt
