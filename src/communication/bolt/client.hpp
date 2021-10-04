// Copyright 2021 Memgraph Ltd.
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

#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"
#include "communication/bolt/v1/encoder/client_encoder.hpp"
#include "communication/client.hpp"
#include "communication/context.hpp"
#include "io/network/endpoint.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

namespace communication::bolt {

/// This exception is thrown whenever an error occurs during query execution
/// that isn't fatal (eg. mistyped query or some transient error occurred).
/// It should be handled by everyone who uses the client.
class ClientQueryException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;

  ClientQueryException() : utils::BasicException("Couldn't execute query!") {}

  template <class... Args>
  ClientQueryException(const std::string &code, Args &&...args)
      : utils::BasicException(std::forward<Args>(args)...), code_(code) {}

  const std::string &code() const { return code_; }

 private:
  std::string code_;
};

/// This exception is thrown whenever a fatal error occurs during query
/// execution and/or connecting to the server.
/// It should be handled by everyone who uses the client.
class ClientFatalException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

// Internal exception used whenever a communication error occurs. You should
// only handle the `ClientFatalException`.
class ServerCommunicationException : public ClientFatalException {
 public:
  ServerCommunicationException() : ClientFatalException("Couldn't communicate with the server!") {}
};

// Internal exception used whenever a malformed data error occurs. You should
// only handle the `ClientFatalException`.
class ServerMalformedDataException : public ClientFatalException {
 public:
  ServerMalformedDataException() : ClientFatalException("The server sent malformed data!") {}
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
  explicit Client(communication::ClientContext *context) : client_(context) {}

  Client(const Client &) = delete;
  Client(Client &&) = delete;
  Client &operator=(const Client &) = delete;
  Client &operator=(Client &&) = delete;

  /// Method used to connect to the server. Before executing queries this method
  /// should be called to set-up the connection to the server. After the
  /// connection is set-up, multiple queries may be executed through a single
  /// established connection.
  /// @throws ClientFatalException when we couldn't connect to the server
  void Connect(const io::network::Endpoint &endpoint, const std::string &username, const std::string &password,
               const std::string &client_name = "memgraph-bolt") {
    if (!client_.Connect(endpoint)) {
      throw ClientFatalException("Couldn't connect to {}!", endpoint);
    }

    if (!client_.Write(kPreamble, sizeof(kPreamble), true)) {
      SPDLOG_ERROR("Couldn't send preamble!");
      throw ServerCommunicationException();
    }
    for (int i = 0; i < 4; ++i) {
      if (!client_.Write(kProtocol, sizeof(kProtocol), i != 3)) {
        SPDLOG_ERROR("Couldn't send protocol version!");
        throw ServerCommunicationException();
      }
    }

    if (!client_.Read(sizeof(kProtocol))) {
      SPDLOG_ERROR("Couldn't get negotiated protocol version!");
      throw ServerCommunicationException();
    }
    if (memcmp(kProtocol, client_.GetData(), sizeof(kProtocol)) != 0) {
      SPDLOG_ERROR("Server negotiated unsupported protocol version!");
      throw ClientFatalException("The server negotiated an usupported protocol version!");
    }
    client_.ShiftData(sizeof(kProtocol));

    if (!encoder_.MessageInit(client_name, {{"scheme", "basic"}, {"principal", username}, {"credentials", password}})) {
      SPDLOG_ERROR("Couldn't send init message!");
      throw ServerCommunicationException();
    }

    Signature signature;
    Value metadata;
    if (!ReadMessage(&signature, &metadata)) {
      SPDLOG_ERROR("Couldn't read init message response!");
      throw ServerCommunicationException();
    }
    if (signature != Signature::Success) {
      SPDLOG_ERROR("Handshake failed!");
      throw ClientFatalException("Handshake with the server failed!");
    }

    SPDLOG_INFO("Metadata of init message response: {}", metadata);
  }

  /// Function used to execute queries against the server. Before you can
  /// execute queries you must connect the client to the server.
  /// @throws ClientQueryException when there is some transient error while
  ///                              executing the query (eg. mistyped query,
  ///                              etc.)
  /// @throws ClientFatalException when we couldn't communicate with the server
  QueryData Execute(const std::string &query, const std::map<std::string, Value> &parameters) {
    if (!client_.IsConnected()) {
      throw ClientFatalException("You must first connect to the server before using the client!");
    }

    SPDLOG_INFO("Sending run message with statement: '{}'; parameters: {}", query, parameters);

    encoder_.MessageRun(query, parameters);
    encoder_.MessagePullAll();

    SPDLOG_INFO("Reading run message response");
    Signature signature;
    Value fields;
    if (!ReadMessage(&signature, &fields)) {
      throw ServerCommunicationException();
    }
    if (fields.type() != Value::Type::Map) {
      throw ServerMalformedDataException();
    }

    if (signature == Signature::Failure) {
      HandleFailure();
      auto &tmp = fields.ValueMap();
      auto it = tmp.find("message");
      if (it != tmp.end()) {
        auto it_code = tmp.find("code");
        if (it_code != tmp.end()) {
          throw ClientQueryException(it_code->second.ValueString(), it->second.ValueString());
        } else {
          throw ClientQueryException("", it->second.ValueString());
        }
      }
      throw ClientQueryException();
    } else if (signature != Signature::Success) {
      throw ServerMalformedDataException();
    }

    SPDLOG_INFO("Reading pull_all message response");
    Marker marker;
    Value metadata;
    std::vector<std::vector<Value>> records;
    while (true) {
      if (!GetMessage()) {
        throw ServerCommunicationException();
      }
      if (!decoder_.ReadMessageHeader(&signature, &marker)) {
        throw ServerCommunicationException();
      }
      if (signature == Signature::Record) {
        Value record;
        if (!decoder_.ReadValue(&record, Value::Type::List)) {
          throw ServerCommunicationException();
        }
        records.emplace_back(std::move(record.ValueList()));
      } else if (signature == Signature::Success) {
        if (!decoder_.ReadValue(&metadata)) {
          throw ServerCommunicationException();
        }
        break;
      } else if (signature == Signature::Failure) {
        Value data;
        if (!decoder_.ReadValue(&data)) {
          throw ServerCommunicationException();
        }
        HandleFailure();
        auto &tmp = data.ValueMap();
        auto it = tmp.find("message");
        if (it != tmp.end()) {
          auto it_code = tmp.find("code");
          if (it_code != tmp.end()) {
            throw ClientQueryException(it_code->second.ValueString(), it->second.ValueString());
          } else {
            throw ClientQueryException("", it->second.ValueString());
          }
        }
        throw ClientQueryException();
      } else {
        throw ServerMalformedDataException();
      }
    }

    if (metadata.type() != Value::Type::Map) {
      throw ServerMalformedDataException();
    }

    QueryData ret{{}, std::move(records), std::move(metadata.ValueMap())};

    auto &header = fields.ValueMap();
    if (header.find("fields") == header.end()) {
      throw ServerMalformedDataException();
    }
    if (header["fields"].type() != Value::Type::List) {
      throw ServerMalformedDataException();
    }
    auto &field_vector = header["fields"].ValueList();

    for (auto &field_item : field_vector) {
      if (field_item.type() != Value::Type::String) {
        throw ServerMalformedDataException();
      }
      ret.fields.emplace_back(std::move(field_item.ValueString()));
    }

    return ret;
  }

  /// Close the active client connection.
  void Close() { client_.Close(); };

 private:
  bool GetMessage() {
    client_.ClearData();
    while (true) {
      if (!client_.Read(kChunkHeaderSize)) return false;

      size_t chunk_size = client_.GetData()[0];
      chunk_size <<= 8;
      chunk_size += client_.GetData()[1];
      if (chunk_size == 0) return true;

      if (!client_.Read(chunk_size)) return false;
      if (decoder_buffer_.GetChunk() != ChunkState::Whole) return false;
      client_.ClearData();
    }
    return true;
  }

  bool ReadMessage(Signature *signature, Value *ret) {
    Marker marker;
    if (!GetMessage()) return false;
    if (!decoder_.ReadMessageHeader(signature, &marker)) return false;
    return ReadMessageData(marker, ret);
  }

  bool ReadMessageData(Marker marker, Value *ret) {
    if (marker == Marker::TinyStruct) {
      *ret = Value();
      return true;
    } else if (marker == Marker::TinyStruct1) {
      return decoder_.ReadValue(ret);
    }
    return false;
  }

  void HandleFailure() {
    if (!encoder_.MessageAckFailure()) {
      throw ServerCommunicationException();
    }
    while (true) {
      Signature signature;
      Value data;
      if (!ReadMessage(&signature, &data)) {
        throw ServerCommunicationException();
      }
      if (signature == Signature::Success) {
        break;
      } else if (signature != Signature::Ignored) {
        throw ServerMalformedDataException();
      }
    }
  }

  // client
  communication::Client client_;
  communication::ClientInputStream input_stream_{client_};
  communication::ClientOutputStream output_stream_{client_};

  // decoder objects
  ChunkedDecoderBuffer<communication::ClientInputStream> decoder_buffer_{input_stream_};
  Decoder<ChunkedDecoderBuffer<communication::ClientInputStream>> decoder_{decoder_buffer_};

  // encoder objects
  ChunkedEncoderBuffer<communication::ClientOutputStream> encoder_buffer_{output_stream_};
  ClientEncoder<ChunkedEncoderBuffer<communication::ClientOutputStream>> encoder_{encoder_buffer_};
};
}  // namespace communication::bolt
