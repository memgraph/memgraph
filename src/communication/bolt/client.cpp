// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/bolt/client.hpp"

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/value.hpp"
#include "utils/logging.hpp"

namespace {
constexpr uint8_t kBoltV43Version[4] = {0x00, 0x00, 0x03, 0x04};
constexpr uint8_t kEmptyBoltVersion[4] = {0x00, 0x00, 0x00, 0x00};
}  // namespace
namespace memgraph::communication::bolt {

Client::Client(communication::ClientContext &context) : client_{&context} {}

void Client::Connect(const io::network::Endpoint &endpoint, const std::string &username, const std::string &password,
                     const std::string &client_name) {
  if (!client_.Connect(endpoint)) {
    throw ClientFatalException("Couldn't connect to {}!", endpoint);
  }

  if (!client_.Write(kPreamble, sizeof(kPreamble), true)) {
    spdlog::error("Couldn't send preamble!");
    throw ServerCommunicationException();
  }

  if (!client_.Write(kBoltV43Version, sizeof(kBoltV43Version), true)) {
    spdlog::error("Couldn't send protocol version!");
    throw ServerCommunicationException();
  }

  for (int i = 0; i < 3; ++i) {
    if (!client_.Write(kEmptyBoltVersion, sizeof(kEmptyBoltVersion), i != 2)) {
      spdlog::error("Couldn't send protocol version!");
      throw ServerCommunicationException();
    }
  }

  if (!client_.Read(sizeof(kBoltV43Version))) {
    spdlog::error("Couldn't get negotiated protocol version!");
    throw ServerCommunicationException();
  }

  if (memcmp(kBoltV43Version, client_.GetData(), sizeof(kBoltV43Version)) != 0) {
    spdlog::error("Server negotiated unsupported protocol version!");
    throw ClientFatalException("The server negotiated an unsupported protocol version!");
  }
  client_.ShiftData(sizeof(kBoltV43Version));

  if (!encoder_.MessageInit({{"user_agent", client_name},
                             {"scheme", "basic"},
                             {"principal", username},
                             {"credentials", password},
                             {"routing", {}}})) {
    spdlog::error("Couldn't send init message!");
    throw ServerCommunicationException();
  }

  Signature signature{};
  Value metadata;
  if (!ReadMessage(signature, metadata)) {
    spdlog::error("Couldn't read init message response!");
    throw ServerCommunicationException();
  }
  if (signature != Signature::Success) {
    spdlog::error("Handshake failed!");
    throw ClientFatalException("Handshake with the server failed!");
  }

  spdlog::debug("Metadata of init message response: {}", metadata);
}

QueryData Client::Execute(const std::string &query, const std::map<std::string, Value> &parameters) {
  if (!client_.IsConnected()) {
    throw ClientFatalException("You must first connect to the server before using the client!");
  }

  spdlog::debug("Sending run message with statement: '{}'; parameters: {}", query, parameters);

  // It is super critical from performance point of view to send the pull message right after the run message. Otherwise
  // the performance will degrade multiple magnitudes.
  encoder_.MessageRun(query, parameters, {});
  encoder_.MessagePull({{"n", Value(-1)}});

  spdlog::debug("Reading run message response");
  Signature signature{};
  Value fields;
  if (!ReadMessage(signature, fields)) {
    throw ServerCommunicationException();
  }
  if (fields.type() != Value::Type::Map) {
    throw ServerMalformedDataException();
  }

  if (signature == Signature::Failure) {
    HandleFailure<ClientQueryException>(fields.ValueMap());
  }
  if (signature != Signature::Success) {
    throw ServerMalformedDataException();
  }

  spdlog::debug("Reading pull_all message response");
  Marker marker{};
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
      HandleFailure<ClientQueryException>(data.ValueMap());
    } else {
      throw ServerMalformedDataException();
    }
  }

  if (metadata.type() != Value::Type::Map) {
    throw ServerMalformedDataException();
  }

  auto &header = fields.ValueMap();

  QueryData ret{{}, std::move(records), std::move(metadata.ValueMap())};

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

  if (header.contains("qid")) {
    ret.metadata["qid"] = header["qid"];
  }

  return ret;
}

void Client::Reset() {
  if (!client_.IsConnected()) {
    throw ClientFatalException("You must first connect to the server before using the client!");
  }

  spdlog::debug("Sending reset message");

  encoder_.MessageReset();

  Signature signature{};
  Value fields;
  // In Execute the pull message is sent right after the run message without reading the answer for the run message.
  // That means some of the messages sent might get ignored.
  while (true) {
    if (!ReadMessage(signature, fields)) {
      throw ServerCommunicationException();
    }
    if (signature == Signature::Success) {
      break;
    }
    if (signature != Signature::Ignored) {
      throw ServerMalformedDataException();
    }
  }
}

std::optional<std::map<std::string, Value>> Client::Route(const std::map<std::string, Value> &routing,
                                                          const std::vector<Value> &bookmarks,
                                                          const std::optional<std::string> &db) {
  if (!client_.IsConnected()) {
    throw ClientFatalException("You must first connect to the server before using the client!");
  }

  spdlog::debug("Sending route message with routing: {}; bookmarks: {}; db: {}", routing, bookmarks,
                db.has_value() ? *db : Value());

  encoder_.MessageRoute(routing, bookmarks, db);

  spdlog::debug("Reading route message response");
  Signature signature{};
  Value fields;
  if (!ReadMessage(signature, fields)) {
    throw ServerCommunicationException();
  }
  if (signature == Signature::Ignored) {
    return std::nullopt;
  }
  if (signature == Signature::Failure) {
    HandleFailure(fields.ValueMap());
  }
  if (signature != Signature::Success) {
    throw ServerMalformedDataException{};
  }
  return fields.ValueMap();
}

void Client::Close() { client_.Close(); };

bool Client::GetMessage() {
  client_.ClearData();
  while (true) {
    if (!client_.Read(kChunkHeaderSize)) return false;

    size_t chunk_size = client_.GetData()[0];
    chunk_size <<= 8U;
    chunk_size += client_.GetData()[1];
    if (chunk_size == 0) return true;

    if (!client_.Read(chunk_size)) return false;
    if (decoder_buffer_.GetChunk() != ChunkState::Whole) return false;
    client_.ClearData();
  }
  return true;
}

bool Client::ReadMessage(Signature &signature, Value &ret) {
  Marker marker{};
  if (!GetMessage()) return false;
  if (!decoder_.ReadMessageHeader(&signature, &marker)) return false;
  return ReadMessageData(marker, ret);
}

bool Client::ReadMessageData(Marker marker, Value &ret) {
  if (marker == Marker::TinyStruct) {
    ret = Value();
    return true;
  }
  if (marker == Marker::TinyStruct1) {
    return decoder_.ReadValue(&ret);
  }
  return false;
}
}  // namespace memgraph::communication::bolt
