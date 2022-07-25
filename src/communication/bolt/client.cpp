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
  for (int i = 0; i < 4; ++i) {
    if (!client_.Write(kProtocol, sizeof(kProtocol), i != 3)) {
      spdlog::error("Couldn't send protocol version!");
      throw ServerCommunicationException();
    }
  }

  if (!client_.Read(sizeof(kProtocol))) {
    spdlog::error("Couldn't get negotiated protocol version!");
    throw ServerCommunicationException();
  }
  if (memcmp(kProtocol, client_.GetData(), sizeof(kProtocol)) != 0) {
    spdlog::error("Server negotiated unsupported protocol version!");
    throw ClientFatalException("The server negotiated an usupported protocol version!");
  }
  client_.ShiftData(sizeof(kProtocol));

  if (!encoder_.MessageInit(client_name, {{"scheme", "basic"}, {"principal", username}, {"credentials", password}})) {
    spdlog::error("Couldn't send init message!");
    throw ServerCommunicationException();
  }

  Signature signature;
  Value metadata;
  if (!ReadMessage(signature, metadata)) {
    spdlog::error("Couldn't read init message response!");
    throw ServerCommunicationException();
  }
  if (signature != Signature::Success) {
    spdlog::error("Handshake failed!");
    throw ClientFatalException("Handshake with the server failed!");
  }

  spdlog::info("Metadata of init message response: {}", metadata);
}

QueryData Client::Execute(const std::string &query, const std::map<std::string, Value> &parameters) {
  if (!client_.IsConnected()) {
    throw ClientFatalException("You must first connect to the server before using the client!");
  }

  spdlog::info("Sending run message with statement: '{}'; parameters: {}", query, parameters);

  encoder_.MessageRun(query, parameters);
  encoder_.MessagePullAll();

  spdlog::info("Reading run message response");
  Signature signature;
  Value fields;
  if (!ReadMessage(signature, fields)) {
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
      }
      throw ClientQueryException("", it->second.ValueString());
    }
    throw ClientQueryException();
  }
  if (signature != Signature::Success) {
    throw ServerMalformedDataException();
  }

  spdlog::info("Reading pull_all message response");
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
        }
        throw ClientQueryException("", it->second.ValueString());
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

void Client::Close() { client_.Close(); };

bool Client::GetMessage() {
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

bool Client::ReadMessage(Signature &signature, Value &ret) {
  Marker marker;
  if (!GetMessage()) return false;
  if (!decoder_.ReadMessageHeader(&signature, &marker)) return false;
  return ReadMessageData(marker, ret);
}

bool Client::ReadMessageData(Marker marker, Value &ret) {
  if (marker == Marker::TinyStruct) {
    ret = Value();
    return true;
  } else if (marker == Marker::TinyStruct1) {
    return decoder_.ReadValue(&ret);
  }
  return false;
}

void Client::HandleFailure() {
  if (!encoder_.MessageAckFailure()) {
    throw ServerCommunicationException();
  }
  while (true) {
    Signature signature;
    Value data;
    if (!ReadMessage(signature, data)) {
      throw ServerCommunicationException();
    }
    if (signature == Signature::Success) {
      break;
    } else if (signature != Signature::Ignored) {
      throw ServerMalformedDataException();
    }
  }
}

}  // namespace memgraph::communication::bolt
