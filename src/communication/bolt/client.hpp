#pragma once

#include <glog/logging.h>

#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"
#include "communication/bolt/v1/encoder/client_encoder.hpp"
#include "communication/client.hpp"
#include "communication/context.hpp"
#include "io/network/endpoint.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

class ClientFatalException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  ClientFatalException()
      : utils::BasicException(
            "Something went wrong while communicating with the server!") {}
};

class ClientQueryException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  ClientQueryException() : utils::BasicException("Couldn't execute query!") {}
};

struct QueryData {
  std::vector<std::string> fields;
  std::vector<std::vector<Value>> records;
  std::map<std::string, Value> metadata;
};

class Client final {
 public:
  explicit Client(communication::ClientContext *context) : client_(context) {}

  Client(const Client &) = delete;
  Client(Client &&) = delete;
  Client &operator=(const Client &) = delete;
  Client &operator=(Client &&) = delete;

  bool Connect(const io::network::Endpoint &endpoint,
               const std::string &username, const std::string &password,
               const std::string &client_name = "memgraph-bolt/0.0.1") {
    if (!client_.Connect(endpoint)) {
      LOG(ERROR) << "Couldn't connect to " << endpoint;
      return false;
    }

    if (!client_.Write(kPreamble, sizeof(kPreamble), true)) {
      LOG(ERROR) << "Couldn't send preamble!";
      return false;
    }
    for (int i = 0; i < 4; ++i) {
      if (!client_.Write(kProtocol, sizeof(kProtocol), i != 3)) {
        LOG(ERROR) << "Couldn't send protocol version!";
        return false;
      }
    }

    if (!client_.Read(sizeof(kProtocol))) {
      LOG(ERROR) << "Couldn't get negotiated protocol version!";
      return false;
    }
    if (memcmp(kProtocol, client_.GetData(), sizeof(kProtocol)) != 0) {
      LOG(ERROR) << "Server negotiated unsupported protocol version!";
      return false;
    }
    client_.ShiftData(sizeof(kProtocol));

    if (!encoder_.MessageInit(client_name, {{"scheme", "basic"},
                                            {"principal", username},
                                            {"credentials", password}})) {
      LOG(ERROR) << "Couldn't send init message!";
      return false;
    }

    Signature signature;
    Value metadata;
    if (!ReadMessage(&signature, &metadata)) {
      LOG(ERROR) << "Couldn't read init message response!";
      return false;
    }
    if (signature != Signature::Success) {
      LOG(ERROR) << "Handshake failed!";
      return false;
    }

    DLOG(INFO) << "Metadata of init message response: " << metadata;

    return true;
  }

  QueryData Execute(const std::string &query,
                    const std::map<std::string, Value> &parameters) {
    DLOG(INFO) << "Sending run message with statement: '" << query
               << "'; parameters: " << parameters;

    encoder_.MessageRun(query, parameters);
    encoder_.MessagePullAll();

    DLOG(INFO) << "Reading run message response";
    Signature signature;
    Value fields;
    if (!ReadMessage(&signature, &fields)) {
      throw ClientFatalException();
    }
    if (fields.type() != Value::Type::Map) {
      throw ClientFatalException();
    }

    if (signature == Signature::Failure) {
      HandleFailure();
      auto &tmp = fields.ValueMap();
      auto it = tmp.find("message");
      if (it != tmp.end()) {
        throw ClientQueryException(it->second.ValueString());
      }
      throw ClientQueryException();
    } else if (signature != Signature::Success) {
      throw ClientFatalException();
    }

    DLOG(INFO) << "Reading pull_all message response";
    Marker marker;
    Value metadata;
    std::vector<std::vector<Value>> records;
    while (true) {
      if (!GetMessage()) {
        throw ClientFatalException();
      }
      if (!decoder_.ReadMessageHeader(&signature, &marker)) {
        throw ClientFatalException();
      }
      if (signature == Signature::Record) {
        Value record;
        if (!decoder_.ReadValue(&record, Value::Type::List)) {
          throw ClientFatalException();
        }
        records.emplace_back(std::move(record.ValueList()));
      } else if (signature == Signature::Success) {
        if (!decoder_.ReadValue(&metadata)) {
          throw ClientFatalException();
        }
        break;
      } else if (signature == Signature::Failure) {
        Value data;
        if (!decoder_.ReadValue(&data)) {
          throw ClientFatalException();
        }
        HandleFailure();
        auto &tmp = data.ValueMap();
        auto it = tmp.find("message");
        if (it != tmp.end()) {
          throw ClientQueryException(it->second.ValueString());
        }
        throw ClientQueryException();
      } else {
        throw ClientFatalException();
      }
    }

    if (metadata.type() != Value::Type::Map) {
      throw ClientFatalException();
    }

    QueryData ret{{}, std::move(records), std::move(metadata.ValueMap())};

    auto &header = fields.ValueMap();
    if (header.find("fields") == header.end()) {
      throw ClientFatalException();
    }
    if (header["fields"].type() != Value::Type::List) {
      throw ClientFatalException();
    }
    auto &field_vector = header["fields"].ValueList();

    for (auto &field_item : field_vector) {
      if (field_item.type() != Value::Type::String) {
        throw ClientFatalException();
      }
      ret.fields.emplace_back(std::move(field_item.ValueString()));
    }

    return ret;
  }

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
      throw ClientFatalException();
    }
    while (true) {
      Signature signature;
      Value data;
      if (!ReadMessage(&signature, &data)) {
        throw ClientFatalException();
      }
      if (signature == Signature::Success) {
        break;
      } else if (signature != Signature::Ignored) {
        throw ClientFatalException();
      }
    }
  }

  // client
  communication::Client client_;
  communication::ClientInputStream input_stream_{client_};
  communication::ClientOutputStream output_stream_{client_};

  // decoder objects
  ChunkedDecoderBuffer<communication::ClientInputStream> decoder_buffer_{
      input_stream_};
  Decoder<ChunkedDecoderBuffer<communication::ClientInputStream>> decoder_{
      decoder_buffer_};

  // encoder objects
  ChunkedEncoderBuffer<communication::ClientOutputStream> encoder_buffer_{
      output_stream_};
  ClientEncoder<ChunkedEncoderBuffer<communication::ClientOutputStream>>
      encoder_{encoder_buffer_};
};
}  // namespace communication::bolt
