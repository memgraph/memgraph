#pragma once

#include <glog/logging.h>

#include "communication/bolt/v1/decoder/buffer.hpp"
#include "communication/bolt/v1/decoder/chunked_decoder_buffer.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/chunked_encoder_buffer.hpp"
#include "communication/bolt/v1/encoder/client_encoder.hpp"

#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

class ClientException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class ClientSocketException : public ClientException {
 public:
  using ClientException::ClientException;
  ClientSocketException()
      : ClientException("Couldn't write/read data to/from the socket!") {}
};

class ClientInvalidDataException : public ClientException {
 public:
  using ClientException::ClientException;
  ClientInvalidDataException()
      : ClientException("The server sent invalid data!") {}
};

class ClientQueryException : public ClientException {
 public:
  using ClientException::ClientException;
  ClientQueryException() : ClientException("Couldn't execute query!") {}
};

struct QueryData {
  std::vector<std::string> fields;
  std::vector<std::vector<DecodedValue>> records;
  std::map<std::string, DecodedValue> metadata;
};

template <typename Socket>
class Client {
 public:
  Client(Socket &&socket, std::string &username, std::string &password,
         std::string client_name = "")
      : socket_(std::move(socket)) {
    DLOG(INFO) << "Sending handshake";
    if (!socket_.Write(kPreamble, sizeof(kPreamble))) {
      throw ClientSocketException();
    }
    for (int i = 0; i < 4; ++i) {
      if (!socket_.Write(kProtocol, sizeof(kProtocol))) {
        throw ClientSocketException();
      }
    }

    DLOG(INFO) << "Reading handshake response";
    if (!GetDataByLen(4)) {
      throw ClientSocketException();
    }
    if (memcmp(kProtocol, buffer_.data(), sizeof(kProtocol)) != 0) {
      throw ClientException("Server negotiated unsupported protocol version!");
    }
    buffer_.Shift(sizeof(kProtocol));

    if (client_name == "") {
      client_name = "memgraph-bolt/0.0.1";
    }

    DLOG(INFO) << "Sending init message";
    if (!encoder_.MessageInit(client_name, {{"scheme", "basic"},
                                            {"principal", username},
                                            {"credentials", password}})) {
      throw ClientSocketException();
    }

    DLOG(INFO) << "Reading init message response";
    Signature signature;
    DecodedValue metadata;
    if (!ReadMessage(&signature, &metadata)) {
      throw ClientException("Couldn't read init message response!");
    }
    if (signature != Signature::Success) {
      throw ClientInvalidDataException();
    }
    DLOG(INFO) << "Metadata of init message response: " << metadata;
  }

  Client(const Client &) = delete;
  Client(Client &&) = delete;
  Client &operator=(const Client &) = delete;
  Client &operator=(Client &&) = delete;

  QueryData Execute(const std::string &query,
                    const std::map<std::string, DecodedValue> &parameters) {
    DLOG(INFO) << "Sending run message with statement: '" << query
               << "'; parameters: " << parameters;

    std::map<std::string, query::TypedValue> params_tv(parameters.begin(),
                                                       parameters.end());
    encoder_.MessageRun(query, params_tv, false);
    encoder_.MessagePullAll();

    DLOG(INFO) << "Reading run message response";
    Signature signature;
    DecodedValue fields;
    if (!ReadMessage(&signature, &fields)) {
      throw ClientInvalidDataException();
    }
    if (fields.type() != DecodedValue::Type::Map) {
      throw ClientInvalidDataException();
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
      throw ClientInvalidDataException();
    }

    DLOG(INFO) << "Reading pull_all message response";
    Marker marker;
    DecodedValue metadata;
    std::vector<std::vector<DecodedValue>> records;
    while (true) {
      if (!GetDataByChunk()) {
        throw ClientInvalidDataException();
      }
      if (!decoder_.ReadMessageHeader(&signature, &marker)) {
        throw ClientInvalidDataException();
      }
      if (signature == Signature::Record) {
        DecodedValue record;
        if (!decoder_.ReadValue(&record, DecodedValue::Type::List)) {
          throw ClientInvalidDataException();
        }
        records.push_back(record.ValueList());
      } else if (signature == Signature::Success) {
        if (!decoder_.ReadValue(&metadata)) {
          throw ClientInvalidDataException();
        }
        break;
      } else if (signature == Signature::Failure) {
        DecodedValue data;
        if (!decoder_.ReadValue(&data)) {
          throw ClientInvalidDataException();
        }
        HandleFailure();
        auto &tmp = data.ValueMap();
        auto it = tmp.find("message");
        if (it != tmp.end()) {
          throw ClientQueryException(it->second.ValueString());
        }
        throw ClientQueryException();
      } else {
        throw ClientInvalidDataException();
      }
    }

    if (metadata.type() != DecodedValue::Type::Map) {
      throw ClientInvalidDataException();
    }

    QueryData ret{{}, records, metadata.ValueMap()};

    auto &header = fields.ValueMap();
    if (header.find("fields") == header.end()) {
      throw ClientInvalidDataException();
    }
    if (header["fields"].type() != DecodedValue::Type::List) {
      throw ClientInvalidDataException();
    }
    auto &field_vector = header["fields"].ValueList();

    for (auto &field_item : field_vector) {
      if (field_item.type() != DecodedValue::Type::String) {
        throw ClientInvalidDataException();
      }
      ret.fields.push_back(field_item.ValueString());
    }

    return ret;
  }

  void Close() { socket_.Close(); };

  ~Client() { Close(); }

 private:
  bool GetDataByLen(uint64_t len) {
    while (buffer_.size() < len) {
      auto buff = buffer_.Allocate();
      int ret = socket_.Read(buff.data, buff.len);
      if (ret == -1) return false;
      buffer_.Written(ret);
    }
    return true;
  }

  bool GetDataByChunk() {
    ChunkState state;
    while ((state = decoder_buffer_.GetChunk()) != ChunkState::Done) {
      if (state == ChunkState::Whole) {
        // The chunk is whole, no need to read more data.
        continue;
      }
      auto buff = buffer_.Allocate();
      int ret = socket_.Read(buff.data, buff.len);
      if (ret == -1) return false;
      buffer_.Written(ret);
    }
    return true;
  }

  bool ReadMessage(Signature *signature, DecodedValue *ret) {
    Marker marker;
    if (!GetDataByChunk()) {
      return false;
    }
    if (!decoder_.ReadMessageHeader(signature, &marker)) {
      return false;
    }
    return ReadMessageData(marker, ret);
  }

  bool ReadMessageData(Marker marker, DecodedValue *ret) {
    if (marker == Marker::TinyStruct) {
      *ret = DecodedValue();
      return true;
    } else if (marker == Marker::TinyStruct1) {
      return decoder_.ReadValue(ret);
    }
    return false;
  }

  void HandleFailure() {
    if (!encoder_.MessageAckFailure()) {
      throw ClientSocketException();
    }
    while (true) {
      Signature signature;
      DecodedValue data;
      if (!ReadMessage(&signature, &data)) {
        throw ClientInvalidDataException();
      }
      if (signature == Signature::Success) {
        break;
      } else if (signature != Signature::Ignored) {
        throw ClientInvalidDataException();
      }
    }
  }

  // socket
  Socket socket_;

  // decoder objects
  Buffer<> buffer_;
  ChunkedDecoderBuffer decoder_buffer_{buffer_};
  Decoder<ChunkedDecoderBuffer> decoder_{decoder_buffer_};

  // encoder objects
  ChunkedEncoderBuffer<Socket> encoder_buffer_{socket_};
  ClientEncoder<ChunkedEncoderBuffer<Socket>> encoder_{encoder_buffer_};
};
}
