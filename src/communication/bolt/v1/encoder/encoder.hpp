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

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/encoder/base_encoder.hpp"

namespace memgraph::communication::bolt {

/**
 * Bolt Encoder.
 * Has public interfaces for writing Bolt specific response messages.
 * Supported messages are: Record, Success, Failure and Ignored.
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class Encoder : private BaseEncoder<Buffer> {
 private:
  using BaseEncoder<Buffer>::WriteRAW;
  using BaseEncoder<Buffer>::WriteList;
  using BaseEncoder<Buffer>::WriteMap;
  using BaseEncoder<Buffer>::buffer_;

 public:
  explicit Encoder(Buffer &buffer) : BaseEncoder<Buffer>(buffer) {}

  using BaseEncoder<Buffer>::UpdateVersion;

  /**
   * Sends a Record message.
   *
   * From the Bolt v1 documentation:
   *   RecordMessage (signature=0x71) {
   *     List<Value> fields
   *   }
   *
   * @param values the fields list object that should be sent
   */
  bool MessageRecord(const std::vector<Value> &values) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct1));
    WriteRAW(utils::UnderlyingCast(Signature::Record));
    WriteList(values);
    // Try to flush all remaining data in the buffer, but tell it that we will
    // send more data (the end of message chunk).
    if (!buffer_.Flush(true)) return false;
    // Flush an empty chunk to indicate that the message is done. Here we tell
    // the buffer that there will be more data because this is a Record message
    // and it will surely be followed by either a Record, Success or Failure
    // message.
    return buffer_.Flush(true);
  }

  /**
   * Sends a Success message.
   *
   * From the Bolt v1 documentation:
   *   SuccessMessage (signature=0x70) {
   *     Map<String,Value> metadata
   *   }
   *
   * @param metadata the metadata map object that should be sent
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessageSuccess(const std::map<std::string, Value> &metadata) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct1));
    WriteRAW(utils::UnderlyingCast(Signature::Success));
    WriteMap(metadata);
    // Try to flush all remaining data in the buffer, but tell it that we will
    // send more data (the end of message chunk).
    if (!buffer_.Flush(true)) return false;
    // Flush an empty chunk to indicate that the message is done.
    return buffer_.Flush();
  }

  /**
   * Sends a Success message.
   *
   * This function sends a success message without additional metadata.
   *
   * @returns true if the data was successfully sent to the client,
   *          false otherwise
   */
  bool MessageSuccess() {
    std::map<std::string, Value> metadata;
    return MessageSuccess(metadata);
  }

  /**
   * Sends a Failure message.
   *
   * From the Bolt v1 documentation:
   *   FailureMessage (signature=0x7F) {
   *     Map<String,Value> metadata
   *   }
   *
   * @param metadata the metadata map object that should be sent
   * @returns true if the data was successfully sent to the client,
   *          false otherwise
   */
  bool MessageFailure(const std::map<std::string, Value> &metadata) {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct1));
    WriteRAW(utils::UnderlyingCast(Signature::Failure));
    WriteMap(metadata);
    // Try to flush all remaining data in the buffer, but tell it that we will
    // send more data (the end of message chunk).
    if (!buffer_.Flush(true)) return false;
    // Flush an empty chunk to indicate that the message is done.
    return buffer_.Flush();
  }

  /**
   * Sends an Ignored message.
   *
   * This function sends an ignored message without additional metadata.
   *
   * @returns true if the data was successfully sent to the client,
   *          false otherwise
   */
  bool MessageIgnored() {
    WriteRAW(utils::UnderlyingCast(Marker::TinyStruct));
    WriteRAW(utils::UnderlyingCast(Signature::Ignored));
    // Try to flush all remaining data in the buffer, but tell it that we will
    // send more data (the end of message chunk).
    if (!buffer_.Flush(true)) return false;
    // Flush an empty chunk to indicate that the message is done.
    return buffer_.Flush();
  }
};
}  // namespace memgraph::communication::bolt
