#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/encoder/base_encoder.hpp"

namespace communication::bolt {

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
  Encoder(Buffer &buffer) : BaseEncoder<Buffer>(buffer) {}

  /**
   * Writes a Record message. This method only stores data in the Buffer.
   * It doesn't send the values out to the Socket (Chunk is called at the
   * end of this method). To send the values Flush method has to be called
   * after this method.
   *
   * From the Bolt v1 documentation:
   *   RecordMessage (signature=0x71) {
   *     List<Value> fields
   *   }
   *
   * @param values the fields list object that should be sent
   */
  void MessageRecord(const std::vector<query::TypedValue> &values) {
    WriteRAW(underlying_cast(Marker::TinyStruct1));
    WriteRAW(underlying_cast(Signature::Record));
    WriteList(values);
    buffer_.Chunk();
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
   * @param flush should method flush the socket
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessageSuccess(const std::map<std::string, query::TypedValue> &metadata,
                      bool flush = true) {
    WriteRAW(underlying_cast(Marker::TinyStruct1));
    WriteRAW(underlying_cast(Signature::Success));
    WriteMap(metadata);
    if (flush) {
      return buffer_.Flush();
    } else {
      buffer_.Chunk();
      // Chunk always succeeds, so return true
      return true;
    }
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
    std::map<std::string, query::TypedValue> metadata;
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
  bool MessageFailure(
      const std::map<std::string, query::TypedValue> &metadata) {
    WriteRAW(underlying_cast(Marker::TinyStruct1));
    WriteRAW(underlying_cast(Signature::Failure));
    WriteMap(metadata);
    return buffer_.Flush();
  }

  /**
   * Sends an Ignored message.
   *
   * From the bolt v1 documentation:
   *   IgnoredMessage (signature=0x7E) {
   *     Map<String,Value> metadata
   *   }
   *
   * @param metadata the metadata map object that should be sent
   * @returns true if the data was successfully sent to the client,
   *          false otherwise
   */
  bool MessageIgnored(
      const std::map<std::string, query::TypedValue> &metadata) {
    WriteRAW(underlying_cast(Marker::TinyStruct1));
    WriteRAW(underlying_cast(Signature::Ignored));
    WriteMap(metadata);
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
    WriteRAW(underlying_cast(Marker::TinyStruct));
    WriteRAW(underlying_cast(Signature::Ignored));
    return buffer_.Flush();
  }
};
}
