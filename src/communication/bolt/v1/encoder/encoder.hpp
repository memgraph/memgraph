#pragma once

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
  using Loggable::logger;
  using BaseEncoder<Buffer>::WriteRAW;
  using BaseEncoder<Buffer>::WriteList;
  using BaseEncoder<Buffer>::WriteMap;
  using BaseEncoder<Buffer>::buffer_;

 public:
  Encoder(Buffer &buffer)
      : BaseEncoder<Buffer>(buffer) {
    logger = logging::log->logger("communication::bolt::Encoder");
  }

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
  void MessageRecord(const std::vector<TypedValue> &values) {
    // 0xB1 = struct 1; 0x71 = record signature
    WriteRAW("\xB1\x71", 2);
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
   */
  void MessageSuccess(const std::map<std::string, TypedValue> &metadata,
                      bool flush = true) {
    // 0xB1 = struct 1; 0x70 = success signature
    WriteRAW("\xB1\x70", 2);
    WriteMap(metadata);
    if (flush)
      buffer_.Flush();
    else
      buffer_.Chunk();
  }

  /**
   * Sends a Success message.
   *
   * This function sends a success message without additional metadata.
   */
  void MessageSuccess() {
    std::map<std::string, TypedValue> metadata;
    MessageSuccess(metadata);
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
   */
  void MessageFailure(const std::map<std::string, TypedValue> &metadata) {
    // 0xB1 = struct 1; 0x7F = failure signature
    WriteRAW("\xB1\x7F", 2);
    WriteMap(metadata);
    buffer_.Flush();
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
   */
  void MessageIgnored(const std::map<std::string, TypedValue> &metadata) {
    // 0xB1 = struct 1; 0x7E = ignored signature
    WriteRAW("\xB1\x7E", 2);
    WriteMap(metadata);
    buffer_.Flush();
  }

  /**
   * Sends an Ignored message.
   *
   * This function sends an ignored message without additional metadata.
   */
  void MessageIgnored() {
    // 0xB0 = struct 0; 0x7E = ignored signature
    WriteRAW("\xB0\x7E", 2);
    buffer_.Flush();
  }
};
}
