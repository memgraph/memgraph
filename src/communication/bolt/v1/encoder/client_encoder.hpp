#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/encoder/base_encoder.hpp"

namespace communication::bolt {

/**
 * Bolt Client Encoder.
 * Has public interfaces for writing Bolt specific request messages.
 * Supported messages are: Init, Run, DiscardAll, PullAll, AckFailure, Reset
 *
 * @tparam Buffer the output buffer that should be used
 */
template <typename Buffer>
class ClientEncoder : private BaseEncoder<Buffer> {
 private:
  using BaseEncoder<Buffer>::WriteRAW;
  using BaseEncoder<Buffer>::WriteList;
  using BaseEncoder<Buffer>::WriteMap;
  using BaseEncoder<Buffer>::WriteString;
  using BaseEncoder<Buffer>::buffer_;

 public:
  ClientEncoder(Buffer &buffer) : BaseEncoder<Buffer>(buffer) {}

  /**
   * Writes a Init message.
   *
   * From the Bolt v1 documentation:
   *   InitMessage (signature=0x01) {
   *     String clientName
   *     Map<String,Value> authToken
   *   }
   *
   * @param client_name the name of the connected client
   * @param auth_token a map with authentication data
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessageInit(const std::string client_name,
                   const std::map<std::string, query::TypedValue> &auth_token) {
    WriteRAW(underlying_cast(Marker::TinyStruct2));
    WriteRAW(underlying_cast(Signature::Init));
    WriteString(client_name);
    WriteMap(auth_token);
    return buffer_.Flush();
  }

  /**
   * Writes a Run message.
   *
   * From the Bolt v1 documentation:
   *   RunMessage (signature=0x10) {
   *     String             statement
   *     Map<String,Value>  parameters
   *   }
   *
   * @param statement the statement that should be executed
   * @param parameters parameters that should be used to execute the statement
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessageRun(const std::string statement,
                  const std::map<std::string, query::TypedValue> &parameters,
                  bool flush = true) {
    WriteRAW(underlying_cast(Marker::TinyStruct2));
    WriteRAW(underlying_cast(Signature::Run));
    WriteString(statement);
    WriteMap(parameters);
    if (flush) {
      return buffer_.Flush();
    } else {
      buffer_.Chunk();
      // Chunk always succeeds, so return true
      return true;
    }
  }

  /**
   * Writes a DiscardAll message.
   *
   * From the Bolt v1 documentation:
   *   DiscardAllMessage (signature=0x2F) {
   *   }
   *
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessageDiscardAll() {
    WriteRAW(underlying_cast(Marker::TinyStruct));
    WriteRAW(underlying_cast(Signature::DiscardAll));
    return buffer_.Flush();
  }

  /**
   * Writes a PullAll message.
   *
   * From the Bolt v1 documentation:
   *   PullAllMessage (signature=0x3F) {
   *   }
   *
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessagePullAll() {
    WriteRAW(underlying_cast(Marker::TinyStruct));
    WriteRAW(underlying_cast(Signature::PullAll));
    return buffer_.Flush();
  }

  /**
   * Writes a AckFailure message.
   *
   * From the Bolt v1 documentation:
   *   AckFailureMessage (signature=0x0E) {
   *   }
   *
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessageAckFailure() {
    WriteRAW(underlying_cast(Marker::TinyStruct));
    WriteRAW(underlying_cast(Signature::AckFailure));
    return buffer_.Flush();
  }

  /**
   * Writes a Reset message.
   *
   * From the Bolt v1 documentation:
   *   ResetMessage (signature=0x0F) {
   *   }
   *
   * @returns true if the data was successfully sent to the client
   *          when flushing, false otherwise
   */
  bool MessageReset() {
    WriteRAW(underlying_cast(Marker::TinyStruct));
    WriteRAW(underlying_cast(Signature::Reset));
    return buffer_.Flush();
  }
};
}
