#pragma once

#include <cstdint>
#include <memory>

namespace communication::rpc {

using MessageSize = uint32_t;

/// Type information on a RPC message.
/// Each message should have a static member `TypeInfo` with this information.
struct MessageType {
  /// Unique ID for a message.
  uint64_t id;
  /// Pretty name of the type.
  std::string name;
};

inline bool operator==(const MessageType &a, const MessageType &b) {
  return a.id == b.id;
}
inline bool operator!=(const MessageType &a, const MessageType &b) {
  return a.id != b.id;
}
inline bool operator<(const MessageType &a, const MessageType &b) {
  return a.id < b.id;
}
inline bool operator<=(const MessageType &a, const MessageType &b) {
  return a.id <= b.id;
}
inline bool operator>(const MessageType &a, const MessageType &b) {
  return a.id > b.id;
}
inline bool operator>=(const MessageType &a, const MessageType &b) {
  return a.id >= b.id;
}

/// Each RPC is defined via this struct.
///
/// `TRequest` and `TResponse` are required to be classes which have a static
/// member `TypeInfo` of `MessageType` type. This is used for proper
/// registration and deserialization of RPC types. Additionally, both `TRequest`
/// and `TResponse` are required to define a nested `Capnp` type, which
/// corresponds to the Cap'n Proto schema type, as well as defined the following
/// serialization functions:
///   * void Save(Capnp::Builder *, ...) const
///   * void Load(const Capnp::Reader &, ...)
template <typename TRequest, typename TResponse>
struct RequestResponse {
  using Request = TRequest;
  using Response = TResponse;
};

}  // namespace communication::rpc
