#pragma once

#include <cstdint>
#include <memory>

#include "utils/typeinfo.hpp"

namespace communication::rpc {

using MessageSize = uint32_t;

/// Each RPC is defined via this struct.
///
/// `TRequest` and `TResponse` are required to be classes which have a static
/// member `kType` of `utils::TypeInfo` type. This is used for proper
/// registration and deserialization of RPC types. Additionally, both `TRequest`
/// and `TResponse` are required to define a nested `Capnp` type, which
/// corresponds to the Cap'n Proto schema type, as well as defined the following
/// serialization functions:
///   * void Save(const TRequest|TResponse &, Capnp::Builder *, ...)
///   * void Load(const Capnp::Reader &, ...)
template <typename TRequest, typename TResponse>
struct RequestResponse {
  using Request = TRequest;
  using Response = TResponse;
};

}  // namespace communication::rpc
