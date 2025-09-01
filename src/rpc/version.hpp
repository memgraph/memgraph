// Copyright 2025 Memgraph Ltd.
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

namespace memgraph::rpc {

// Extend this enum when changing protocol and if you need ProtocolMessageHeader
enum ProtocolVersion : uint8_t {
  // versioning of RPC was/will be introduced in 2.13
  // We start the versioning with a strange number, to radically reduce the
  // probability of accidental match/conformance with pre 2.13 versions
  V1,
  // TypeId has been changed, they were not stable
  // Added stable numbering for replication types to be in
  // 2000-2999 range. We shouldn't need to version bump again
  // for any TypeIds that get added.
  V2,
  // To each RPC main uuid was added
  V3,
  // The order of metadata and data deltas has been changed
  // It is now possible that both can be sent in one commit
  // this is due to auto index creation
  V4,
  // Moved coordinator section in TypeId.
  V5,
  // Added RPC versioning support. Both request and response messages serialize kVersion. The goal is to enable
  // no-downtime
  // upgrade from v5 to new versions. Upgrade from v4 to v5 will be breaking in a sense that ISSU cannot be performed
  // without downtime. When serializing request, we first write protocol version then request id, followed by request
  // version. Same for response. If you change the request version, the response needs to be bumped too, i.e. you should
  // always introduce both new response and new request even in situations when only request or response get changed.
  V6
};

constexpr auto current_protocol_version = ProtocolVersion::V6;

struct ProtocolMessageHeader {
  // protocol version read in the message
  ProtocolVersion protocol_version;
  utils::TypeId message_id{utils::TypeId::UNKNOWN};
  uint64_t message_version{
      0};  // request/response version. 0 is safe default because 1 is the first version for all messages
};

// @throws UnsupportedRpcVersionException
inline auto LoadMessageHeader(slk::Reader *reader) -> ProtocolMessageHeader {
  ProtocolMessageHeader header;
  slk::Load(&header.protocol_version, reader);
  switch (header.protocol_version) {
    case V1:
    case V2:
    case V3:
    case V4:
    case V5:
      slk::Load(&header.message_id, reader);
      header.message_version = 1;  // default
      break;
    case V6:
      slk::Load(&header.message_id, reader);
      slk::Load(&header.message_version, reader);
      break;
    default:
      throw UnsupportedRpcVersionException();
  }
  return header;
}

inline void SaveMessageHeader(ProtocolMessageHeader const &message_header, slk::Builder *builder) {
  slk::Save(message_header.protocol_version, builder);
  slk::Save(message_header.message_id, builder);
  slk::Save(message_header.message_version, builder);
}

}  // namespace memgraph::rpc
