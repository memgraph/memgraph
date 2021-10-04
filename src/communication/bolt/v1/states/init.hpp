// Copyright 2021 Memgraph Ltd.
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

#include <fmt/format.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/exceptions.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"

namespace communication::bolt {

namespace detail {
template <typename TSession>
std::optional<Value> StateInitRunV1(TSession &session, const Marker marker) {
  if (UNLIKELY(marker != Marker::TinyStruct2)) {
    spdlog::trace("Expected TinyStruct2 marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
    spdlog::trace(
        "The client sent malformed data, but we are continuing "
        "because the official Neo4j Java driver sends malformed "
        "data. D'oh!");
    // TODO: this should be uncommented when the Neo4j Java driver is fixed
    // return State::Close;
  }

  Value client_name;
  if (!session.decoder_.ReadValue(&client_name, Value::Type::String)) {
    spdlog::trace("Couldn't read client name!");
    return std::nullopt;
  }

  Value metadata;
  if (!session.decoder_.ReadValue(&metadata, Value::Type::Map)) {
    spdlog::trace("Couldn't read metadata!");
    return std::nullopt;
  }

  spdlog::info("Client connected '{}'", client_name.ValueString());

  return metadata;
}

template <typename TSession>
std::optional<Value> StateInitRunV4(TSession &session, const Marker marker) {
  if (UNLIKELY(marker != Marker::TinyStruct1)) {
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
    spdlog::trace(
        "The client sent malformed data, but we are continuing "
        "because the official Neo4j Java driver sends malformed "
        "data. D'oh!");
    // TODO: this should be uncommented when the Neo4j Java driver is fixed
    // return State::Close;
  }

  Value metadata;
  if (!session.decoder_.ReadValue(&metadata, Value::Type::Map)) {
    spdlog::trace("Couldn't read metadata!");
    return std::nullopt;
  }

  const auto &data = metadata.ValueMap();
  if (!data.count("user_agent")) {
    spdlog::warn("The client didn't supply the user agent!");
    return std::nullopt;
  }

  spdlog::info("Client connected '{}'", data.at("user_agent").ValueString());

  return metadata;
}
}  // namespace detail

/**
 * Init state run function.
 * This function runs everything to initialize a Bolt session with the client.
 * @param session the session that should be used for the run.
 */
template <typename Session>
State StateInitRun(Session &session) {
  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    spdlog::trace("Missing header data!");
    return State::Close;
  }

  if (UNLIKELY(signature == Signature::Noop && session.version_.major == 4 && session.version_.minor == 1)) {
    SPDLOG_DEBUG("Received NOOP message");
    return State::Init;
  }

  if (UNLIKELY(signature != Signature::Init)) {
    spdlog::trace("Expected Init signature, but received 0x{:02X}!", utils::UnderlyingCast(signature));
    return State::Close;
  }

  auto maybeMetadata =
      session.version_.major == 1 ? detail::StateInitRunV1(session, marker) : detail::StateInitRunV4(session, marker);

  if (!maybeMetadata) {
    return State::Close;
  }

  // Get authentication data.
  std::string username;
  std::string password;
  auto &data = maybeMetadata->ValueMap();
  if (!data.count("scheme")) {
    spdlog::warn("The client didn't supply authentication information!");
    return State::Close;
  }
  if (data["scheme"].ValueString() == "basic") {
    if (!data.count("principal") || !data.count("credentials")) {
      spdlog::warn("The client didn't supply authentication information!");
      return State::Close;
    }
    username = data["principal"].ValueString();
    password = data["credentials"].ValueString();
  } else if (data["scheme"].ValueString() != "none") {
    spdlog::warn("Unsupported authentication scheme: {}", data["scheme"].ValueString());
    return State::Close;
  }

  // Authenticate the user.
  if (!session.Authenticate(username, password)) {
    if (!session.encoder_.MessageFailure(
            {{"code", "Memgraph.ClientError.Security.Unauthenticated"}, {"message", "Authentication failure"}})) {
      spdlog::trace("Couldn't send failure message to the client!");
    }
    // Throw an exception to indicate to the network stack that the session
    // should be closed and cleaned up.
    throw SessionClosedException("The client is not authenticated!");
  }

  // Return success.
  {
    bool success_sent = false;
    // Neo4j's Java driver 4.1.1+ requires connection_id.
    // The only usage in the mentioned version is for logging purposes.
    // Because it's not critical for the regular usage of the driver
    // we send a hardcoded value for now.
    std::map<std::string, Value> metadata{{"connection_id", "bolt-1"}};
    if (auto server_name = session.GetServerNameForInit(); server_name) {
      metadata.insert({"server", *server_name});
    }
    success_sent = session.encoder_.MessageSuccess(metadata);
    if (!success_sent) {
      spdlog::trace("Couldn't send success message to the client!");
      return State::Close;
    }
  }

  return State::Idle;
}
}  // namespace communication::bolt
