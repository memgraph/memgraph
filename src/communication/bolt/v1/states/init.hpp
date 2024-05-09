// Copyright 2024 Memgraph Ltd.
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

#include <fmt/core.h>
#include <fmt/format.h>
#include <optional>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/exceptions.hpp"
#include "communication/metrics.hpp"
#include "spdlog/spdlog.h"
#include "utils/likely.hpp"
#include "utils/logging.hpp"

namespace memgraph::communication::bolt {

namespace details {

template <typename TSession>
void HandleAuthFailure(TSession &session) {
  if (!session.encoder_.MessageFailure(
          {{"code", "Memgraph.ClientError.Security.Unauthenticated"}, {"message", "Authentication failure"}})) {
    spdlog::trace("Couldn't send failure message to the client!");
  }
  // Throw an exception to indicate to the network stack that the session
  // should be closed and cleaned up.
  throw SessionClosedException("The client is not authenticated!");
}

template <typename TSession>
std::optional<State> BasicAuthentication(TSession &session, std::map<std::string, Value> &data) {
  if (!data.contains("principal")) {  // Special case principal = ""
    spdlog::warn("The client didn't supply the principal field! Trying with \"\"...");
    data["principal"] = "";
  }
  if (!data.contains("credentials")) {  // Special case credentials = ""
    spdlog::warn("The client didn't supply the credentials field! Trying with \"\"...");
    data["credentials"] = "";
  }
  auto username = data["principal"].ValueString();
  auto password = data["credentials"].ValueString();

  if (!session.Authenticate(username, password)) {
    HandleAuthFailure(session);
  }

  return std::nullopt;
}

template <typename TSession>
std::optional<State> CustomAuthentication(TSession &session, std::map<std::string, Value> &data) {
  if (!data.contains("response")) {
    spdlog::warn("The client didn’t supply the SAML response!");
    return State::Close;
  }

  auto scheme = data["scheme"].ValueString();
  auto identity_provider_response = data["credentials"].ValueString();
  if (!session.Authenticate(identity_provider_response)) {
    HandleAuthFailure(session);
  }
  return std::nullopt;
}

template <typename TSession>
std::optional<State> AuthenticateUser(TSession &session, Value &metadata) {
  // Get authentication data.
  // From neo4j driver v4.4, fields that have a default value are not sent.
  // In order to have back-compatibility, the missing fields will be added.

  auto &data = metadata.ValueMap();
  if (data.empty()) {  // Special case auth=None
    spdlog::warn("The client didn't supply the authentication scheme! Trying with \"none\"...");
    data["scheme"] = "none";
  }

  if (data["scheme"].ValueString() == "basic" || data["scheme"].ValueString() == "none") {
    return BasicAuthentication(session, data);
  } else if (data["scheme"].ValueString() == "custom") {
    return CustomAuthentication(session, data);
  }

  spdlog::warn("Unsupported authentication scheme: {}", data["scheme"].ValueString());
  return State::Close;
}

template <typename TSession>
std::optional<Value> GetMetadataV1(TSession &session, const Marker marker) {
  if (marker != Marker::TinyStruct2) [[unlikely]] {
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
std::optional<Value> GetMetadataV4(TSession &session, const Marker marker) {
  if (marker != Marker::TinyStruct1) [[unlikely]] {
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

  auto &data = metadata.ValueMap();
  if (!data.contains("user_agent")) {
    spdlog::warn("The client didn't supply the user agent!");
    return std::nullopt;
  }

  spdlog::info("Client connected '{}'", data.at("user_agent").ValueString());

  return metadata;
}

template <typename TSession>
std::optional<Value> GetInitDataV5(TSession &session, const Marker marker) {
  if (marker != Marker::TinyStruct1) [[unlikely]] {
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
    return std::nullopt;
  }

  Value metadata;
  if (!session.decoder_.ReadValue(&metadata, Value::Type::Map)) {
    spdlog::trace("Couldn't read metadata!");
    return std::nullopt;
  }

  const auto &data = metadata.ValueMap();
  if (!data.contains("user_agent")) {
    spdlog::warn("The client didn't supply the user agent!");
    return std::nullopt;
  }

  spdlog::info("Client connected '{}'", data.at("user_agent").ValueString());

  return metadata;
}

template <typename TSession>
std::optional<Value> GetAuthDataV5(TSession &session, const Marker marker) {
  if (marker != Marker::TinyStruct1) [[unlikely]] {
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
    return std::nullopt;
  }

  Value metadata;
  if (!session.decoder_.ReadValue(&metadata, Value::Type::Map)) {
    spdlog::trace("Couldn't read metadata!");
    return std::nullopt;
  }

  return metadata;
}

template <typename TSession>
State SendSuccessMessage(TSession &session) {
  // Neo4j's Java driver 4.1.1+ requires connection_id.
  // The only usage in the mentioned version is for logging purposes.
  // Because it's not critical for the regular usage of the driver
  // we send a hardcoded value for now.
  std::map<std::string, Value> metadata{{"connection_id", "bolt-1"}};
  if (auto server_name = session.GetServerNameForInit(); server_name) {
    metadata.insert({"server", std::move(*server_name)});
  }
  bool success_sent = session.encoder_.MessageSuccess(metadata);
  if (!success_sent) {
    spdlog::trace("Couldn't send success message to the client!");
    return State::Close;
  }

  return State::Idle;
}

template <typename TSession>
State StateInitRunV1(TSession &session, const Marker marker, const Signature signature) {
  if (signature != Signature::Init) [[unlikely]] {
    spdlog::trace("Expected Init signature, but received 0x{:02X}!", utils::UnderlyingCast(signature));
    return State::Close;
  }

  auto maybeMetadata = GetMetadataV1(session, marker);

  if (!maybeMetadata) {
    return State::Close;
  }
  if (auto result = AuthenticateUser(session, *maybeMetadata)) {
    return result.value();
  }

  // Register session to metrics
  RegisterNewSession(session, *maybeMetadata);

  return SendSuccessMessage(session);
}

template <typename TSession, int bolt_minor = 0>
State StateInitRunV4(TSession &session, Marker marker, Signature signature) {
  if constexpr (bolt_minor > 0) {
    if (signature == Signature::Noop) [[unlikely]] {
      SPDLOG_DEBUG("Received NOOP message");
      return State::Init;
    }
  }

  if (signature != Signature::Init) [[unlikely]] {
    spdlog::trace("Expected Init signature, but received 0x{:02X}!", utils::UnderlyingCast(signature));
    return State::Close;
  }

  auto maybeMetadata = GetMetadataV4(session, marker);

  if (!maybeMetadata) {
    return State::Close;
  }
  if (auto result = AuthenticateUser(session, *maybeMetadata)) {
    return result.value();
  }

  // Register session to metrics
  RegisterNewSession(session, *maybeMetadata);

  return SendSuccessMessage(session);
}

template <typename TSession>
State StateInitRunV5(TSession &session, Marker marker, Signature signature) {
  if (signature == Signature::Noop) [[unlikely]] {
    SPDLOG_DEBUG("Received NOOP message");
    return State::Init;
  }

  if (signature == Signature::Init) {
    auto maybeMetadata = GetInitDataV5(session, marker);

    if (!maybeMetadata) {
      return State::Close;
    }

    if (SendSuccessMessage(session) == State::Close) {
      return State::Close;
    }

    // Register session to metrics
    TouchNewSession(session, *maybeMetadata);

    // Stay in Init
    return State::Init;
  }

  if (signature == Signature::LogOn) {
    if (marker != Marker::TinyStruct1) [[unlikely]] {
      spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", utils::UnderlyingCast(marker));
      spdlog::trace(
          "The client sent malformed data, but we are continuing "
          "because the official Neo4j Java driver sends malformed "
          "data. D'oh!");
      return State::Close;
    }

    auto maybeMetadata = GetAuthDataV5(session, marker);
    if (!maybeMetadata) {
      return State::Close;
    }
    auto result = AuthenticateUser(session, *maybeMetadata);
    if (result) {
      spdlog::trace("Failed to authenticate, closing connection...");
      return State::Close;
    }

    if (SendSuccessMessage(session) == State::Close) {
      return State::Close;
    }

    // Register session to metrics
    UpdateNewSession(session, *maybeMetadata);

    return State::Idle;
  }

  spdlog::trace("Expected Init signature, but received 0x{:02X}!", utils::UnderlyingCast(signature));
  return State::Close;
}
}  // namespace details

/**
 * Init state run function.
 * This function runs everything to initialize a Bolt session with the client.
 * @param session the session that should be used for the run.
 */
template <typename TSession>
State StateInitRun(TSession &session) {
  DMG_ASSERT(!session.encoder_buffer_.HasData(), "There should be no data to write in this state");

  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    spdlog::trace("Missing header data!");
    return State::Close;
  }

  switch (session.version_.major) {
    case 1: {
      return details::StateInitRunV1<TSession>(session, marker, signature);
    }
    case 4: {
      if (session.version_.minor > 0) {
        return details::StateInitRunV4<TSession, 1>(session, marker, signature);
      }
      return details::StateInitRunV4<TSession>(session, marker, signature);
    }
    case 5: {
      return details::StateInitRunV5<TSession>(session, marker, signature);
    }
  }
  spdlog::trace("Unsupported bolt version:{}.{})!", session.version_.major, session.version_.minor);
  return State::Close;
}
}  // namespace memgraph::communication::bolt
