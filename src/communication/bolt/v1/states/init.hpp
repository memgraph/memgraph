// Copyright 2026 Memgraph Ltd.
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
#include <set>
#include <utility>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/exceptions.hpp"
#include "flags/auth.hpp"
#include "flags/coord_flag_env_handler.hpp"
#include "license/license.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::communication::bolt {

namespace details {

template <typename TSession>
void HandleAuthFailure(TSession &session, std::string const &message = "Authentication failure") {
  if (!session.encoder_.MessageFailure(
          {{"code", "Memgraph.ClientError.Security.Unauthenticated"}, {"message", message}})) {
    spdlog::trace("Couldn't send failure message to the client!");
  }
  // Throw an exception to indicate to the network stack that the session
  // should be closed and cleaned up.
  throw SessionClosedException("The client is not authenticated!");
}

template <typename TSession>
void HandleResourceFailure(TSession &session) {
  if (!session.encoder_.MessageFailure({{"code", "Memgraph.ClientError.Statement.SessionLimitReached"},
                                        {"message", "User reached the limit of concurent sessions"}})) {
    spdlog::trace("Couldn't send failure message to the client!");
  }
  // Throw an exception to indicate to the network stack that the session
  // should be closed and cleaned up.
  throw SessionClosedException("The user cannot connect due to the imposed session limit!");
}

template <typename TSession>
std::optional<State> BasicAuthentication(TSession &session, memgraph::communication::bolt::map_t &data) {
  auto principal_it = data.find("principal");
  if (principal_it == data.end() || !principal_it->second.IsString()) {  // Special case principal = ""
    spdlog::warn("The client didn't supply the principal field! Trying with \"\"...");
    data["principal"] = "";
  }
  auto credentials_it = data.find("credentials");
  if (credentials_it == data.end() || !credentials_it->second.IsString()) {  // Special case credentials = ""
    spdlog::warn("The client didn't supply the credentials field! Trying with \"\"...");
    data["credentials"] = "";
  }
  auto username = data["principal"].ValueString();
  auto password = data["credentials"].ValueString();

  const auto auth_res = session.Authenticate(username, password);
  if (!auth_res) {
    switch (auth_res.error()) {
      case AuthFailure::kGeneric:
        HandleAuthFailure(session);
        break;
      case AuthFailure::kResourceBound:
        HandleResourceFailure(session);
        break;
    }
  }

  return std::nullopt;
}

// Extracts the SSO scheme and identity provider response from the handshake data.
// Returns std::nullopt (and logs a warning) if either field is missing or not a string.
inline std::optional<std::pair<std::string, std::string>> ExtractSSOCredentials(
    memgraph::communication::bolt::map_t &data) {
  auto cred_it = data.find("credentials");
  if (cred_it == data.end() || !cred_it->second.IsString()) {
    spdlog::warn("The client didn't supply the SSO token!");
    return std::nullopt;
  }
  auto scheme_it = data.find("scheme");
  if (scheme_it == data.end() || !scheme_it->second.IsString()) {
    spdlog::warn("The client didn't supply a valid SSO scheme!");
    return std::nullopt;
  }
  return std::make_pair(scheme_it->second.ValueString(), cred_it->second.ValueString());
}

template <typename TSession>
std::optional<State> SSOAuthentication(TSession &session, memgraph::communication::bolt::map_t &data) {
  auto credentials = ExtractSSOCredentials(data);
  if (!credentials) {
    return State::Close;
  }
  const auto &[scheme, identity_provider_response] = *credentials;
  const auto auth_res = session.SSOAuthenticate(scheme, identity_provider_response);
  if (!auth_res) {
    switch (auth_res.error()) {
      case AuthFailure::kGeneric:
        HandleAuthFailure(session);
        break;
      case AuthFailure::kResourceBound:
        HandleResourceFailure(session);
        break;
    }
  }
  return std::nullopt;
}

#ifdef MG_ENTERPRISE
template <typename TSession>
std::optional<State> CoordinatorSSOAuthentication(TSession &session, memgraph::communication::bolt::map_t &data) {
  auto credentials = ExtractSSOCredentials(data);
  if (!credentials) {
    return State::Close;
  }
  const auto &[scheme, identity_provider_response] = *credentials;
  const auto auth_res = session.CoordinatorSSOAuthenticate(scheme, identity_provider_response);
  if (!auth_res) {
    // Rejected: invalid token, a role the module returned that doesn't exist on the coordinator, or a missing
    // enterprise license. HandleAuthFailure sends the failure message and throws to close the connection.
    HandleAuthFailure(session,
                      "SSO authentication failed: invalid token, an unknown role, or a missing enterprise license.");
  }
  return std::nullopt;
}
#endif

template <typename TSession>
std::optional<State> AuthenticateUser(TSession &session, Value &metadata) {
  // Get authentication data.
  // From neo4j driver v4.4, fields that have a default value are not sent.
  // In order to have back-compatibility, the missing fields will be added.
  auto &data = metadata.ValueMap();
  auto scheme_it = data.find("scheme");
  if (scheme_it == data.end() || !scheme_it->second.IsString()) {  // Special case auth=None
    spdlog::warn("The client didn't supply the authentication scheme! Trying with \"none\"...");
    data["scheme"] = "none";
  }

  auto scheme_in_module_mappings = [](std::string_view auth_scheme) {
    if (auth_scheme == "basic") {  // "Basic" refers to username + password auth, as opposed to SSO
      return false;
    }
    for (const auto &mapping : utils::Split(FLAGS_auth_module_mappings, ";")) {
      const auto module_and_scheme = utils::Split(mapping, ":");
      // An empty element (e.g. from a trailing ';' in the flag) splits into an empty vector.
      if (module_and_scheme.empty()) {
        continue;
      }
      if (auth_scheme == utils::Trim(module_and_scheme[0])) {
        return true;
      }
    }
    return false;
  };

  const auto &schema = data["scheme"].ValueString();

#ifdef MG_ENTERPRISE
  if (auto const &coordination_setup = flags::CoordinationSetupInstance(); coordination_setup.IsCoordinator()) {
    // Coordinator auth: when no SSO module is configured, basic/none is a passthrough (credentials ignored, session
    // keeps full COORDINATOR_WRITE, no license required). Once SSO is configured (--auth-module-mappings non-empty),
    // basic/none is denied so the credential-less passthrough can't bypass the SSO privilege model -- but only while
    // SSO can actually grant a privileged session. It can't when the enterprise license is invalid (SSO then rejects
    // every login) or when the committed role set has no COORDINATOR_WRITE role (SSO validates the module's roles
    // against that set, and only a WRITE role can create the first administrator). In either case basic stays open as
    // the break-glass path so an admin is never permanently locked out -- covering a fresh boot with mappings and no
    // roles, and dropping the last writable role on a live cluster. A transient leader outage leaves the role set
    // unknown; that case is fail-closed (basic denied), matching the SSO path, since it is temporary and SSO is
    // unavailable then too. An SSO scheme present in --auth-module-mappings runs the coordinator SSO path
    // (enterprise-gated); any other/unknown scheme is rejected.
    const bool sso_configured = !FLAGS_auth_module_mappings.empty();
    if (schema == "basic" || schema == "none") {
      if (sso_configured) {
        bool deny_basic = license::global_license_checker.IsEnterpriseValidFast();
        if (deny_basic) {
          // nullopt (leader unreachable / no coordinator state) => keep basic denied (fail-closed).
          deny_basic = session.CoordinatorHasWritableRole().value_or(true);
        }
        if (deny_basic) {
          spdlog::warn(
              "Basic/none authentication is disabled on this coordinator because SSO is configured with a valid "
              "license and a COORDINATOR_WRITE role exists; connect with an SSO scheme listed in the "
              "auth-module-mappings flag.");
          HandleAuthFailure(session,
                            "Basic authentication is disabled on this coordinator because SSO is configured; connect "
                            "with an SSO scheme listed in the auth-module-mappings flag.");
          return State::Close;
        }
        spdlog::warn(
            "Allowing basic-auth passthrough on this coordinator as a break-glass path: SSO can't currently grant a "
            "privileged session (invalid enterprise license, or no COORDINATOR_WRITE role in the committed role "
            "set).");
      }
      session.CoordinatorPassthroughAuthenticate();
      return std::nullopt;
    }
    if (scheme_in_module_mappings(schema)) {
      return CoordinatorSSOAuthentication(session, data);
    }
    spdlog::warn(
        "The \"{}\" authentication scheme isn't supported on coordinators: connect with basic auth or an SSO scheme "
        "listed in the auth-module-mappings flag.",
        schema);
    HandleAuthFailure(
        session,
        fmt::format("The \"{}\" authentication scheme isn't supported on this coordinator; connect with basic auth or "
                    "an SSO scheme listed in the auth-module-mappings flag.",
                    schema));
    return State::Close;
  }
#endif

  if (schema == "basic" || schema == "none") {
    return BasicAuthentication(session, data);
  }
  if (scheme_in_module_mappings(schema)) {
    return SSOAuthentication(session, data);
  }

  spdlog::warn(
      "The \"{}\" authentication scheme doesn’t have an associated single sign-on module in the auth-module-mappings "
      "flag or isn’t otherwise supported",
      schema);
  HandleAuthFailure(session);

  return State::Close;
}

template <typename TSession>
std::optional<Value> GetMetadataV1(TSession &session, const Marker marker) {
  if (marker != Marker::TinyStruct2) [[unlikely]] {
    spdlog::trace("Expected TinyStruct2 marker, but received 0x{:02X}!", std::to_underlying(marker));
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
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", std::to_underlying(marker));
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
  auto user_agent_it = data.find("user_agent");
  if (user_agent_it == data.end() || !user_agent_it->second.IsString()) {
    spdlog::warn("The client didn't supply the user agent!");
    return std::nullopt;
  }

  spdlog::info("Client connected '{}'", user_agent_it->second.ValueString());

  return metadata;
}

template <typename TSession>
std::optional<Value> GetInitDataV5(TSession &session, const Marker marker) {
  if (marker != Marker::TinyStruct1) [[unlikely]] {
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", std::to_underlying(marker));
    return std::nullopt;
  }

  Value metadata;
  if (!session.decoder_.ReadValue(&metadata, Value::Type::Map)) {
    spdlog::trace("Couldn't read metadata!");
    return std::nullopt;
  }

  const auto &data = metadata.ValueMap();
  auto user_agent_it = data.find("user_agent");
  if (user_agent_it == data.end() || !user_agent_it->second.IsString()) {
    spdlog::warn("The client didn't supply the user agent!");
    return std::nullopt;
  }

  spdlog::info("Client connected '{}'", user_agent_it->second.ValueString());

  return metadata;
}

template <typename TSession>
std::optional<Value> GetAuthDataV5(TSession &session, const Marker marker) {
  if (marker != Marker::TinyStruct1) [[unlikely]] {
    spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", std::to_underlying(marker));
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
  map_t metadata{{"connection_id", "bolt-1"}};
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
    spdlog::trace("Expected Init signature, but received 0x{:02X}!", std::to_underlying(signature));
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
    spdlog::trace("Expected Init signature, but received 0x{:02X}!", std::to_underlying(signature));
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
      spdlog::trace("Expected TinyStruct1 marker, but received 0x{:02X}!", std::to_underlying(marker));
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

  spdlog::trace("Expected Init signature, but received 0x{:02X}!", std::to_underlying(signature));
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
