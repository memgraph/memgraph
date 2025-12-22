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

#include "common.hpp"

#include <nlohmann/json.hpp>

namespace r = std::ranges;

std::string GetAuthenticationJSON(const Credentials &creds) {
  nlohmann::json json_creds;
  json_creds["username"] = creds.username;
  json_creds["password"] = creds.passsword;
  return json_creds.dump();
}

void AssertLogMessage(std::string const &log_message) {
  const auto json_message = nlohmann::json::parse(log_message);
  if (json_message.contains("success")) {
    spdlog::info("Received auth message: {}", json_message.dump());
    AssertAuthMessage(json_message);
    return;
  }
  MG_ASSERT(json_message.at("event").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("event").get<std::string>() == "log", "Event is not equal to `log`!");
  MG_ASSERT(json_message.at("level").is_string(), "Level is not a string!");
  MG_ASSERT(r::count(kSupportedLogLevels, json_message.at("level")) == 1);
  MG_ASSERT(json_message.at("message").is_string(), "Message is not a string!");
}

void AssertAuthMessage(nlohmann::json const &json_message, bool const success) {
  MG_ASSERT(json_message.at("message").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("success").is_boolean(), "Success is not a boolean!");
  MG_ASSERT(json_message.at("success").template get<bool>() == success, "Success does not match expected!");
}

void AssertAuthMessage(std::string const &received_message, bool const success) {
  auto json_message = nlohmann::json::parse(received_message);
  AssertAuthMessage(json_message, false);
}
