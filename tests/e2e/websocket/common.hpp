// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/ssl/ssl_stream.hpp>
#include <boost/beast/websocket.hpp>
#include <json/json.hpp>
#include <mgclient.hpp>

#include "utils/logging.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;
namespace ssl = boost::asio::ssl;

constexpr std::array kSupportedLevels{"debug", "trace", "info", "warning", "error", "critical"};

struct Credentials {
  std::string_view username;
  std::string_view passsword;
};

std::string GetAuthenticationJSON(const Credentials &creds) {
  nlohmann::json json_creds;
  json_creds["username"] = creds.username;
  json_creds["password"] = creds.passsword;
  return json_creds.dump();
}

void Fail(beast::error_code ec, char const *what) { std::cerr << what << ": " << ec.message() << "\n"; }

std::unique_ptr<mg::Client> GetBoltClient(const uint16_t bolt_port, const bool use_ssl) {
  auto client = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  MG_ASSERT(client, "Failed to connect!");

  return client;
}

void CleanDatabase(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("MATCH (n) DETACH DELETE n;"));
  client->DiscardAll();
}

void AddUser(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("CREATE USER test IDENTIFIED BY 'testing';"));
  client->DiscardAll();
}

void AddVertex(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("CREATE ();"));
  client->DiscardAll();
}
void AddConnectedVertices(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("CREATE ()-[:TO]->();"));
  client->DiscardAll();
}

void RunQueries(std::unique_ptr<mg::Client> &mg_client) {
  CleanDatabase(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddConnectedVertices(mg_client);
  CleanDatabase(mg_client);
}

void AssertAuthMessage(auto &json_message, const bool success = true) {
  MG_ASSERT(json_message.at("message").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("success").is_boolean(), "Success is not a boolean!");
  MG_ASSERT(json_message.at("success").template get<bool>() == success, "Success does not match expected!");
}

void AssertLogMessage(const std::string &log_message) {
  const auto json_message = nlohmann::json::parse(log_message);
  if (json_message.contains("success")) {
    spdlog::info("Received auth message: {}", json_message.dump());
    AssertAuthMessage(json_message);
    return;
  }
  MG_ASSERT(json_message.at("event").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("event").get<std::string>() == "log", "Event is not equal to `log`!");
  MG_ASSERT(json_message.at("level").is_string(), "Level is not a string!");
  MG_ASSERT(std::ranges::count(kSupportedLevels, json_message.at("level")) > 0);
  MG_ASSERT(json_message.at("message").is_string(), "Message is not a string!");
}

template <typename TWebsocketClient>
void TestWebsocketWithoutAnyUsers(std::unique_ptr<mg::Client> &mg_client) {
  spdlog::info("Starting websocket connection without any users.");
  auto websocket_client = TWebsocketClient();
  websocket_client.Connect("127.0.0.1", "7444");

  RunQueries(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  const auto received_messages = websocket_client.GetReceivedMessages();
  spdlog::info("Received {} messages.", received_messages.size());
  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  for (const auto &log_message : received_messages) {
    AssertLogMessage(log_message);
  }
  spdlog::info("Finishing websocket connection without any users.");
}

template <typename TWebsocketClient>
void TestWebsocketWithAuthentication(std::unique_ptr<mg::Client> &mg_client) {
  spdlog::info("Starting websocket connection with users.");
  AddUser(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto websocket_client = TWebsocketClient({"test", "testing"});
  websocket_client.Connect("127.0.0.1", "7444");

  RunQueries(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  const auto received_messages = websocket_client.GetReceivedMessages();
  spdlog::info("Received {} messages.", received_messages.size());

  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  for (const auto &log_message : received_messages) {
    AssertLogMessage(log_message);
  }
  spdlog::info("Finishing websocket connection with users.");
}

template <typename TWebsocketClient>
void TestWebsocketWithoutBeingAuthorized(std::unique_ptr<mg::Client> &mg_client) {
  spdlog::info("Starting websocket connection with users but without being authenticated.");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto websocket_client = TWebsocketClient();
  websocket_client.Connect("127.0.0.1", "7444");

  RunQueries(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  const auto received_messages = websocket_client.GetReceivedMessages();
  spdlog::info("Received {} messages.", received_messages.size());

  MG_ASSERT(received_messages.size() < 2, "There is no more than one received message!");
  if (!received_messages.empty()) {
    auto json_message = nlohmann::json::parse(received_messages[0]);
    AssertAuthMessage(json_message, false);
  }
  spdlog::info("Finishing websocket connection with users but without being authenticated.");
}
