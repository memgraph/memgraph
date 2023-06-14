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
#include <optional>
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

inline constexpr std::array kSupportedLogLevels{"debug", "trace", "info", "warning", "error", "critical"};

struct Credentials {
  std::string_view username;
  std::string_view password;
};

inline void Fail(beast::error_code ec, char const *what) { std::cerr << what << ": " << ec.message() << "\n"; }

inline std::string GetAuthenticationJSON(const Credentials &creds) {
  nlohmann::json json_creds;
  json_creds["username"] = creds.username;
  json_creds["password"] = creds.password;
  return json_creds.dump();
}

template <bool ssl = false>
class Session : public std::enable_shared_from_this<Session<ssl>> {
  using std::enable_shared_from_this<Session<ssl>>::shared_from_this;

 public:
  explicit Session(net::io_context &ioc, ssl::context &ctx, std::vector<std::string> &expected_messages) requires(ssl)
      : resolver_(net::make_strand(ioc)), ws_(net::make_strand(ioc), ctx), received_messages_{expected_messages} {}

  explicit Session(net::io_context &ioc, std::vector<std::string> &expected_messages) requires(!ssl)
      : resolver_(net::make_strand(ioc)), ws_(net::make_strand(ioc)), received_messages_{expected_messages} {}

  template <typename... Args>
  explicit Session(Credentials creds, Args &&...args) : Session<ssl>(std::forward<Args>(args)...) {
    creds_.emplace(creds);
  }

  void Run(std::string_view host, std::string_view port) {
    host_ = host;
    resolver_.async_resolve(host, port, beast::bind_front_handler(&Session::OnResolve, shared_from_this()));
  }

  void OnResolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) {
      return Fail(ec, "resolve");
    }

    beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
    beast::get_lowest_layer(ws_).async_connect(results,
                                               beast::bind_front_handler(&Session::OnConnect, shared_from_this()));
  }

  void OnConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type ep) {
    if (ec) {
      return Fail(ec, "connect");
    }

    host_ = fmt::format("{}:{}", host_, ep.port());

    beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));

    if constexpr (ssl) {
      if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), host_.c_str())) {
        ec = beast::error_code(static_cast<int>(::ERR_get_error()), net::error::get_ssl_category());
        return Fail(ec, "connect");
      }
      ws_.next_layer().async_handshake(ssl::stream_base::client,
                                       beast::bind_front_handler(&Session::OnSSLHandshake, shared_from_this()));
    } else {
      ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

      ws_.set_option(websocket::stream_base::decorator([](websocket::request_type &req) {
        req.set(http::field::user_agent, std::string(BOOST_BEAST_VERSION_STRING) + " websocket-client-async");
      }));

      host_ = fmt::format("{}:{}", host_, ep.port());

      ws_.async_handshake(host_, "/", beast::bind_front_handler(&Session::OnHandshake, shared_from_this()));
    }
  }

  void OnHandshake(beast::error_code ec) {
    if (ec) {
      return Fail(ec, "handshake");
    }
    if (creds_) {
      ws_.async_write(net::buffer(GetAuthenticationJSON(*creds_)),
                      beast::bind_front_handler(&Session::OnWrite, shared_from_this()));
    } else {
      ws_.async_read(buffer_, beast::bind_front_handler(&Session::OnRead, shared_from_this()));
    }
  }

  void OnSSLHandshake(beast::error_code ec) {
    if (ec) {
      return Fail(ec, "ssl_handshake");
    }

    beast::get_lowest_layer(ws_).expires_never();
    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
    ws_.set_option(websocket::stream_base::decorator([](websocket::request_type &req) {
      req.set(http::field::user_agent, std::string(BOOST_BEAST_VERSION_STRING) + " websocket-client-async-ssl");
    }));

    ws_.async_handshake(host_, "/", beast::bind_front_handler(&Session::OnHandshake, shared_from_this()));
  }

  void OnWrite(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) {
      return Fail(ec, "write");
    }

    ws_.async_read(buffer_, beast::bind_front_handler(&Session::OnRead, shared_from_this()));
  }

  void OnRead(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    if (ec) {
      return Fail(ec, "read");
    }

    received_messages_.push_back(boost::beast::buffers_to_string(buffer_.data()));
    buffer_.clear();

    ws_.async_read(buffer_, beast::bind_front_handler(&Session::OnRead, shared_from_this()));
  }

  void OnClose(beast::error_code ec) {
    if (ec) {
      return Fail(ec, "close");
    }
  }

 private:
  using InternalStream = std::conditional_t<ssl, beast::ssl_stream<beast::tcp_stream>, beast::tcp_stream>;
  tcp::resolver resolver_;
  websocket::stream<InternalStream> ws_;
  beast::flat_buffer buffer_;
  std::string host_;
  std::vector<std::string> &received_messages_;
  std::optional<Credentials> creds_{std::nullopt};
};

std::unique_ptr<mg::Client> GetBoltClient(const uint16_t bolt_port, const bool use_ssl) {
  auto client = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  MG_ASSERT(client, "Failed to connect!");

  return client;
}

inline void CleanDatabase(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("MATCH (n) DETACH DELETE n;"));
  client->DiscardAll();
}

inline void AddUser(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("CREATE USER test IDENTIFIED BY 'testing';"));
  client->DiscardAll();
}

inline void AddVertex(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("CREATE ();"));
  client->DiscardAll();
}
inline void AddConnectedVertices(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("CREATE ()-[:TO]->();"));
  client->DiscardAll();
}

inline void RunQueries(std::unique_ptr<mg::Client> &mg_client) {
  CleanDatabase(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddVertex(mg_client);
  AddConnectedVertices(mg_client);
  CleanDatabase(mg_client);
}

inline void AssertAuthMessage(auto &json_message, const bool success = true) {
  MG_ASSERT(json_message.at("message").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("success").is_boolean(), "Success is not a boolean!");
  MG_ASSERT(json_message.at("success").template get<bool>() == success, "Success does not match expected!");
}

inline void AssertLogMessage(const std::string &log_message) {
  const auto json_message = nlohmann::json::parse(log_message);
  if (json_message.contains("success")) {
    spdlog::info("Received auth message: {}", json_message.dump());
    AssertAuthMessage(json_message);
    return;
  }
  MG_ASSERT(json_message.at("event").is_string(), "Event is not a string!");
  MG_ASSERT(json_message.at("event").get<std::string>() == "log", "Event is not equal to `log`!");
  MG_ASSERT(json_message.at("level").is_string(), "Level is not a string!");
  MG_ASSERT(std::ranges::count(kSupportedLogLevels, json_message.at("level")) == 1);
  MG_ASSERT(json_message.at("message").is_string(), "Message is not a string!");
}

template <typename TWebsocketClient>
void TestWebsocketWithoutAnyUsers(std::unique_ptr<mg::Client> &mg_client, const std::string_view monitoring_port) {
  spdlog::info("Starting websocket connection without any users.");
  auto websocket_client = TWebsocketClient();
  websocket_client.Connect("127.0.0.1", monitoring_port);

  RunQueries(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  websocket_client.AwaitClose();
  const auto received_messages = websocket_client.GetReceivedMessages();
  spdlog::info("Received {} messages.", received_messages.size());
  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  std::ranges::for_each(received_messages, AssertLogMessage);

  spdlog::info("Finishing websocket connection without any users.");
}

template <typename TWebsocketClient>
void TestWebsocketWithAuthentication(std::unique_ptr<mg::Client> &mg_client, const std::string_view monitoring_port) {
  spdlog::info("Starting websocket connection with users.");
  AddUser(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto websocket_client = TWebsocketClient({"test", "testing"});
  websocket_client.Connect("127.0.0.1", monitoring_port);

  RunQueries(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  websocket_client.AwaitClose();
  const auto received_messages = websocket_client.GetReceivedMessages();
  spdlog::info("Received {} messages.", received_messages.size());

  MG_ASSERT(!received_messages.empty(), "There are no received messages!");
  std::ranges::for_each(received_messages, AssertLogMessage);

  spdlog::info("Finishing websocket connection with users.");
}

template <typename TWebsocketClient>
void TestWebsocketWithoutBeingAuthorized(std::unique_ptr<mg::Client> &mg_client,
                                         const std::string_view monitoring_port) {
  spdlog::info("Starting websocket connection with users but without being authenticated.");
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto websocket_client = TWebsocketClient({"wrong", "credentials"});
  websocket_client.Connect("127.0.0.1", monitoring_port);

  RunQueries(mg_client);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  websocket_client.Close();
  websocket_client.AwaitClose();
  const auto received_messages = websocket_client.GetReceivedMessages();
  spdlog::info("Received {} messages.", received_messages.size());

  MG_ASSERT(received_messages.size() == 1, "There must be only one message received!");
  if (!received_messages.empty()) {
    auto json_message = nlohmann::json::parse(received_messages[0]);
    AssertAuthMessage(json_message, false);
  }
  spdlog::info("Finishing websocket connection with users but without being authenticated.");
}

template <typename TWebsocketClient>
void RunTestCases(std::unique_ptr<mg::Client> &mg_client, const std::string_view monitoring_port) {
  TestWebsocketWithoutAnyUsers<TWebsocketClient>(mg_client, monitoring_port);
  TestWebsocketWithAuthentication<TWebsocketClient>(mg_client, monitoring_port);
  TestWebsocketWithoutBeingAuthorized<TWebsocketClient>(mg_client, monitoring_port);
}
