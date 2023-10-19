// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/websocket/session.hpp"

#include <functional>
#include <memory>
#include <string>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <boost/asio/bind_executor.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/stream_traits.hpp>
#include <json/json.hpp>

#include "communication/context.hpp"
#include "communication/websocket/auth.hpp"
#include "utils/logging.hpp"

namespace memgraph::communication::websocket {
namespace {
void LogError(const boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Websocket session failed on {}: {}", what, ec.message());
}
}  // namespace

std::variant<Session::PlainWebSocket, Session::SSLWebSocket> Session::CreateWebSocket(tcp::socket &&socket,
                                                                                      ServerContext &context) {
  if (context.use_ssl()) {
    ssl_context_.emplace(context.context_clone());
    return Session::SSLWebSocket{std::move(socket), *ssl_context_};
  }

  return Session::PlainWebSocket{std::move(socket)};
}

Session::Session(tcp::socket &&socket, ServerContext &context, AuthenticationInterface &auth)
    : ws_(CreateWebSocket(std::move(socket), context)), strand_{boost::asio::make_strand(GetExecutor())}, auth_{auth} {}

bool Session::Run() {
  ExecuteForWebsocket([](auto &&ws) {
    ws.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));

    ws.set_option(boost::beast::websocket::stream_base::decorator([](boost::beast::websocket::response_type &res) {
      res.set(boost::beast::http::field::server, "Memgraph WS");
    }));
  });

  if (auto *ssl_ws = std::get_if<SSLWebSocket>(&ws_); ssl_ws != nullptr) {
    try {
      boost::beast::get_lowest_layer(*ssl_ws).expires_after(std::chrono::seconds(30));
      ssl_ws->next_layer().handshake(boost::asio::ssl::stream_base::server);
    } catch (const boost::system::system_error &e) {
      spdlog::warn("Failed on SSL handshake: {}", e.what());
      return false;
    }
  }

  auto result = ExecuteForWebsocket([](auto &&ws) -> bool {
    // Accept the websocket handshake
    boost::beast::error_code ec;
    ws.accept(ec);
    if (ec) {
      LogError(ec, "accept");
      return false;
    }
    return true;
  });

  if (!result) {
    return false;
  }

  authenticated_ = !auth_.HasAnyUsers();
  connected_.store(true, std::memory_order_relaxed);

  // run on the strand
  boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoRead(); });
  return true;
}

void Session::Write(std::shared_ptr<std::string> message) {
  boost::asio::dispatch(strand_, [message = std::move(message), shared_this = shared_from_this()]() mutable {
    if (!shared_this->connected_.load(std::memory_order_relaxed)) {
      return;
    }
    if (!shared_this->IsAuthenticated()) {
      return;
    }
    shared_this->messages_.push_back(std::move(message));
    if (shared_this->messages_.size() > 1) {
      return;
    }
    shared_this->DoWrite();
  });
}

bool Session::IsConnected() const { return connected_.load(std::memory_order_relaxed); }

void Session::DoWrite() {
  ExecuteForWebsocket([this](auto &&ws) {
    auto next_message = messages_.front();
    ws.async_write(boost::asio::buffer(*next_message),
                   boost::asio::bind_executor(
                       strand_, [message_string = std::move(next_message), shared_this = shared_from_this()](
                                    boost::beast::error_code ec, const size_t bytes_transferred) {
                         shared_this->OnWrite(ec, bytes_transferred);
                       }));
  });
}

void Session::OnWrite(boost::beast::error_code ec, size_t /*bytes_transferred*/) {
  messages_.pop_front();

  if (close_) {
    DoShutdown();
    return;
  }
  if (ec) {
    close_ = true;
    return LogError(ec, "write");
  }
  if (!messages_.empty()) {
    DoWrite();
  }
}

void Session::DoRead() {
  ExecuteForWebsocket([this](auto &&ws) {
    ws.async_read(buffer_, boost::asio::bind_executor(strand_, std::bind_front(&Session::OnRead, shared_from_this())));
  });
  ;
}

void Session::DoClose() {
  ExecuteForWebsocket([this](auto &&ws) mutable {
    ws.async_close(boost::beast::websocket::close_code::normal,
                   boost::asio::bind_executor(strand_, [shared_this = shared_from_this()](boost::beast::error_code ec) {
                     shared_this->OnClose(ec);
                   }));
  });
}

void Session::OnClose(boost::beast::error_code ec) {
  connected_.store(false, std::memory_order_relaxed);
  if (ec) {
    return LogError(ec, "close");
  }
}

utils::BasicResult<std::string> Session::Authorize(const nlohmann::json &creds) {
  if (!auth_.Authenticate(creds.at("username").get<std::string>(), creds.at("password").get<std::string>())) {
    return {"Authentication failed!"};
  }
#ifdef MG_ENTERPRISE
  if (!auth_.HasUserPermission(creds.at("username").get<std::string>(), auth::Permission::WEBSOCKET)) {
    return {"Authorization failed!"};
  }
#endif
  return {};
}

void Session::OnRead(const boost::beast::error_code ec, const size_t /*bytes_transferred*/) {
  if (ec == boost::beast::websocket::error::closed) {
    DoShutdown();
    return;
  }

  if (ec) {
    LogError(ec, "read");
    return;
  }

  if (!IsAuthenticated()) {
    auto response = nlohmann::json();
    auto auth_failed = [this, &response](const std::string &message) {
      response["success"] = false;
      response["message"] = message;
      MG_ASSERT(messages_.empty());
      messages_.push_back(std::make_shared<std::string>(response.dump()));
      close_ = true;
      DoWrite();
    };
    try {
      const auto creds = nlohmann::json::parse(boost::beast::buffers_to_string(buffer_.data()));
      buffer_.consume(buffer_.size());

      if (const auto result = Authorize(creds); result.HasError()) {
        std::invoke(auth_failed, result.GetError());
        return;
      }
      response["success"] = true;
      response["message"] = "User has been successfully authenticated!";
      MG_ASSERT(messages_.empty());
      authenticated_ = true;
      messages_.push_back(std::make_shared<std::string>(response.dump()));
      DoWrite();
    } catch (const nlohmann::json::out_of_range &out_of_range) {
      const auto err_msg = fmt::format("Invalid JSON for authentication received: {}!", out_of_range.what());
      spdlog::error(err_msg);
      std::invoke(auth_failed, err_msg);
      return;
    } catch (const nlohmann::json::parse_error &parse_error) {
      const auto err_msg = fmt::format("Cannot parse JSON for WebSocket authentication: {}!", parse_error.what());
      spdlog::error(err_msg);
      std::invoke(auth_failed, err_msg);
      return;
    }
  }

  DoRead();
}

bool Session::IsAuthenticated() const { return authenticated_; }

void Session::DoShutdown() {
  std::visit(utils::Overloaded{[this](SSLWebSocket &ssl_ws) {
                                 boost::beast::get_lowest_layer(ssl_ws).expires_after(std::chrono::seconds(30));
                                 ssl_ws.next_layer().async_shutdown(
                                     [shared_this = shared_from_this()](boost::beast::error_code ec) {
                                       if (ec) {
                                         LogError(ec, "shutdown");
                                       }
                                       shared_this->DoClose();
                                     });
                               },
                               [this](auto && /* plain_ws */) { DoClose(); }},
             ws_);
}

}  // namespace memgraph::communication::websocket
