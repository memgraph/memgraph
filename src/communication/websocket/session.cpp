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

#include "communication/websocket/session.hpp"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/stream_traits.hpp>
#include <nlohmann/json.hpp>

#include "communication/context.hpp"
#include "communication/websocket/auth.hpp"
#include "utils/logging.hpp"

namespace memgraph::communication::websocket {
namespace {
// A client that stops reading must not make the server hold its log backlog forever, so the queue is capped and the
// oldest queued message is dropped for each new one. Overflow is reported from a posted handler rather than inline:
// Write can run inside the logging sink that feeds it, and a nested log call there would deadlock on the sink's lock.
constexpr size_t kMaxQueuedMessages = 65536;

void LogError(const boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Websocket session failed on {}: {}", what, ec.message());
}
}  // namespace

std::variant<Session::PlainWebSocket, Session::SSLWebSocket> Session::CreateWebSocket(tcp::socket &&socket,
                                                                                      ServerContext &context) {
  if (context.use_ssl()) {
    ssl_context_ = context.context_clone();
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

  authenticated_ = !auth_.AccessControlled();
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
    if (shared_this->messages_.size() >= kMaxQueuedMessages) {
      shared_this->messages_.pop_front();
      if (!std::exchange(shared_this->overflow_reported_, true)) {
        boost::asio::post(shared_this->strand_, [] {
          spdlog::warn("Websocket client is not keeping up; dropping the oldest queued log messages");
        });
      }
    }
    shared_this->messages_.push_back(std::move(message));
    if (!shared_this->in_flight_) {
      shared_this->DoWrite();
    }
  });
}

bool Session::IsConnected() const { return connected_.load(std::memory_order_relaxed); }

void Session::DoWrite() {
  in_flight_ = std::move(messages_.front());
  messages_.pop_front();
  ExecuteForWebsocket([this](auto &&ws) {
    ws.async_write(
        boost::asio::buffer(*in_flight_),
        boost::asio::bind_executor(
            strand_, [shared_this = shared_from_this()](boost::beast::error_code ec, const size_t bytes_transferred) {
              shared_this->OnWrite(ec, bytes_transferred);
            }));
  });
}

void Session::OnWrite(boost::beast::error_code ec, size_t /*bytes_transferred*/) {
  in_flight_.reset();
  if (!connected_.load(std::memory_order_relaxed)) {
    return;
  }
  if (ec) {
    Disconnect();
    return LogError(ec, "write");
  }
  if (close_) {
    DoShutdown();
    return;
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
  messages_.clear();
  if (ec) {
    return LogError(ec, "close");
  }
}

// Tears the session down after a transport error. Nothing can be delivered any more, so the backlog is released,
// further messages are refused and the listener drops the session on its next accept. Closing the socket cancels the
// outstanding read, whose completion is then ignored.
void Session::Disconnect() {
  connected_.store(false, std::memory_order_relaxed);
  messages_.clear();
  ExecuteForWebsocket([](auto &&ws) { boost::beast::get_lowest_layer(ws).close(); });
}

std::expected<void, std::string> Session::Authorize(const nlohmann::json &creds) {
  if (!auth_.Authenticate(creds.at("username").get<std::string>(), creds.at("password").get<std::string>())) {
    return std::unexpected{"Authentication failed!"};
  }
#ifdef MG_ENTERPRISE
  if (!auth_.HasWebsocketPermission()) {
    return std::unexpected{"Authorization failed!"};
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
    if (connected_.load(std::memory_order_relaxed)) {
      Disconnect();
      LogError(ec, "read");
    }
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

      if (const auto result = Authorize(creds); !result.has_value()) {
        std::invoke(auth_failed, result.error());
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
    } catch (const std::exception &e) {
      spdlog::error("Authentication failed: {}", e.what());
      std::invoke(auth_failed, "Authentication failed!");
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
