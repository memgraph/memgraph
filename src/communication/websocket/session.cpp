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

#include "communication/websocket/session.hpp"

#include <functional>
#include <memory>
#include <string>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <boost/asio/bind_executor.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <json/json.hpp>

#include "communication/context.hpp"
#include "utils/logging.hpp"

namespace communication::websocket {
namespace {
void LogError(boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Websocket session failed on {}: {}", what, ec.message());
}
}  // namespace

void Session::Run() {
  ws_.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));

  ws_.set_option(boost::beast::websocket::stream_base::decorator(
      [](boost::beast::websocket::response_type &res) { res.set(boost::beast::http::field::server, "Memgraph WS"); }));

  // Accept the websocket handshake
  boost::beast::error_code ec;
  ws_.accept(ec);
  if (ec) {
    return LogError(ec, "accept");
  }
  connected_.store(true, std::memory_order_relaxed);

  // run on the strand
  boost::asio::dispatch(strand_, [shared_this = shared_from_this()] { shared_this->DoRead(); });
}

void Session::Write(std::shared_ptr<std::string> message) {
  boost::asio::dispatch(strand_, [message = std::move(message), shared_this = shared_from_this()]() mutable {
    if (!shared_this->connected_.load(std::memory_order_relaxed)) {
      return;
    }
    if (!shared_this->authenticated_) {
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
  auto next_message = messages_.front();
  ws_.async_write(
      boost::asio::buffer(*next_message),
      boost::asio::bind_executor(strand_, [message_string = std::move(next_message), shared_this = shared_from_this()](
                                              boost::beast::error_code ec, const size_t bytes_transferred) {
        shared_this->OnWrite(ec, bytes_transferred);
      }));
}

void Session::OnWrite(boost::beast::error_code ec, size_t /*bytes_transferred*/) {
  messages_.pop_front();

  if (ec) {
    return LogError(ec, "write");
  }
  if (close_) {
    DoClose();
    return;
  }
  if (!messages_.empty()) {
    DoWrite();
  }
}

void Session::DoRead() {
  ws_.async_read(
      buffer_, boost::asio::bind_executor(strand_, [shared_this = shared_from_this()](boost::beast::error_code ec,
                                                                                      const size_t bytes_transferred) {
        shared_this->OnRead(ec, bytes_transferred);
      }));
}

void Session::DoClose() {
  ws_.async_close(boost::beast::websocket::close_code::normal,
                  boost::asio::bind_executor(strand_, [shared_this = shared_from_this()](boost::beast::error_code ec) {
                    shared_this->OnClose(ec);
                  }));
}

void Session::OnClose(boost::beast::error_code ec) {
  if (ec) {
    return LogError(ec, "close");
  }
  connected_.store(false, std::memory_order_relaxed);
}

utils::BasicResult<std::string> Session::Authorize(const nlohmann::json &creds) {
  if (!auth_.Authenticate(creds.at("username").get<std::string>(), creds.at("password").get<std::string>())) {
    return {"Authentication failed!"};
  }
#ifndef MG_ENTERPRISE
  if (auth_.UserHasPermission(creds.at("username").get<std::string>(), auth::Permission::WEBSOCKET)) {
    return {"Authorization failed!"};
  }
#endif
  return {};
}

void Session::OnRead(const boost::beast::error_code ec, const size_t /*bytes_transferred*/) {
  if (ec == boost::beast::websocket::error::closed) {
    messages_.clear();
    connected_.store(false, std::memory_order_relaxed);
    return;
  }

  if (auth_.HasAnyUsers() && !authenticated_) {
    auto response = nlohmann::json();
    auto auth_failed = [this, &response]() {
      response["success"] = false;
      MG_ASSERT(messages_.empty());
      messages_.push_back(make_shared<std::string>(response.dump()));
      close_ = true;
      DoWrite();
    };
    try {
      const auto creds = nlohmann::json::parse(boost::beast::buffers_to_string(buffer_.data()));
      buffer_.consume(buffer_.size());

      if (const auto result = Authorize(creds); result.HasError()) {
        response["message"] = result.GetError();
        std::invoke(auth_failed);
        return;
      }
      response["success"] = true;
      response["message"] = "User has been successfully authenticated!";
      MG_ASSERT(messages_.empty());
      messages_.push_back(make_shared<std::string>(response.dump()));
      DoWrite();
      authenticated_ = true;
    } catch (const nlohmann::json::out_of_range &out_of_range) {
      const auto err_msg = fmt::format("Invalid JSON for authentication received: {}!", out_of_range.what());
      spdlog::error(err_msg);
      response["message"] = err_msg;
      std::invoke(auth_failed);
      return;
    } catch (const nlohmann::json::parse_error &parse_error) {
      const auto err_msg = fmt::format("Cannot parse JSON for WebSocket authentication: {}!", parse_error.what());
      spdlog::error(err_msg);
      response["message"] = err_msg;
      std::invoke(auth_failed);
      return;
    }
  }

  DoRead();
}

}  // namespace communication::websocket
