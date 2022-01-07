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

#include "communication/websocket/listener.hpp"

namespace communication::websocket {
namespace {
void LogError(boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Websocket listener failed on {}: {}", what, ec.message());
}
}  // namespace

void Listener::Run() { DoAccept(); }

void Listener::WriteToAll(std::shared_ptr<std::string> message) {
  auto sessions_ptr = sessions_.Lock();
  for (auto &session : *sessions_ptr) {
    session->Write(message);
  }
}

Listener::Listener(boost::asio::io_context &ioc, tcp::endpoint endpoint, SafeAuth *auth)
    : ioc_(ioc), acceptor_(ioc), auth_(auth) {
  boost::beast::error_code ec;

  // Open the acceptor
  acceptor_.open(endpoint.protocol(), ec);
  if (ec) {
    LogError(ec, "open");
    return;
  }

  // Allow address reuse
  acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
  if (ec) {
    LogError(ec, "set_option");
    return;
  }

  // Bind to the server address
  acceptor_.bind(endpoint, ec);
  if (ec) {
    LogError(ec, "bind");
    return;
  }

  acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
  if (ec) {
    LogError(ec, "listen");
    return;
  }

  spdlog::info("WebSocket server is listening on {}:{}", endpoint.address(), endpoint.port());
}

void Listener::DoAccept() {
  acceptor_.async_accept(
      ioc_, [shared_this = shared_from_this()](auto ec, auto socket) { shared_this->OnAccept(ec, std::move(socket)); });
}

void Listener::OnAccept(boost::beast::error_code ec, tcp::socket socket) {
  if (ec) {
    return LogError(ec, "accept");
  }

  {
    auto sessions_ptr = sessions_.Lock();
    sessions_ptr->emplace_back(Session::Create(std::move(socket), auth_))->Run();

    // Clean disconnected clients
    std::erase_if(*sessions_ptr, [](const auto &elem) { return !elem->IsConnected(); });
  }

  DoAccept();
}
}  // namespace communication::websocket
