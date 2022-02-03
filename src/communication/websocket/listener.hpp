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

#pragma once

#define BOOST_ASIO_USE_TS_EXECUTOR_AS_DEFAULT

#include <list>
#include <memory>

#include <spdlog/spdlog.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>

#include "communication/context.hpp"
#include "communication/websocket/session.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace communication::websocket {
class Listener : public std::enable_shared_from_this<Listener> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<Listener> Create(Args &&...args) {
    return std::shared_ptr<Listener>{new Listener(std::forward<Args>(args)...)};
  }

  // Start accepting incoming connections
  void Run();
  void WriteToAll(std::shared_ptr<std::string> message);
  tcp::endpoint GetEndpoint() const;

 private:
  Listener(boost::asio::io_context &ioc, ServerContext *context, tcp::endpoint endpoint, AuthenticationInterface &auth);

  void DoAccept();
  void OnAccept(boost::beast::error_code ec, tcp::socket socket);

  boost::asio::io_context &ioc_;
  ServerContext *context_;
  tcp::acceptor acceptor_;
  utils::Synchronized<std::list<std::shared_ptr<Session>>, utils::SpinLock> sessions_;
  AuthenticationInterface &auth_;
};
}  // namespace communication::websocket
