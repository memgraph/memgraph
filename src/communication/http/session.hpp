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

#pragma once

#include <deque>
#include <memory>
#include <optional>
#include <variant>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <json/json.hpp>

#include "communication/context.hpp"
#include "utils/result.hpp"
#include "utils/synchronized.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::communication::http {
using tcp = boost::asio::ip::tcp;
namespace http = boost::beast::http;

class Session : public std::enable_shared_from_this<Session> {
 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>{new Session{std::forward<Args>(args)...}};
  }

  void Run();
  bool IsConnected() const;

 private:
  explicit Session(tcp::socket &&socket, ServerContext &context);

  void OnWrite(boost::beast::error_code ec, size_t bytes_transferred);

  void DoRead();
  void OnRead(boost::beast::error_code ec, size_t bytes_transferred);

  void DoClose();

  auto GetExecutor() { return stream_.get_executor(); }

  boost::beast::tcp_stream stream_;

  boost::beast::flat_buffer buffer_;
  boost::beast::http::request<boost::beast::http::string_body> req_;
  std::shared_ptr<void> res_;

  boost::asio::strand<boost::beast::tcp_stream::executor_type> strand_;
  std::atomic<bool> connected_{false};
  bool close_{false};
};
}  // namespace memgraph::communication::http
