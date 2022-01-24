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

#pragma once

#include <deque>
#include <memory>
#include <span>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include "communication/buffer.hpp"
#include "communication/session.hpp"

namespace communication::websocket {
void LogError(boost::beast::error_code ec, const std::string_view what) {
  spdlog::warn("Websocket session failed on {}: {}", what, ec.message());
}

template <typename TSession, typename TSessionData>
class Session : public std::enable_shared_from_this<Session<TSession, TSessionData>> {
  using tcp = boost::asio::ip::tcp;

 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>{new Session{std::forward<Args>(args)...}};
  }

  void Run() {
    boost::asio::dispatch(strand_, [shared_this = this->shared_from_this()] { shared_this->OnRun(); });
  }

 private:
  explicit Session(tcp::socket &&socket, TSessionData *data)
      : endpoint_(socket.local_endpoint()),
        ws_(std::move(socket)),
        strand_{boost::asio::make_strand(ws_.get_executor())},
        data_{data} {}

  void OnRun() {
    ws_.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));
    boost::asio::socket_base::keep_alive option(true);
    ws_.set_option(boost::beast::websocket::stream_base::decorator([](boost::beast::websocket::response_type &res) {
      res.set(boost::beast::http::field::server, "Memgraph WS");
      res.set(boost::beast::http::field::sec_websocket_protocol, "binary");
    }));

    ws_.binary(true);
    // This buffer will hold the HTTP request as raw characters

    // This buffer is required for reading HTTP messages
    //    flat_buffer buffer;

    // Read the HTTP request ourselves
    //    boost::beast::http::request<http::string_body> req;
    //    http::read(sock, buffer, req);
    //    std::cout << "bu
    // Read into our buffer until we reach the end of the HTTP request.
    // No parsing takes place here, we are just accumulating data.

    //    boost::beast::net::read_until(sock, net::dynamic_buffer(s), "\r\n\r\n");

    // Now accept the connection, using the buffered data.
    //    ws_.accept(net::buffer(s));
    ws_.async_accept(boost::asio::bind_executor(
        strand_, [shared_this = this->shared_from_this()](auto ec) { shared_this->OnAccept(ec); }));
  }

  void OnAccept(boost::beast::error_code ec) {
    if (ec) {
      return LogError(ec, "accept");
    }

    session_data_.emplace(this->shared_from_this());

    // run on the strand
    boost::asio::dispatch(strand_, [shared_this = this->shared_from_this()] { shared_this->DoRead(); });
  }

  void DoWrite(const uint8_t *data, size_t len, bool have_more = false) {
    boost::beast::error_code ec;
    ws_.write(boost::asio::const_buffer{data, len}, ec);
    if (ec) {
      return LogError(ec, "write");
    }
  }

  void DoRead() {
    auto buf = session_data_->input_buffer_.write_end()->Allocate();

    auto mutable_buffer = std::make_unique<boost::asio::mutable_buffer>(buf.data, buf.len);

    ws_.async_read_some(
        *mutable_buffer,
        boost::asio::bind_executor(
            strand_, [mutable_buffer = std::move(mutable_buffer), shared_this = this->shared_from_this()](
                         auto ec, auto bytes_transferred) { shared_this->OnRead(ec, bytes_transferred); }));
  }

  void OnRead(boost::beast::error_code ec, size_t bytes_transferred) {
    if (ec == boost::beast::websocket::error::closed) {
      return;
    }
    if (ec) {
      return LogError(ec, "read");
    }

    session_data_->input_buffer_.write_end()->Written(bytes_transferred);

    try {
      session_data_->session_.Execute();
    } catch (const SessionClosedException &e) {
      spdlog::info("Closed connection");
      return;
    }
    DoRead();
  }

  boost::asio::ip::tcp::endpoint endpoint_;
  boost::beast::websocket::stream<boost::beast::tcp_stream> ws_;
  boost::beast::flat_buffer buffer_;
  boost::asio::strand<decltype(ws_)::executor_type> strand_;

  TSessionData *data_;

  struct SessionData {
    explicit SessionData(std::shared_ptr<Session<TSession, TSessionData>> session)
        : output_stream_([session](const uint8_t *data, size_t len, bool have_more) {
            session->DoWrite(data, len, have_more);
            return true;
          }),
          session_(session->data_,
                   io::network::Endpoint{session->endpoint_.address().to_string(), session->endpoint_.port()},
                   input_buffer_.read_end(), &output_stream_) {}

    Buffer input_buffer_;
    communication::OutputStream output_stream_;
    TSession session_;
  };

  std::optional<SessionData> session_data_;
};
}  // namespace communication::websocket
