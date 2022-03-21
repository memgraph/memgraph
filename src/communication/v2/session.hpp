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

#define BOOST_ASIO_USE_TS_EXECUTOR_AS_DEFAULT 1

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/system_context.hpp>
#include <boost/system/detail/error_code.hpp>

#include "communication/buffer.hpp"
#include "communication/context.hpp"
#include "communication/exceptions.hpp"
#include "communication/helpers.hpp"
#include "io/network/socket.hpp"
#include "io/network/stream_buffer.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"
#include "utils/message.hpp"

namespace memgraph::communication::v2 {

/**
 * This is used to provide input to user Sessions. All Sessions used with the
 * network stack should use this class as their input stream.
 */
using InputStream = communication::Buffer::ReadEnd;
using tcp = boost::asio::ip::tcp;

/**
 * This is used to provide output from user Sessions. All Sessions used with the
 * network stack should use this class for their output stream.
 */
class OutputStream final {
 public:
  OutputStream(std::function<bool(const uint8_t *, size_t, bool)> write_function) : write_function_(write_function) {}

  OutputStream(const OutputStream &) = delete;
  OutputStream(OutputStream &&) = delete;
  OutputStream &operator=(const OutputStream &) = delete;
  OutputStream &operator=(OutputStream &&) = delete;

  bool Write(const uint8_t *data, size_t len, bool have_more = false) { return write_function_(data, len, have_more); }

  bool Write(const std::string &str, bool have_more = false) {
    return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(), have_more);
  }

 private:
  std::function<bool(const uint8_t *, size_t, bool)> write_function_;
};

/**
 * This class is used internally in the communication stack to handle all user
 * Sessions. It handles socket ownership, inactivity timeout and protocol
 * wrapping.
 */
template <class TSession, class TSessionData>
class Session final : public std::enable_shared_from_this<Session<TSession, TSessionData>> {
 public:
  template <typename... Args>
  static std::shared_ptr<Session> Create(Args &&...args) {
    return std::shared_ptr<Session>(new Session(std::forward<Args>(args)...));
  }

  Session(const Session &) = delete;
  Session(Session &&) = delete;
  Session &operator=(const Session &) = delete;
  Session &operator=(Session &&) = delete;

  ~Session() {
    if (execution_active_) {
      spdlog::error("Session: Destructor called while execution is active");
    }
  }

  bool Start() {
    if (execution_active_) {
      return false;
    }
    execution_active_ = true;
    boost::asio::dispatch(strand_, [shared_this = this->shared_from_this()] { shared_this->DoRead(); });
    return true;
  }

  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    boost::asio::dispatch(this->strand_, [shared_this = this->shared_from_this(), data, len, have_more] {
      if (shared_this->socket_.is_open()) {
        shared_this->socket_.async_write_some(
            boost::asio::buffer(data, len),
            boost::asio::bind_executor(
                shared_this->strand_,
                [shared_this, have_more](const boost::system::error_code &ec, std::size_t /*bytes_transferred*/) {
                  if (ec) {
                    shared_this->OnError(ec);
                  } else {
                    if (!have_more) {
                      shared_this->DoRead();
                    }
                  }
                }));
      }
    });
    return true;
  }

  bool IsConnected() const noexcept { return execution_active_; }

 private:
  explicit Session(tcp::socket &&socket, TSessionData *data, tcp::endpoint endpoint)
      : socket_(std::move(socket)),
        output_stream_([this](const uint8_t *data, size_t len, bool have_more) { return true; }),
        session_(data, endpoint, input_buffer_.read_end(), &output_stream_),
        strand_{boost::asio::make_strand(socket_.get_executor())} {
    using rcv_timeout_option = boost::asio::detail::socket_option::integer<SOL_SOCKET, SO_RCVTIMEO>;

    socket_.set_option(boost::asio::ip::tcp::no_delay(true));        // enable PSH
    socket_.set_option(boost::asio::socket_base::keep_alive(true));  // enable SO_KEEPALIVE
    socket_.set_option(rcv_timeout_option{1000});                    // set rcv timeout for seession
  }

  void DoRead() {
    if (!execution_active_) {
      return;
    }
    auto buffer = input_buffer_.write_end()->Allocate();
    socket_.async_read_some(
        boost::asio::buffer(buffer.data, buffer.len),
        boost::asio::bind_executor(strand_, [shared_this = this->shared_from_this()](
                                                const boost::system::error_code &ec, std::size_t bytes_transferred) {
          if (ec) {
            shared_this->OnError(ec);
          } else {
            shared_this->input_buffer_.write_end()->Commit(bytes_transferred);
            shared_this->session_.OnRead();
          }
        }));
  }

  void OnRead(const boost::system::error_code ec, const size_t /*bytes_transferred*/) {
    if (!execution_active_) {
      return;
    }
    if (ec) {
      OnError(ec);
      return;
    }
    session_.Execute();
  }

  void OnError(const boost::system::error_code &ec) {
    if (ec == boost::asio::error::operation_aborted) {
      return;
    }

    if (ec == boost::asio::error::eof) {
      spdlog::info("Session closed by peer");
    } else {
      spdlog::error("Session error: {}", ec.message());
    }

    DoShutdown();
  }

  void DoShutdown() {
    boost::asio::dispatch(strand_, [shared_this = this->shared_from_this()] {
      shared_this->execution_active_ = false;
      shared_this->OnShutdown();
    });
  }

  void OnShutdown() { socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both); }

  void DoWrite() {
    if (!execution_active_) {
      return;
    }
    auto buffer = input_buffer_.write_end()->Allocate();
    socket_.async_read_some(
        boost::asio::buffer(buffer.data, buffer.len),
        boost::asio::bind_executor(strand_, [shared_this = this->shared_from_this()](
                                                const boost::system::error_code &ec, std::size_t bytes_transferred) {
          if (ec) {
            shared_this->OnError(ec);
          } else {
            shared_this->input_buffer_.write_end()->Commit(bytes_transferred);
            shared_this->session_.Execute();
            shared_this->session_.OnRead();
          }
        }));
  }

  void OnWrite() {
    if (!execution_active_) {
      DoShutdown();
      return;
    }
  }

  tcp::socket socket_;
  communication::Buffer input_buffer_;
  OutputStream output_stream_;
  TSession session_;
  std::deque<std::shared_ptr<std::string>> messages_;
  bool execution_active_{false};
  boost::asio::strand<tcp::socket> strand_;
};
}  // namespace memgraph::communication::v2
