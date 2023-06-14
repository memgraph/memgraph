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

#include <functional>
#include <iostream>
#include <optional>

#include "io/network/endpoint.hpp"

namespace memgraph::io::network {

/**
 * This class creates a network socket.
 * It is used to connect/bind/listen on a Endpoint (address + port).
 * It has wrappers for setting network socket flags and wrappers for
 * reading/writing data from/to the socket.
 */
class Socket {
 public:
  Socket() noexcept = default;
  Socket(const Socket &) = delete;
  Socket &operator=(const Socket &) = delete;
  Socket(Socket &&) noexcept;
  Socket &operator=(Socket &&) noexcept;
  ~Socket() noexcept;

  /**
   * Closes the socket if it is open.
   */
  void Close();

  /**
   * Shutdown the socket if it is open.
   */
  void Shutdown();

  /**
   * Checks whether the socket is open.
   *
   * @return socket open status:
   *             true if the socket is open
   *             false if the socket is closed
   */
  bool IsOpen() const;

  /**
   * Connects the socket to the specified endpoint.
   *
   * @param endpoint Endpoint to which to connect to
   *
   * @return connection success status:
   *             true if the connect succeeded
   *             false if the connect failed
   */
  bool Connect(const Endpoint &endpoint);

  /**
   * Binds the socket to the specified endpoint.
   *
   * @param endpoint Endpoint to which to bind to
   *
   * @return bind success status:
   *             true if the bind succeeded
   *             false if the bind failed
   */
  bool Bind(const Endpoint &endpoint);

  /**
   * Start listening on the bound socket.
   *
   * @param backlog maximum number of pending connections in the connection
   *                queue
   *
   * @return listen success status:
   *             true if the listen succeeded
   *             false if the listen failed
   */
  bool Listen(int backlog);

  /**
   * Accepts a new connection.
   * This function accepts a new connection on a listening socket.
   *
   * @return socket if accepted, nullopt otherwise.
   */
  std::optional<Socket> Accept();

  /**
   * Sets the socket to non-blocking.
   */
  void SetNonBlocking();

  /**
   * Enables TCP keep-alive on the socket.
   */
  void SetKeepAlive();

  /**
   * Enables TCP no_delay on the socket.
   * When enabled, the socket doesn't wait for an ACK of every data packet
   * before sending the next packet.
   */
  void SetNoDelay();

  /**
   * Sets the socket timeout.
   *
   * @param sec timeout seconds value
   * @param usec timeout microseconds value
   */
  void SetTimeout(int64_t sec, int64_t usec);

  /**
   * Checks if there are any errors on a socket. Returns 0 if there are none.
   */
  int ErrorStatus() const;

  /**
   * Returns the socket file descriptor.
   */
  int fd() const { return socket_; }

  /**
   * Returns the currently active endpoint of the socket.
   */
  const Endpoint &endpoint() const { return endpoint_; }

  /**
   * Write data to the socket.
   * These functions guarantee that all data will be written.
   *
   * @param data uint8_t* to data that should be written
   * @param len length of char* or uint8_t* data
   * @param have_more set to true if you plan to send more data to allow the
   * kernel to buffer the data instead of immediately sending it out
   *
   * @return write success status:
   *             true if write succeeded
   *             false if write failed
   */
  bool Write(const uint8_t *data, size_t len, bool have_more = false);
  bool Write(const std::string &s, bool have_more = false);

  /**
   * Read data from the socket.
   * This function is a direct wrapper for the read function.
   *
   * @param buffer pointer to the read buffer
   * @param len length of the read buffer
   * @param nonblock set to true if you want a non-blocking read
   *
   * @return read success status:
   *             > 0 if data was read, means number of read bytes
   *             == 0 if the client closed the connection
   *             < 0 if an error has occurred
   */
  ssize_t Read(void *buffer, size_t len, bool nonblock = false);

  /**
   * Wait until the socket becomes ready for a `Read` operation.
   * This function blocks indefinitely waiting for the socket to change its
   * state. This function is useful when you need a blocking operation on a
   * non-blocking socket, you can call this function to ensure that your next
   * `Read` operation will succeed.
   *
   * The function returns `true` if the wait succeeded (there is data waiting to
   * be read from the socket) and returns `false` if the wait failed (the socket
   * was closed or something else bad happened).
   *
   * @return wait success status:
   *             true if the wait succeeded
   *             false if the wait failed
   */
  bool WaitForReadyRead();

  /**
   * Wait until the socket becomes ready for a `Write` operation.
   * This function blocks indefinitely waiting for the socket to change its
   * state. This function is useful when you need a blocking operation on a
   * non-blocking socket, you can call this function to ensure that your next
   * `Write` operation will succeed.
   *
   * The function returns `true` if the wait succeeded (the socket can be written
   * to) and returns `false` if the wait failed (the socket was closed or
   * something else bad happened).
   *
   * @return wait success status:
   *             true if the wait succeeded
   *             false if the wait failed
   */
  bool WaitForReadyWrite();

 private:
  Socket(int fd, const Endpoint &endpoint) : socket_(fd), endpoint_(endpoint) {}

  int socket_ = -1;
  Endpoint endpoint_;
};
}  // namespace memgraph::io::network
