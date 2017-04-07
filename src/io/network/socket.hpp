#pragma once

#include "io/network/network_endpoint.hpp"

#include <iostream>

namespace io::network {

/**
 * This class creates a network socket.
 * It is used to connect/bind/listen on a NetworkEndpoint (address + port).
 * It has wrappers for setting network socket flags and wrappers for
 * reading/writing data from/to the socket.
 */
class Socket {
 public:
  Socket();
  Socket(const Socket& s);
  Socket(Socket&& other);
  Socket& operator=(Socket&& other);
  ~Socket();

  /**
   * Closes the socket if it is open.
   */
  void Close();

  /**
   * Checks whether the socket is open.
   *
   * @return socket open status:
   *             true if the socket is open
   *             false if the socket is closed
   */
  bool IsOpen();

  /**
   * Connects the socket to the specified endpoint.
   *
   * @param endpoint NetworkEndpoint to which to connect to
   *
   * @return connection success status:
   *             true if the connect succeeded
   *             false if the connect failed
   */
  bool Connect(NetworkEndpoint& endpoint);

  /**
   * Binds the socket to the specified endpoint.
   *
   * @param endpoint NetworkEndpoint to which to bind to
   *
   * @return bind success status:
   *             true if the bind succeeded
   *             false if the bind failed
   */
  bool Bind(NetworkEndpoint& endpoint);

  /**
   * Start listening on the bound socket.
   *
   * @param backlog maximum number of pending connections in the connection queue
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
   * @param s Socket object that will be instantiated with the new connection
   *
   * @return accept success status:
   *             true if a new connection was accepted and the socket 's' was instantiated
   *             false if a new connection accept failed
   */
  bool Accept(Socket* s);

  /**
   * Sets the socket to non-blocking.
   *
   * @return set non-blocking success status:
   *             true if the socket was successfully set to non-blocking
   *             false if the socket was not set to non-blocking
   */
  bool SetNonBlocking();

  /**
   * Enables TCP keep-alive on the socket.
   *
   * @return enable keep-alive success status:
   *             true if keep-alive was successfully enabled on the socket
   *             false if keep-alive was not enabled
   */
  bool SetKeepAlive();

  /**
   * Sets the socket timeout.
   *
   * @param sec timeout seconds value
   * @param usec timeout microseconds value
   * @return set socket timeout status:
   *             true if the timeout was successfully set to
   *                 sec seconds + usec microseconds
   *             false if the timeout was not set
   */
  bool SetTimeout(long sec, long usec);

  // TODO: this will be removed
  operator int();

  /**
   * Returns the socket ID.
   * The socket ID is its unix file descriptor number.
   */
  int id() const;

  /**
   * Returns the currently active endpoint of the socket.
   */
  NetworkEndpoint& endpoint();

  /**
   * Write data to the socket.
   * Theese functions guarantee that all data will be written.
   *
   * @param str std::string to write to the socket
   * @param data char* or uint8_t* to data that should be written
   * @param len length of char* or uint8_t* data
   *
   * @return write success status:
   *             true if write succeeded
   *             false if write failed
   */
  bool Write(const std::string& str);
  bool Write(const char* data, size_t len);
  bool Write(const uint8_t* data, size_t len);

  /**
   * Read data from the socket.
   * This function is a direct wrapper for the read function.
   *
   * @param buffer pointer to the read buffer
   * @param len length of the read buffer
   *
   * @return read success status:
   *             > 0 if data was read, means number of read bytes
   *             == 0 if the client closed the connection
   *             < 0 if an error has occurred
   */
  int Read(void* buffer, size_t len);

 private:
  Socket(int sock, NetworkEndpoint& endpoint);

  int socket_;
  NetworkEndpoint endpoint_;
};
}
