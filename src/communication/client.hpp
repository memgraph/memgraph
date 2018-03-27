#pragma once

#include "communication/buffer.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/socket.hpp"

namespace communication {

/**
 * This class implements a generic network Client.
 * It uses blocking sockets and provides an API that can be used to receive/send
 * data over the network connection.
 */
class Client {
 public:
  /**
   * This function connects to a remote server and returns whether the connect
   * succeeded.
   */
  bool Connect(const io::network::Endpoint &endpoint) {
    if (!socket_.Connect(endpoint)) return false;
    socket_.SetKeepAlive();
    socket_.SetNoDelay();
    return true;
  }

  /**
   * This function returns `true` if the socket is in an error state.
   */
  bool ErrorStatus() { return socket_.ErrorStatus(); }

  /**
   * This function shuts down the socket.
   */
  void Shutdown() { socket_.Shutdown(); }

  /**
   * This function closes the socket.
   */
  void Close() { socket_.Close(); }

  /**
   * This function is used to receive `len` bytes from the socket and stores it
   * in an internal buffer. It returns `true` if the read succeeded and `false`
   * if it didn't.
   */
  bool Read(size_t len) {
    size_t received = 0;
    buffer_.write_end().Resize(buffer_.read_end().size() + len);
    while (received < len) {
      auto buff = buffer_.write_end().Allocate();
      int got = socket_.Read(buff.data, len - received);
      if (got <= 0) return false;
      buffer_.write_end().Written(got);
      received += got;
    }
    return true;
  }

  /**
   * This function returns a pointer to the read data that is currently stored
   * in the client.
   */
  uint8_t *GetData() { return buffer_.read_end().data(); }

  /**
   * This function returns the size of the read data that is currently stored in
   * the client.
   */
  size_t GetDataSize() { return buffer_.read_end().size(); }

  /**
   * This function removes first `len` bytes from the data buffer.
   */
  void ShiftData(size_t len) { buffer_.read_end().Shift(len); }

  /**
   * This function clears the data buffer.
   */
  void ClearData() { buffer_.read_end().Clear(); }

  // Write end
  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    return socket_.Write(data, len, have_more);
  }
  bool Write(const std::string &str, bool have_more = false) {
    return Write(reinterpret_cast<const uint8_t *>(str.data()), str.size(),
                 have_more);
  }

  const io::network::Endpoint &endpoint() { return socket_.endpoint(); }

 private:
  io::network::Socket socket_;

  Buffer buffer_;
};

/**
 * This class provides a stream-like input side object to the client.
 */
class ClientInputStream {
 public:
  ClientInputStream(Client &client) : client_(client) {}

  uint8_t *data() { return client_.GetData(); }

  size_t size() const { return client_.GetDataSize(); }

  void Shift(size_t len) { client_.ShiftData(len); }

  void Clear() { client_.ClearData(); }

 private:
  Client &client_;
};

/**
 * This class provides a stream-like output side object to the client.
 */
class ClientOutputStream {
 public:
  ClientOutputStream(Client &client) : client_(client) {}

  bool Write(const uint8_t *data, size_t len, bool have_more = false) {
    return client_.Write(data, len, have_more);
  }
  bool Write(const std::string &str, bool have_more = false) {
    return client_.Write(str, have_more);
  }

 private:
  Client &client_;
};

}  // namespace communication
