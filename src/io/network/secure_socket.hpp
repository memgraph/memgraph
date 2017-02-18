#pragma once

#include "io/network/socket.hpp"
#include "tls.hpp"
#include "tls_error.hpp"
#include "utils/types/byte.hpp"

#include <iostream>

namespace io {

class SecureSocket {
 public:
  SecureSocket(Socket&& socket, const Tls::Context& tls)
      : socket(std::forward<Socket>(socket)) {
    ssl = SSL_new(tls);
    SSL_set_fd(ssl, this->socket);

    SSL_set_accept_state(ssl);

    if (SSL_accept(ssl) <= 0) ERR_print_errors_fp(stderr);
  }

  SecureSocket(SecureSocket&& other) {
    *this = std::forward<SecureSocket>(other);
  }

  SecureSocket& operator=(SecureSocket&& other) {
    socket = std::move(other.socket);

    ssl = other.ssl;
    other.ssl = nullptr;

    return *this;
  }

  ~SecureSocket() {
    if (ssl == nullptr) return;

    std::cout << "DELETING SSL" << std::endl;

    SSL_free(ssl);
  }

  int error(int status) { return SSL_get_error(ssl, status); }

  int write(const std::string& str) { return write(str.c_str(), str.size()); }

  int write(const byte* data, size_t len) { return SSL_write(ssl, data, len); }

  int write(const char* data, size_t len) { return SSL_write(ssl, data, len); }

  int read(char* buffer, size_t len) { return SSL_read(ssl, buffer, len); }

  operator int() { return socket; }

  operator Socket&() { return socket; }

 private:
  Socket socket;
  SSL* ssl{nullptr};
};
}
