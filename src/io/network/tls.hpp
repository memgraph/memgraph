#pragma once

#include <string>

#include <openssl/err.h>
#include <openssl/ssl.h>

namespace io {

class Tls {
 public:
  class Context {
   public:
    Context();
    ~Context();

    Context& cert(const std::string& path);
    Context& key(const std::string& path);

    operator SSL_CTX*() const { return ctx; }

   private:
    SSL_CTX* ctx;
  };

  static void initialize();
  static void cleanup();
};
}
