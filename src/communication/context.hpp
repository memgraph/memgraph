#pragma once

#include <string>

#include <openssl/ssl.h>

namespace communication {

class ClientContext final {
 public:
  /**
   * This constructor constructs a ClientContext that can either not use SSL
   * (`use_ssl` is `false` by default), or it constructs a ClientContext that
   * doesn't use a client certificate when `use_ssl` is set to `true`.
   */
  explicit ClientContext(bool use_ssl = false);

  /**
   * This constructor constructs a ClientContext that uses SSL and uses the
   * specific client private key and certificate combination. If the parameters
   * `key_file` and `cert_file` are equal to "" then the constructor falls back
   * to the above constructor that uses SSL without certificates.
   */
  ClientContext(const std::string &key_file, const std::string &cert_file);

  SSL_CTX *context();

  bool use_ssl();

 private:
  bool use_ssl_;
  SSL_CTX *ctx_;
};

class ServerContext final {
 public:
  /**
   * This constructor constructs a ServerContext that doesn't use SSL.
   */
  ServerContext();

  /**
   * This constructor constructs a ServerContext that uses SSL. The parameters
   * `key_file` and `cert_file` can't be "" because when setting up a server it
   * is mandatory to supply a private key and certificate. The parameter
   * `ca_file` can be "" because SSL doesn't necessarily need to check that the
   * client has a valid certificate. If you specify `verify_peer` to be `true`
   * to check that the client certificate is valid, then you need to supply a
   * valid `ca_file` as well.
   */
  ServerContext(const std::string &key_file, const std::string &cert_file,
                const std::string &ca_file = "", bool verify_peer = false);

  SSL_CTX *context();

  bool use_ssl();

 private:
  bool use_ssl_;
  SSL_CTX *ctx_;
};

}  // namespace communication
