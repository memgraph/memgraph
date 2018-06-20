#include <glog/logging.h>

#include "communication/context.hpp"

namespace communication {

ClientContext::ClientContext(bool use_ssl) : use_ssl_(use_ssl), ctx_(nullptr) {
  if (use_ssl_) {
    ctx_ = SSL_CTX_new(TLS_client_method());
    CHECK(ctx_ != nullptr) << "Couldn't create client SSL_CTX object!";

    // Disable legacy SSL support. Other options can be seen here:
    // https://www.openssl.org/docs/man1.0.2/ssl/SSL_CTX_set_options.html
    SSL_CTX_set_options(ctx_, SSL_OP_NO_SSLv3);
  }
}

ClientContext::ClientContext(const std::string &key_file,
                             const std::string &cert_file)
    : ClientContext(true) {
  if (key_file != "" && cert_file != "") {
    CHECK(SSL_CTX_use_certificate_file(ctx_, cert_file.c_str(),
                                       SSL_FILETYPE_PEM) == 1)
        << "Couldn't load client certificate from file: " << cert_file;
    CHECK(SSL_CTX_use_PrivateKey_file(ctx_, key_file.c_str(),
                                      SSL_FILETYPE_PEM) == 1)
        << "Couldn't load client private key from file: " << key_file;
  }
}

SSL_CTX *ClientContext::context() { return ctx_; }

bool ClientContext::use_ssl() { return use_ssl_; }

ServerContext::ServerContext() : use_ssl_(false), ctx_(nullptr) {}

ServerContext::ServerContext(const std::string &key_file,
                             const std::string &cert_file,
                             const std::string &ca_file, bool verify_peer)
    : use_ssl_(true), ctx_(SSL_CTX_new(TLS_server_method())) {
  // TODO (mferencevic): add support for encrypted private keys
  // TODO (mferencevic): add certificate revocation list (CRL)
  CHECK(SSL_CTX_use_certificate_file(ctx_, cert_file.c_str(),
                                     SSL_FILETYPE_PEM) == 1)
      << "Couldn't load server certificate from file: " << cert_file;
  CHECK(SSL_CTX_use_PrivateKey_file(ctx_, key_file.c_str(), SSL_FILETYPE_PEM) ==
        1)
      << "Couldn't load server private key from file: " << key_file;

  // Disable legacy SSL support. Other options can be seen here:
  // https://www.openssl.org/docs/man1.0.2/ssl/SSL_CTX_set_options.html
  SSL_CTX_set_options(ctx_, SSL_OP_NO_SSLv3);

  if (ca_file != "") {
    // Load the certificate authority file.
    CHECK(SSL_CTX_load_verify_locations(ctx_, ca_file.c_str(), nullptr) == 1)
        << "Couldn't load certificate authority from file: " << ca_file;

    if (verify_peer) {
      // Add the CA to list of accepted CAs that is sent to the client.
      STACK_OF(X509_NAME) *ca_names = SSL_load_client_CA_file(ca_file.c_str());
      CHECK(ca_names != nullptr)
          << "Couldn't load certificate authority from file: " << ca_file;
      // `ca_names` doesn' need to be free'd because we pass it to
      // `SSL_CTX_set_client_CA_list`:
      // https://mta.openssl.org/pipermail/openssl-users/2015-May/001363.html
      SSL_CTX_set_client_CA_list(ctx_, ca_names);

      // Enable verification of the client certificate.
      SSL_CTX_set_verify(
          ctx_, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    }
  }
}

SSL_CTX *ServerContext::context() { return ctx_; }

bool ServerContext::use_ssl() { return use_ssl_; }

}  // namespace communication
