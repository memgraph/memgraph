#include <glog/logging.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "utils/signals.hpp"

namespace communication {

void Init() {
  // Initialize the OpenSSL library.
  SSL_library_init();
  OpenSSL_add_ssl_algorithms();
  SSL_load_error_strings();
  ERR_load_crypto_strings();

  // Ignore SIGPIPE.
  CHECK(utils::SignalIgnore(utils::Signal::Pipe)) << "Couldn't ignore SIGPIPE!";
}
}  // namespace communication
