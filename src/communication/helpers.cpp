#include <openssl/err.h>

#include "communication/helpers.hpp"

namespace communication {

const std::string SslGetLastError() {
  char buff[2048];
  auto err = ERR_get_error();
  ERR_error_string_n(err, buff, sizeof(buff));
  return std::string(buff);
}
}  // namespace communication
