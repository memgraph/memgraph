#pragma once

#include <string>

namespace communication {

/**
 * This function reads and returns a string describing the last OpenSSL error.
 */
const std::string SslGetLastError();

}  // namespace communication
