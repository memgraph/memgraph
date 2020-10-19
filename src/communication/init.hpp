#pragma once

namespace communication {

/**
 * Create this object in each `main` file that uses the Communication stack. It
 * is used to initialize all libraries (primarily OpenSSL) and to fix some
 * issues also related to OpenSSL (handling of SIGPIPE).
 *
 * Description of OpenSSL init can be seen here:
 * https://wiki.openssl.org/index.php/Library_Initialization
 *
 * NOTE: This object must be created **exactly** once.
 */

struct SSLInit {
  SSLInit();

  SSLInit(const SSLInit &) = delete;
  SSLInit(SSLInit &&) = delete;
  SSLInit &operator=(const SSLInit &) = delete;
  SSLInit &operator=(SSLInit &&) = delete;
  ~SSLInit();
};

}  // namespace communication
