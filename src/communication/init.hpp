#pragma once

namespace communication {

/**
 * Call this function in each `main` file that uses the Communication stack. It
 * is used to initialize all libraries (primarily OpenSSL) and to fix some
 * issues also related to OpenSSL (handling of SIGPIPE).
 *
 * Description of OpenSSL init can be seen here:
 * https://wiki.openssl.org/index.php/Library_Initialization
 *
 * NOTE: This function must be called **exactly** once.
 */
void Init();
}  // namespace communication
