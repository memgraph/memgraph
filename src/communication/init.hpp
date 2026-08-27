// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

namespace memgraph::communication {

/**
 * Puts the process into FIPS 140-3 approved mode: verifies that the validated
 * OpenSSL FIPS provider is available and operational, then routes every
 * implicit and explicit EVP fetch to it. Exits with
 * `ExitCode::FipsModeUnavailable` if either step fails — a half-configured
 * FIPS deployment is worse than none, because it invites a compliance claim
 * that isn't true.
 *
 * Call this only when `--fips-mode=true`, after the logger is initialized (so
 * a failure is actually reported) and before anything creates an SSL context
 * or hashes a password.
 *
 * NOTE: This routes everything that goes *through EVP* to the validated
 * module. It has no effect on crypto that bypasses EVP entirely, which is why
 * the non-approved password hash algorithms need their own separate check.
 */
void EnableFipsMode();

/**
 * Create this object in each `main` file that uses the Communication stack. It
 * is used to initialize all libraries (primarily OpenSSL) and to fix some
 * issues also related to OpenSSL (handling of SIGPIPE).
 *
 * We define a struct to take advantage of RAII so that the proper cleanup
 * is called after we are finished using the SSL connection.
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

}  // namespace memgraph::communication
