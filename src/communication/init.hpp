// Copyright 2021 Memgraph Ltd.
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

namespace communication {

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

}  // namespace communication
