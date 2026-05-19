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

#include <cstdint>
#include <string>

namespace memgraph::utils {

struct TlsConfig {
  std::string key_file;
  std::string cert_file;
  std::string ca_file;

  friend bool operator==(TlsConfig const &, TlsConfig const &) = default;
};

enum class SSL_CTX_ERR_TYPE : uint8_t {
  FAIL_CERT_FILE,
  FAIL_KEY_FILE,
  FAIL_SET_OPTIONS,
  FAIL_LOAD_CA,
  FAIL_SET_SSL_VERIFICATION_MODE,
  FLAGS_NOT_CONFIGURED
};

struct SSL_CTX_Error {
  SSL_CTX_ERR_TYPE err_type;
  std::string msg;
};

}  // namespace memgraph::utils
